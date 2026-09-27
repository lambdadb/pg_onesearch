/* Backend-local, single-request transport candidate. No PG calls in curl callbacks. */
#include "postgres.h"

#include <curl/curl.h>
#include <zlib.h>

#include "miscadmin.h"
#include "storage/ipc.h"
#include "http.h"

typedef struct Request
{
    CURL *easy;
    CURLM *multi;
    bool own_multi;
    struct curl_slist *headers;
    bool attached;
    char *data;                  /* malloc: callback must never longjmp through curl */
    size_t len;
    size_t capacity;
    bool too_large;
    bool out_of_memory;
    int encoding;               /* 0 = identity/absent, 1 = gzip, 2 = unsupported */
    bool encoding_seen;
    const char *body;
    size_t body_len;
    size_t sent;
} Request;

static CURLM *pool;
static bool initialized;

static void
close_pool(int code, Datum arg)
{
    if (pool)
        curl_multi_cleanup(pool);
    pool = NULL;
    /* Do not tear down libcurl global state shared with other extensions. */
}

static void
init_pool(void)
{
    if (!initialized)
    {
        const curl_version_info_data *version;

        if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK)
            ereport(ERROR, (errmsg("remote HTTP initialization failed")));
        version = curl_version_info(CURLVERSION_NOW);
        /* A synchronous resolver can block inside perform despite the poll bound. */
        if (!(version->features & CURL_VERSION_ASYNCHDNS))
            ereport(ERROR, (errmsg("remote HTTP requires asynchronous DNS support")));
        on_proc_exit(close_pool, (Datum) 0);
        initialized = true;
    }
    if (!pool)
    {
        pool = curl_multi_init();
        if (!pool)
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("remote HTTP allocation failed")));
        if (curl_multi_setopt(pool, CURLMOPT_MAXCONNECTS, 2L) != CURLM_OK)
        {
            close_pool(0, (Datum) 0);
            ereport(ERROR, (errmsg("remote HTTP pool configuration failed")));
        }
    }
}

static size_t
receive_body(char *data, size_t size, size_t count, void *arg)
{
    Request *request = arg;
    size_t bytes;
    size_t capacity;
    char *buffer;

    if (size != 0 && count > SIZE_MAX / size)
        return 0;
    bytes = size * count;
    /* Bound wire bytes; gzip expansion is separately bounded in decode_body. */
    if (bytes > PGOS_MAX_RESPONSE - request->len)
    {
        request->too_large = true;
        return 0;
    }
    if (request->len + bytes + 1 > request->capacity)
    {
        capacity = Min((size_t) PGOS_MAX_RESPONSE + 1,
                       Max(request->len + bytes + 1, Max(request->capacity * 2, (size_t) 4096)));
        buffer = realloc(request->data, capacity);
        if (!buffer)
        {
            request->out_of_memory = true;
            return 0;
        }
        request->data = buffer;
        request->capacity = capacity;
    }
    memcpy(request->data + request->len, data, bytes);
    request->len += bytes;
    request->data[request->len] = '\0';
    return bytes;
}

static size_t
receive_header(char *data, size_t size, size_t count, void *arg)
{
    Request *request = arg;
    size_t bytes;

    if (size != 0 && count > SIZE_MAX / size)
        return 0;
    bytes = size * count;
    if (bytes >= 5 && strncasecmp(data, "HTTP/", 5) == 0)
    {
        /* Reset for the final response after an informational response. */
        request->encoding = 0;
        request->encoding_seen = false;
    }
    else if (bytes >= 17 && strncasecmp(data, "Content-Encoding:", 17) == 0)
    {
        const char *start = data + 17;
        const char *end = data + bytes;

        while (start < end && (*start == ' ' || *start == '\t'))
            start++;
        while (end > start && (end[-1] == '\r' || end[-1] == '\n' || end[-1] == ' ' || end[-1] == '\t'))
            end--;
        if (request->encoding_seen)
            request->encoding = 2;
        else if (end - start == 4 && strncasecmp(start, "gzip", 4) == 0)
            request->encoding = 1;
        else if (end - start == 8 && strncasecmp(start, "identity", 8) == 0)
            request->encoding = 0;
        else
            request->encoding = 2;
        request->encoding_seen = true;
    }
    return bytes;
}

static void
add_header(Request *request, const char *header)
{
    struct curl_slist *next = curl_slist_append(request->headers, header);

    if (!next)
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("remote HTTP allocation failed")));
    request->headers = next;
}

static size_t
send_body(char *buffer, size_t size, size_t count, void *arg)
{
    Request *request = arg;
    size_t bytes;

    if (size != 0 && count > SIZE_MAX / size)
        return CURL_READFUNC_ABORT;
    bytes = Min(size * count, request->body_len - request->sent);
    memcpy(buffer, request->body + request->sent, bytes);
    request->sent += bytes;
    return bytes;
}

static int
refuse_replay(void *arg, curl_off_t offset, int origin)
{
    /* curl can replay POST after an empty response on a reused connection.
     * Refuse rewinding even for reads: retry policy belongs to the API caller. */
    return CURL_SEEKFUNC_FAIL;
}

#define SETOPT(option, value) \
    do { \
        if (curl_easy_setopt(request->easy, option, value) != CURLE_OK) \
            ereport(ERROR, (errmsg("remote HTTP request configuration failed"))); \
    } while (0)

static char *
decode_body(Request *request)
{
    char *result;
    size_t len = request->len;

    if (request->encoding == 2)
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("unsupported remote content encoding")));
    /* Decode ourselves to verify the complete gzip member/CRC, even without
     * Content-Encoding. Curl still removes HTTP transfer framing. */
    if (request->encoding == 1 || (len >= 2 && (unsigned char) request->data[0] == 0x1f &&
                                  (unsigned char) request->data[1] == 0x8b))
    {
        z_stream *stream = palloc0(sizeof(z_stream));

        result = palloc(PGOS_MAX_RESPONSE + 1);
        if (inflateInit2(stream, MAX_WBITS + 16) != Z_OK)
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("remote gzip initialization failed")));
        PG_TRY();
        {
            int status;

            stream->next_in = (Bytef *) request->data;
            stream->avail_in = len;
            do
            {
                CHECK_FOR_INTERRUPTS();
                stream->next_out = (Bytef *) result + stream->total_out;
                stream->avail_out = Min((size_t) 65536, PGOS_MAX_RESPONSE + 1 - stream->total_out);
                status = inflate(stream, Z_NO_FLUSH);
                if (stream->total_out > PGOS_MAX_RESPONSE)
                    ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("remote response exceeds byte limit")));
                if (status != Z_OK && status != Z_STREAM_END)
                    ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid remote gzip body")));
            } while (status != Z_STREAM_END);
            /* Do not silently ignore corrupt tails or concatenate extra payloads. */
            if (stream->avail_in != 0)
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("trailing remote gzip data")));
            len = stream->total_out;
        }
        PG_FINALLY();
        {
            inflateEnd(stream);
            pfree(stream);
        }
        PG_END_TRY();
        result[len] = '\0';
    }
    else
        result = pnstrdup(request->data ? request->data : "", len);
    if (!len || memchr(result, '\0', len))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid remote response body")));
    return result;
}

static char *
http_request(const char *url, const char *api_key, const char *body,
               int timeout_ms, const char *ca_file)
{
    Request *request = palloc0(sizeof(Request));
    char *result = NULL;

    if (body && strlen(body) > PGOS_MAX_REQUEST)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("remote request exceeds byte limit")));
    init_pool();               /* lazy: never initialize handles in the postmaster */
    request->body = body;
    request->body_len = body ? strlen(body) : 0;
    PG_TRY();
    {
        int running;
        int messages;
        CURLMsg *message;
        CURLcode transfer = CURLE_FAILED_INIT;
        long status = 0;

        request->own_multi = body == NULL;
        request->multi = request->own_multi ? curl_multi_init() : pool;
        if (!request->multi)
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("remote HTTP allocation failed")));

        request->easy = curl_easy_init();
        if (!request->easy)
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("remote HTTP allocation failed")));
        if (body)
        {
            char *authorization = psprintf("x-api-key: %s", api_key);

            add_header(request, authorization);
            pfree(authorization);
            add_header(request, "Content-Type: application/json");
            add_header(request, "Expect:");
        }
        SETOPT(CURLOPT_URL, url);
        SETOPT(CURLOPT_PROTOCOLS_STR, "https");
        SETOPT(CURLOPT_FOLLOWLOCATION, 0L);
        SETOPT(CURLOPT_PATH_AS_IS, 1L);
        SETOPT(CURLOPT_PROXY, "");        /* no implicit process proxy routing */
        SETOPT(CURLOPT_NETRC, CURL_NETRC_IGNORED);
        SETOPT(CURLOPT_SSL_VERIFYPEER, 1L);
        SETOPT(CURLOPT_SSL_VERIFYHOST, 2L);
        if (ca_file && ca_file[0])
            SETOPT(CURLOPT_CAINFO, ca_file);
        SETOPT(CURLOPT_NOSIGNAL, 1L);
        SETOPT(CURLOPT_CONNECTTIMEOUT_MS, (long) Min(timeout_ms, 3000));
        SETOPT(CURLOPT_TIMEOUT_MS, (long) timeout_ms);
        SETOPT(CURLOPT_TCP_KEEPALIVE, 1L);
        SETOPT(CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
        SETOPT(CURLOPT_HTTPHEADER, request->headers);
        if (body)
        {
            SETOPT(CURLOPT_POST, 1L);
            SETOPT(CURLOPT_READFUNCTION, send_body);
            SETOPT(CURLOPT_READDATA, request);
            SETOPT(CURLOPT_SEEKFUNCTION, refuse_replay);
            SETOPT(CURLOPT_SEEKDATA, request);
            SETOPT(CURLOPT_POSTFIELDSIZE, (long) request->body_len);
        }
        else
        {
            SETOPT(CURLOPT_HTTPGET, 1L);
            SETOPT(CURLOPT_FRESH_CONNECT, 1L);
            SETOPT(CURLOPT_FORBID_REUSE, 1L);
        }
        SETOPT(CURLOPT_ACCEPT_ENCODING, "gzip");
        SETOPT(CURLOPT_HTTP_CONTENT_DECODING, 0L);
        SETOPT(CURLOPT_HEADERFUNCTION, receive_header);
        SETOPT(CURLOPT_HEADERDATA, request);
        SETOPT(CURLOPT_WRITEFUNCTION, receive_body);
        SETOPT(CURLOPT_WRITEDATA, request);
        if (curl_multi_add_handle(request->multi, request->easy) != CURLM_OK)
            ereport(ERROR, (errmsg("remote HTTP scheduling failed")));
        request->attached = true;
        do
        {
            CHECK_FOR_INTERRUPTS();
            if (curl_multi_perform(request->multi, &running) != CURLM_OK)
                ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("remote HTTP processing failed")));
            CHECK_FOR_INTERRUPTS();
            if (running && curl_multi_poll(request->multi, NULL, 0, 100, NULL) != CURLM_OK)
                ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("remote HTTP wait failed")));
        } while (running);
        while ((message = curl_multi_info_read(request->multi, &messages)) != NULL)
            if (message->msg == CURLMSG_DONE && message->easy_handle == request->easy)
                transfer = message->data.result;
        if (request->too_large)
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("remote response exceeds byte limit")));
        if (request->out_of_memory)
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("remote HTTP allocation failed")));
        if (transfer != CURLE_OK)
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                            errmsg("remote HTTP transport failed (curl code %d)", (int) transfer)));
        if (curl_easy_getinfo(request->easy, CURLINFO_RESPONSE_CODE, &status) != CURLE_OK)
            ereport(ERROR, (errmsg("remote HTTP status unavailable")));
        if (status != 200)
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                            errmsg("remote HTTP returned status %ld", status)));
        result = decode_body(request);
    }
    PG_FINALLY();
    {
        /* Also runs after cancellation, allocation errors, or transaction abort. */
        if (request->attached)
            curl_multi_remove_handle(request->multi, request->easy);
        if (request->easy)
            curl_easy_cleanup(request->easy);
        if (request->own_multi && request->multi)
            curl_multi_cleanup(request->multi);
        curl_slist_free_all(request->headers);
        free(request->data);
        pfree(request);
    }
    PG_END_TRY();
    return result;
}

char *
pgos_http_post(const char *url, const char *api_key, const char *body,
               int timeout_ms, const char *ca_file)
{
    return http_request(url, api_key, body, timeout_ms, ca_file);
}

char *
pgos_http_get(const char *url, int timeout_ms, const char *ca_file)
{
    return http_request(url, NULL, NULL, timeout_ms, ca_file);
}
