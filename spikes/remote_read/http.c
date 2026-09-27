/* Backend-local, single-request transport candidate. No PG calls in curl callbacks. */
#include "postgres.h"

#include <curl/curl.h>

#include "miscadmin.h"
#include "storage/ipc.h"
#include "http.h"

typedef struct Request
{
    CURL *easy;
    struct curl_slist *headers;
    bool attached;
    char *data;                  /* malloc: callback must never longjmp through curl */
    size_t len;
    size_t capacity;
    bool too_large;
    bool out_of_memory;
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
    /* libcurl delivers decompressed bytes here; bound expansion as well. */
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

char *
pgos_http_post(const char *url, const char *api_key, const char *body,
               int timeout_ms, const char *ca_file)
{
    Request *request = palloc0(sizeof(Request));
    char *result = NULL;

    if (strlen(body) > PGOS_MAX_REQUEST)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("remote request exceeds byte limit")));
    init_pool();               /* lazy: never initialize handles in the postmaster */
    request->body = body;
    request->body_len = strlen(body);
    PG_TRY();
    {
        int running;
        int messages;
        CURLMsg *message;
        CURLcode transfer = CURLE_FAILED_INIT;
        long status = 0;
        char *authorization = psprintf("x-api-key: %s", api_key);

        request->easy = curl_easy_init();
        if (!request->easy)
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("remote HTTP allocation failed")));
        add_header(request, authorization);
        pfree(authorization);
        add_header(request, "Content-Type: application/json");
        add_header(request, "Expect:");
        SETOPT(CURLOPT_URL, url);
        SETOPT(CURLOPT_PROTOCOLS_STR, "https");
        SETOPT(CURLOPT_FOLLOWLOCATION, 0L);
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
        SETOPT(CURLOPT_POST, 1L);
        SETOPT(CURLOPT_READFUNCTION, send_body);
        SETOPT(CURLOPT_READDATA, request);
        SETOPT(CURLOPT_SEEKFUNCTION, refuse_replay);
        SETOPT(CURLOPT_SEEKDATA, request);
        SETOPT(CURLOPT_POSTFIELDSIZE, (long) strlen(body));
        SETOPT(CURLOPT_ACCEPT_ENCODING, "gzip");
        SETOPT(CURLOPT_WRITEFUNCTION, receive_body);
        SETOPT(CURLOPT_WRITEDATA, request);
        if (curl_multi_add_handle(pool, request->easy) != CURLM_OK)
            ereport(ERROR, (errmsg("remote HTTP scheduling failed")));
        request->attached = true;
        do
        {
            CHECK_FOR_INTERRUPTS();
            if (curl_multi_perform(pool, &running) != CURLM_OK)
                ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("remote HTTP processing failed")));
            CHECK_FOR_INTERRUPTS();
            if (running && curl_multi_poll(pool, NULL, 0, 100, NULL) != CURLM_OK)
                ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("remote HTTP wait failed")));
        } while (running);
        while ((message = curl_multi_info_read(pool, &messages)) != NULL)
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
        if (!request->len || memchr(request->data, '\0', request->len))
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid remote response body")));
        result = pnstrdup(request->data, request->len);
    }
    PG_FINALLY();
    {
        /* Also runs after cancellation, allocation errors, or transaction abort. */
        if (request->attached)
            curl_multi_remove_handle(pool, request->easy);
        if (request->easy)
            curl_easy_cleanup(request->easy);
        curl_slist_free_all(request->headers);
        free(request->data);
        pfree(request);
    }
    PG_END_TRY();
    return result;
}
