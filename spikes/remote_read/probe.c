/* Test-only LambdaDB adapter. Intentionally no index, mutation or snapshot claim. */
#include "postgres.h"

#include <curl/curl.h>

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/miscnodes.h"
#include "portability/instr_time.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "http.h"

#if PG_VERSION_NUM < 180000 || PG_VERSION_NUM >= 190000
#error "This probe targets PostgreSQL 18 only"
#endif

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(pgos_remote_query);
void _PG_init(void);

static int timeout_ms = 5000;

void
_PG_init(void)
{
    DefineCustomIntVariable("pgos_remote_probe.timeout_ms", "Total HTTP request deadline.",
                            NULL, &timeout_ms, 5000, 100, 60000, PGC_SUSET, 0, NULL, NULL, NULL);
}

static void
check_name(const char *name)
{
    const unsigned char *p = (const unsigned char *) name;

    if (!*p || strlen(name) > 128)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid remote resource name")));
    for (; *p; p++)
        if (!((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
              (*p >= '0' && *p <= '9') || *p == '-' || *p == '_'))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid remote resource name")));
}

static void
check_url(const char *origin, bool download)
{
    CURLU *url = curl_url();
    char *part = NULL;
    bool valid = false;

    if (!url)
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("remote URL allocation failed")));
    /* No PG calls until all libcurl URL allocations are released. */
    if (curl_url_set(url, CURLUPART_URL, origin, 0) == CURLUE_OK &&
        curl_url_get(url, CURLUPART_SCHEME, &part, 0) == CURLUE_OK)
    {
        valid = strcmp(part, "https") == 0;
        curl_free(part);
        part = NULL;
        if (curl_url_get(url, CURLUPART_PATH, &part, 0) != CURLUE_OK ||
            (!download && strcmp(part, "/") != 0))
            valid = false;
        curl_free(part);
        part = NULL;
        if (curl_url_get(url, CURLUPART_USER, &part, 0) == CURLUE_OK)
            valid = false;
        curl_free(part);
        part = NULL;
        if (curl_url_get(url, CURLUPART_QUERY, &part, 0) == CURLUE_OK && !download)
            valid = false;
        curl_free(part);
        part = NULL;
        if (curl_url_get(url, CURLUPART_FRAGMENT, &part, 0) == CURLUE_OK)
            valid = false;
        curl_free(part);
    }
    curl_url_cleanup(url);
    if (!valid)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid remote HTTPS URL")));
}

static JsonbValue *
field(Jsonb *object, const char *name)
{
    JsonbValue key;

    key.type = jbvString;
    key.val.string.val = (char *) name;
    key.val.string.len = strlen(name);
    return findJsonbValueFromContainer(&object->root, JB_FOBJECT, &key);
}

static int
remaining_timeout(instr_time started)
{
    instr_time elapsed;
    int remaining;

    CHECK_FOR_INTERRUPTS();
    INSTR_TIME_SET_CURRENT(elapsed);
    INSTR_TIME_SUBTRACT(elapsed, started);
    remaining = timeout_ms - (int) INSTR_TIME_GET_MILLISEC(elapsed);
    if (remaining <= 0)
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("remote query deadline exceeded")));
    return remaining;
}

static Jsonb *
parse_response(char *response)
{
    Datum parsed;
    ErrorSaveContext errors = {T_ErrorSaveContext};

    /* Never include remote JSON tokens in parser diagnostics. */
    if (!pg_verify_mbstr(PG_UTF8, response, strlen(response), true) ||
        !DirectInputFunctionCallSafe(jsonb_in, response, InvalidOid, -1, (Node *) &errors, &parsed))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("remote response is not valid JSON")));
    return DatumGetJsonbP(parsed);
}

static void
validate_docs(Jsonb *docs, int size)
{
    JsonbIterator *iterator;
    JsonbIteratorToken token;
    JsonbValue item;
    JsonbValue *ids[100];
    int count = 0;

    if (!JB_ROOT_IS_ARRAY(docs) || JB_ROOT_IS_SCALAR(docs) || JB_ROOT_COUNT(docs) > size)
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid remote result array")));
    iterator = JsonbIteratorInit(&docs->root);
    while ((token = JsonbIteratorNext(&iterator, &item, true)) != WJB_DONE)
    {
        Jsonb *object;
        JsonbValue *doc;
        JsonbValue *id;
        JsonbValue *score;
        int i;

        if (token != WJB_ELEM)
            continue;
        CHECK_FOR_INTERRUPTS();
        if (item.type != jbvBinary || !JsonContainerIsObject(item.val.binary.data))
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid remote result item")));
        object = JsonbValueToJsonb(&item);
        doc = field(object, "doc");
        score = field(object, "score");
        if (!doc || doc->type != jbvBinary || !JsonContainerIsObject(doc->val.binary.data) ||
            !score || score->type != jbvNumeric)
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid remote document or score")));
        id = field(JsonbValueToJsonb(doc), "id");
        if (!id || id->type != jbvString || !id->val.string.len)
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid remote document identity")));
        for (i = 0; i < count; i++)
            if (ids[i]->val.string.len == id->val.string.len &&
                memcmp(ids[i]->val.string.val, id->val.string.val, id->val.string.len) == 0)
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("duplicate remote document identity")));
        ids[count++] = id;
    }
}

static Jsonb *
hydrate_result(Jsonb *result, int size, instr_time started)
{
    JsonbValue *inline_docs;
    JsonbValue *docs_value;
    Jsonb *docs;
    Jsonb *patch;
    StringInfoData normalized;
    bool offloaded;

    if (!JB_ROOT_IS_OBJECT(result))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid remote query envelope")));
    inline_docs = field(result, "isDocsInline");
    docs_value = field(result, "docs");
    if (!inline_docs || inline_docs->type != jbvBool || !docs_value || docs_value->type != jbvBinary ||
        !JsonContainerIsArray(docs_value->val.binary.data))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid remote query envelope")));
    offloaded = !inline_docs->val.boolean;
    docs = JsonbValueToJsonb(docs_value);
    if (offloaded)
    {
        JsonbValue *link = field(result, "docsUrl");
        char *url;
        char *response;

        if (JB_ROOT_COUNT(docs) != 0 || !link || link->type != jbvString ||
            link->val.string.len == 0 || link->val.string.len > 16384)
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid remote download envelope")));
        url = pnstrdup(link->val.string.val, link->val.string.len);
        check_url(url, true);
        response = pgos_http_get(url, remaining_timeout(started), getenv("PGOS_PROBE_CA_FILE"));
        docs = parse_response(response);
    }
    validate_docs(docs, size);
    /* Preserve API metadata, replace docs, and never return signed URLs to SQL.
     * wasOffloaded is probe-only evidence that the C download path was used. */
    initStringInfo(&normalized);
    appendStringInfo(&normalized, "{\"isDocsInline\":true,\"wasOffloaded\":%s,\"docs\":%s}",
                     offloaded ? "true" : "false", JsonbToCString(NULL, &docs->root, VARSIZE(docs)));
    patch = parse_response(normalized.data);
    result = DatumGetJsonbP(DirectFunctionCall2(jsonb_concat, JsonbPGetDatum(result), JsonbPGetDatum(patch)));
    result = DatumGetJsonbP(DirectFunctionCall2(jsonb_delete, JsonbPGetDatum(result), CStringGetTextDatum("docsUrl")));
    (void) remaining_timeout(started);
    return result;
}

Datum
pgos_remote_query(PG_FUNCTION_ARGS)
{
    const char *origin = getenv("LAMBDADB_BASE_URL");
    const char *project = getenv("LAMBDADB_PROJECT_NAME");
    const char *key = getenv("LAMBDADB_PROJECT_API_KEY");
    char *collection;
    char *tag;
    Jsonb *query;
    int size;
    StringInfoData body;
    char *url;
    char *response;
    Jsonb *result;
    instr_time started;

    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("remote probe requires superuser")));
    if (GetDatabaseEncoding() != PG_UTF8)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("remote probe requires UTF8 database")));
    if (!origin || !project || !key || !key[0] || strlen(key) > 8192 ||
        strpbrk(key, "\r\n"))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid remote connection settings")));
    check_url(origin, false);
    check_name(project);
    collection = text_to_cstring(PG_GETARG_TEXT_PP(0));
    tag = text_to_cstring(PG_GETARG_TEXT_PP(1));
    query = PG_GETARG_JSONB_P(2);
    size = PG_GETARG_INT32(3);
    check_name(collection);
    check_name(tag);
    if (!JB_ROOT_IS_OBJECT(query) || size < 1 || size > 100)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("query requires an object and size 1..100")));
    url = psprintf("%.*s/projects/%s/collections/%s/query",
                   (int) strlen(origin) - (origin[strlen(origin)-1] == '/'), origin, project, collection);
    initStringInfo(&body);
    appendStringInfoString(&body, "{\"ref\":{\"kind\":\"tag\",\"name\":");
    escape_json(&body, tag);
    appendStringInfo(&body, "},\"size\":%d,\"includeVectors\":true,\"query\":%s}",
                     size, JsonbToCString(NULL, &query->root, VARSIZE(query)));
    INSTR_TIME_SET_CURRENT(started);
    response = pgos_http_post(url, key, body.data, remaining_timeout(started), getenv("PGOS_PROBE_CA_FILE"));
    result = hydrate_result(parse_response(response), size, started);
    PG_RETURN_JSONB_P(result);
}
