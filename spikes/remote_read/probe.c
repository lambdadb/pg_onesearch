/* Test-only LambdaDB adapter. Intentionally no index, mutation or snapshot claim. */
#include "postgres.h"

#include <curl/curl.h>

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/miscnodes.h"
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
check_origin(const char *origin)
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
        if (curl_url_get(url, CURLUPART_PATH, &part, 0) != CURLUE_OK || strcmp(part, "/") != 0)
            valid = false;
        curl_free(part);
        part = NULL;
        if (curl_url_get(url, CURLUPART_USER, &part, 0) == CURLUE_OK)
            valid = false;
        curl_free(part);
        part = NULL;
        if (curl_url_get(url, CURLUPART_QUERY, &part, 0) == CURLUE_OK)
            valid = false;
        curl_free(part);
        part = NULL;
        if (curl_url_get(url, CURLUPART_FRAGMENT, &part, 0) == CURLUE_OK)
            valid = false;
        curl_free(part);
    }
    curl_url_cleanup(url);
    if (!valid)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("remote base URL must be an HTTPS origin")));
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
    Datum parsed;
    Jsonb *result;
    JsonbValue *inline_docs;
    JsonbValue *docs;
    ErrorSaveContext errors = {T_ErrorSaveContext};

    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("remote probe requires superuser")));
    if (GetDatabaseEncoding() != PG_UTF8)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("remote probe requires UTF8 database")));
    if (!origin || !project || !key || !key[0] || strlen(key) > 8192 ||
        strpbrk(key, "\r\n"))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid remote connection settings")));
    check_origin(origin);
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
    response = pgos_http_post(url, key, body.data, timeout_ms, getenv("PGOS_PROBE_CA_FILE"));
    /* Soft JSON errors prevent response tokens/URLs from leaking in PG diagnostics. */
    if (!pg_verify_mbstr(PG_UTF8, response, strlen(response), true) ||
        !DirectInputFunctionCallSafe(jsonb_in, response, InvalidOid, -1, (Node *) &errors, &parsed))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("remote response is not valid JSON")));
    result = DatumGetJsonbP(parsed);
    if (!JB_ROOT_IS_OBJECT(result))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid remote query envelope")));
    inline_docs = field(result, "isDocsInline");
    docs = field(result, "docs");
    if (!inline_docs || inline_docs->type != jbvBool || !docs || docs->type != jbvBinary ||
        !JsonContainerIsArray(docs->val.binary.data))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid remote query envelope")));
    if (!inline_docs->val.boolean)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("offloaded query results are not supported by this probe")));
    PG_RETURN_JSONB_P(result);
}
