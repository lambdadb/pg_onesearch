#include "postgres.h"

#include <math.h>

#include "common/shortest_dec.h"
#include "fmgr.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/builtins.h"

#if PG_VERSION_NUM < 180000 || PG_VERSION_NUM >= 190000
#error "pg_onesearch currently targets PostgreSQL 18 only"
#endif

PG_MODULE_MAGIC;

#define MIN_DIM 2
#define MAX_DIM 4096

typedef struct OneVector
{
    int32 vl_len_;
    int32 dim;
    float4 values[FLEXIBLE_ARRAY_MEMBER];
} OneVector;

PG_FUNCTION_INFO_V1(onesearch_vector_in);
PG_FUNCTION_INFO_V1(onesearch_vector_out);
PG_FUNCTION_INFO_V1(onesearch_vector_recv);
PG_FUNCTION_INFO_V1(onesearch_vector_send);
PG_FUNCTION_INFO_V1(onesearch_vector_typmod_in);
PG_FUNCTION_INFO_V1(onesearch_vector_typmod_out);
PG_FUNCTION_INFO_V1(onesearch_vector_coerce);
PG_FUNCTION_INFO_V1(onesearch_cosine_distance);

static void
check_dim(int dim)
{
    if (dim < MIN_DIM || dim > MAX_DIM)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("vector dimensions must be between %d and %d", MIN_DIM, MAX_DIM)));
}

static void
check_typmod(int dim, int32 typmod)
{
    if (typmod == -1)
        return;
    check_dim(typmod);
    if (dim != typmod)
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("expected %d dimensions, got %d", typmod, dim)));
}

static void
check_value(float4 value)
{
    if (!isfinite(value))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("vector elements must be finite")));
}

static OneVector *
new_vector(int dim)
{
    Size size;
    OneVector *result;

    check_dim(dim);
    size = offsetof(OneVector, values) + sizeof(float4) * dim;
    result = palloc0(size);
    SET_VARSIZE(result, size);
    result->dim = dim;
    return result;
}

static const char *
skip_space(const char *p)
{
    while (*p == ' ' || *p == '\t' || *p == '\n' ||
           *p == '\r' || *p == '\f' || *p == '\v')
        p++;
    return p;
}

static void
invalid_text(void)
{
    ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("invalid input syntax for onesearch.vector"),
                    errhint("Use a bracketed, comma-separated list of finite numbers.")));
}

Datum
onesearch_vector_in(PG_FUNCTION_ARGS)
{
    const char *p = skip_space(PG_GETARG_CSTRING(0));
    int32 typmod = PG_GETARG_INT32(2);
    float4 values[MAX_DIM];
    int dim = 0;
    OneVector *result;

    if (*p++ != '[')
        invalid_text();
    p = skip_space(p);
    if (*p != ']')
    {
        for (;;)
        {
            const char *start = p;
            char *token;

            if (dim == MAX_DIM)
                check_dim(dim + 1);
            while (*p && *p != ',' && *p != ']')
                p++;
            if (p == start || !*p)
                invalid_text();
            token = pnstrdup(start, p - start);
            values[dim] = DatumGetFloat4(DirectFunctionCall1(float4in, CStringGetDatum(token)));
            pfree(token);
            check_value(values[dim++]);
            if (*p == ']')
                break;
            p = skip_space(p + 1);
        }
    }
    if (*skip_space(p + 1) != '\0')
        invalid_text();
    check_dim(dim);
    check_typmod(dim, typmod);
    result = new_vector(dim);
    memcpy(result->values, values, dim * sizeof(float4));
    PG_RETURN_POINTER(result);
}

Datum
onesearch_vector_out(PG_FUNCTION_ARGS)
{
    OneVector *v = (OneVector *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    StringInfoData buf;
    int i;

    initStringInfo(&buf);
    appendStringInfoChar(&buf, '[');
    for (i = 0; i < v->dim; i++)
    {
        char number[FLOAT_SHORTEST_DECIMAL_LEN];

        if (i)
            appendStringInfoChar(&buf, ',');
        float_to_shortest_decimal_buf(v->values[i], number);
        appendStringInfoString(&buf, number);
    }
    appendStringInfoChar(&buf, ']');
    PG_FREE_IF_COPY(v, 0);
    PG_RETURN_CSTRING(buf.data);
}

Datum
onesearch_vector_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
    int32 typmod = PG_GETARG_INT32(2);
    int dim = pq_getmsgint(buf, 4);
    OneVector *result;
    int i;

    check_dim(dim);
    check_typmod(dim, typmod);
    /* Validate length before reading or allocating element data. */
    if (buf->len - buf->cursor != dim * (int) sizeof(float4))
        ereport(ERROR, (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                        errmsg("invalid vector binary payload length")));
    result = new_vector(dim);
    for (i = 0; i < dim; i++)
    {
        result->values[i] = pq_getmsgfloat4(buf);
        check_value(result->values[i]);
    }
    pq_getmsgend(buf);
    PG_RETURN_POINTER(result);
}

Datum
onesearch_vector_send(PG_FUNCTION_ARGS)
{
    OneVector *v = (OneVector *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    StringInfoData buf;
    int i;

    pq_begintypsend(&buf);
    pq_sendint32(&buf, v->dim);
    for (i = 0; i < v->dim; i++)
        pq_sendfloat4(&buf, v->values[i]);
    PG_FREE_IF_COPY(v, 0);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum
onesearch_vector_typmod_in(PG_FUNCTION_ARGS)
{
    int n;
    int32 *mods = ArrayGetIntegerTypmods(PG_GETARG_ARRAYTYPE_P(0), &n);

    if (n != 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("vector requires exactly one dimension modifier")));
    check_dim(mods[0]);
    PG_RETURN_INT32(mods[0]);
}

Datum
onesearch_vector_typmod_out(PG_FUNCTION_ARGS)
{
    int32 typmod = PG_GETARG_INT32(0);

    if (typmod == -1)
        PG_RETURN_CSTRING(pstrdup(""));
    check_dim(typmod);
    PG_RETURN_CSTRING(psprintf("(%d)", typmod));
}

Datum
onesearch_vector_coerce(PG_FUNCTION_ARGS)
{
    OneVector *v = (OneVector *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));

    check_typmod(v->dim, PG_GETARG_INT32(1));
    PG_RETURN_POINTER(v);
}

Datum
onesearch_cosine_distance(PG_FUNCTION_ARGS)
{
    OneVector *a = (OneVector *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    OneVector *b = (OneVector *) PG_DETOAST_DATUM(PG_GETARG_DATUM(1));
    double dot = 0.0, aa = 0.0, bb = 0.0, similarity;
    int i;

    if (a->dim != b->dim)
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("vector dimensions differ: %d and %d", a->dim, b->dim)));
    /* float64 accumulation avoids overflow/underflow of finite float32 squares. */
    for (i = 0; i < a->dim; i++)
    {
        double x = a->values[i];
        double y = b->values[i];

        dot += x * y;
        aa += x * x;
        bb += y * y;
    }
    if (aa == 0.0 || bb == 0.0)
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("cosine distance is undefined for zero vectors")));
    similarity = dot / (sqrt(aa) * sqrt(bb));
    similarity = fmax(-1.0, fmin(1.0, similarity));
    PG_FREE_IF_COPY(a, 0);
    PG_FREE_IF_COPY(b, 1);
    PG_RETURN_FLOAT8(1.0 - similarity);
}
