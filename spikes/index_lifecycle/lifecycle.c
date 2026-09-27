/* A local, permanently unpublished IAM lifecycle experiment. No remote I/O. */
#include "postgres.h"
#include "access/amapi.h"
#include "access/generic_xlog.h"
#include "access/heapam.h"
#include "access/relation.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#include "catalog/namespace.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "optimizer/cost.h"
#include "parser/parse_func.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/uuid.h"

#if PG_VERSION_NUM < 180000 || PG_VERSION_NUM >= 190000
#error "This lifecycle probe targets PostgreSQL 18 only"
#endif

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(pgos_lifecycle_handler);
PG_FUNCTION_INFO_V1(pgos_lifecycle_info);

#define PROBE_MAGIC 0x50474f53
#define PROBE_VERSION 1
#define MODE_TEXT 1
#define MODE_VECTOR 2

typedef struct ProbeMeta
{
    uint32 magic;
    uint32 version;
    pg_uuid_t generation;
    Oid heap;
    int16 key_attnum;
    int16 value_attnum;
    int32 dimensions;
    int32 mode;
} ProbeMeta;

typedef struct BuildState
{
    Oid cosine;
    double tuples;
} BuildState;

static void
require_superuser(void)
{
    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("lifecycle probe requires superuser")));
}

static void
unsupported(void)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("unsupported lifecycle probe index definition")));
}

static void
unpublished(void)
{
    ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("lifecycle probe index has no published search state"),
                    errhint("This test module only exercises local index lifecycle.")));
}

static Oid
cosine_function(Relation index)
{
    Oid type = TupleDescAttr(RelationGetDescr(index), 0)->atttypid;
    Oid signature[2] = {type, type};

    if (type == TEXTOID)
        return InvalidOid;
    return LookupFuncName(list_make2(makeString("onesearch"), makeString("cosine_distance")),
                          2, signature, false);
}

static void
validate_value(Oid cosine, Datum value, bool isnull)
{
    if (!isnull && OidIsValid(cosine))
        (void) OidFunctionCall2(cosine, value, value);
}

static void
build_callback(Relation index, ItemPointer tid, Datum *values, bool *isnull,
               bool alive, void *opaque)
{
    BuildState *state = opaque;

    validate_value(state->cosine, values[0], isnull[0]);
    if (!isnull[0])
        state->tuples++;
}

static IndexBuildResult *
probe_build(Relation heap, Relation index, IndexInfo *info)
{
    ProbeMeta meta = {0};
    Relation pk;
    Oid pk_oid;
    Oid type;
    Form_pg_attribute attribute;
    BuildState state = {0};
    IndexBuildResult *result;
    Buffer buffer;
    GenericXLogState *wal;
    Page page;

    require_superuser();
    if (heap->rd_rel->relkind != RELKIND_RELATION || heap->rd_rel->relispartition ||
        heap->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT ||
        heap->rd_rel->relam != HEAP_TABLE_AM_OID || heap->rd_rel->relrowsecurity ||
        info->ii_Concurrent || info->ii_NumIndexAttrs != 1 || info->ii_Expressions ||
        info->ii_Predicate || info->ii_IndexAttrNumbers[0] <= 0)
        unsupported();
    attribute = TupleDescAttr(RelationGetDescr(heap), info->ii_IndexAttrNumbers[0] - 1);
    type = attribute->atttypid;
    state.cosine = cosine_function(index);
    if (type != TEXTOID && !OidIsValid(state.cosine))
        unsupported();
    if (OidIsValid(state.cosine) && attribute->atttypmod < 2)
        unsupported();
    pk_oid = RelationGetPrimaryKeyIndex(heap, false);
    if (!OidIsValid(pk_oid))
        unsupported();
    pk = index_open(pk_oid, AccessShareLock);
    if (pk->rd_index->indnkeyatts != 1 || !pk->rd_index->indisvalid ||
        pk->rd_index->indkey.values[0] <= 0 ||
        TupleDescAttr(RelationGetDescr(heap), pk->rd_index->indkey.values[0] - 1)->atttypid != INT8OID)
        unsupported();
    meta.key_attnum = pk->rd_index->indkey.values[0];
    index_close(pk, AccessShareLock);
    if (RelationGetNumberOfBlocks(index) != 0)
        elog(ERROR, "lifecycle probe build requires an empty index");
    result = palloc0(sizeof(IndexBuildResult));
    result->heap_tuples = table_index_build_scan(heap, index, info, true, true, build_callback, &state, NULL);
    /* This count is only build validation, not stored TIDs or searchable hits. */
    result->index_tuples = state.tuples;
    meta.magic = PROBE_MAGIC;
    meta.version = PROBE_VERSION;
    meta.heap = RelationGetRelid(heap);
    meta.value_attnum = info->ii_IndexAttrNumbers[0];
    meta.mode = OidIsValid(state.cosine) ? MODE_VECTOR : MODE_TEXT;
    meta.dimensions = meta.mode == MODE_VECTOR ? attribute->atttypmod : 0;
    if (!pg_strong_random(meta.generation.data, UUID_LEN))
        elog(ERROR, "could not generate lifecycle probe identity");
    meta.generation.data[6] = (meta.generation.data[6] & 0x0f) | 0x40;
    meta.generation.data[8] = (meta.generation.data[8] & 0x3f) | 0x80;
    buffer = ReadBuffer(index, P_NEW);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    wal = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(wal, buffer, GENERIC_XLOG_FULL_IMAGE);
    PageInit(page, BLCKSZ, 0);
    memcpy(PageGetContents(page), &meta, sizeof(meta));
    ((PageHeader) page)->pd_lower = MAXALIGN(SizeOfPageHeaderData) + sizeof(meta);
    GenericXLogFinish(wal);
    UnlockReleaseBuffer(buffer);
    return result;
}

static void
probe_buildempty(Relation index)
{
    unsupported(); /* UNLOGGED init forks are outside the experiment. */
}

static bool
probe_insert(Relation index, Datum *values, bool *isnull, ItemPointer tid,
             Relation heap, IndexUniqueCheck unique, bool unchanged, IndexInfo *info)
{
    require_superuser();
    validate_value(cosine_function(index), values[0], isnull[0]);
    /* No tuple storage, outbox, or remote writes. The index stays unpublished. */
    return false;
}

static IndexBulkDeleteResult *
probe_vacuum(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    if (!stats)
        stats = palloc0(sizeof(IndexBulkDeleteResult));
    stats->num_pages = RelationGetNumberOfBlocks(info->index);
    stats->num_index_tuples = 0;
    return stats;
}

static IndexBulkDeleteResult *
probe_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                 IndexBulkDeleteCallback callback, void *opaque)
{
    return probe_vacuum(info, stats); /* There are no stored TIDs to retire. */
}

static void
probe_cost(PlannerInfo *root, IndexPath *path, double loops, Cost *startup,
           Cost *total, Selectivity *selectivity, double *correlation, double *pages)
{
    *startup = 1e10;
    *total = 1e10;
    *selectivity = 1;
    *correlation = 0;
    *pages = 1;
}

static bytea *
probe_options(Datum options, bool validate)
{
    if (validate && untransformRelOptions(options) != NIL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("lifecycle probe accepts no index options")));
    return NULL;
}

static bool
probe_validate(Oid opclass)
{
    /* Operator definitions are trusted test setup, not a product opclass ABI. */
    return true;
}

static IndexScanDesc
probe_beginscan(Relation index, int keys, int orderbys)
{
    unpublished();
    return NULL;
}

static void
probe_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
    unpublished();
}

static bool
probe_gettuple(IndexScanDesc scan, ScanDirection direction)
{
    unpublished();
    return false;
}

static void
probe_endscan(IndexScanDesc scan)
{
}

Datum
pgos_lifecycle_handler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *am = makeNode(IndexAmRoutine);

    am->amstrategies = 1;
    am->amcanorderbyop = true;
    am->amoptionalkey = true;
    am->ambuild = probe_build;
    am->ambuildempty = probe_buildempty;
    am->aminsert = probe_insert;
    am->ambulkdelete = probe_bulkdelete;
    am->amvacuumcleanup = probe_vacuum;
    am->amcostestimate = probe_cost;
    am->amoptions = probe_options;
    am->amvalidate = probe_validate;
    am->ambeginscan = probe_beginscan;
    am->amrescan = probe_rescan;
    am->amgettuple = probe_gettuple;
    am->amendscan = probe_endscan;
    PG_RETURN_POINTER(am);
}

Datum
pgos_lifecycle_info(PG_FUNCTION_ARGS)
{
    Relation index;
    Buffer buffer;
    Page page;
    ProbeMeta meta;
    char *uuid;
    char *json;

    require_superuser();
    index = relation_open(PG_GETARG_OID(0), AccessShareLock);
    if (index->rd_rel->relkind != RELKIND_INDEX ||
        index->rd_rel->relam != get_am_oid("pgos_lifecycle", false))
        unsupported();
    if (RelationGetNumberOfBlocks(index) != 1)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("invalid lifecycle probe metadata length")));
    buffer = ReadBuffer(index, 0);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buffer);
    if (PageIsNew(page) || ((PageHeader) page)->pd_lower != MAXALIGN(SizeOfPageHeaderData) + sizeof(meta))
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("invalid lifecycle probe metadata page")));
    memcpy(&meta, PageGetContents(page), sizeof(meta));
    UnlockReleaseBuffer(buffer);
    if (meta.magic != PROBE_MAGIC || meta.version != PROBE_VERSION ||
        meta.heap != index->rd_index->indrelid ||
        (meta.mode != MODE_TEXT && meta.mode != MODE_VECTOR))
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("unsupported lifecycle probe metadata format")));
    uuid = DatumGetCString(DirectFunctionCall1(uuid_out, UUIDPGetDatum(&meta.generation)));
    json = psprintf("{\"generation\":\"%s\",\"format_version\":%u,\"heap_oid\":%u,"
                    "\"key_attnum\":%d,\"value_attnum\":%d,\"dimensions\":%d,"
                    "\"mode\":\"%s\",\"state\":\"unpublished\"}",
                    uuid, meta.version, meta.heap, meta.key_attnum, meta.value_attnum,
                    meta.dimensions, meta.mode == MODE_VECTOR ? "vector" : "text");
    /* Keep the index generation stable for this transaction. */
    relation_close(index, NoLock);
    PG_RETURN_DATUM(DirectFunctionCall1(jsonb_in, CStringGetDatum(json)));
}
