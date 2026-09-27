/* Test-only statement-snapshot reader. No planner hooks or durable TID cache. */
#include "postgres.h"
#include "access/table.h"
#include "catalog/index.h"
#include "catalog/pg_type_d.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(pgos_snapshot_view);
PG_FUNCTION_INFO_V1(pgos_snapshot_search);
void _PG_init(void);
static int pause_before_capture;
static int pause_after_capture;

void
_PG_init(void)
{
    DefineCustomIntVariable("pgos_snapshot_probe.pause_before_capture", "Test synchronization lock.", NULL,
                            &pause_before_capture, 0, 0, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pgos_snapshot_probe.pause_after_capture", "Test synchronization lock.", NULL,
                            &pause_after_capture, 0, 0, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
}

static Datum
select_one(const char *sql, int nargs, Oid *types, Datum *args, Snapshot snapshot, bool *isnull)
{
    SPIPlanPtr plan = SPI_prepare(sql, nargs, types);
    Datum result;
    int status;

    if (!plan)
        elog(ERROR, "snapshot probe could not prepare query");
    status = SPI_execute_snapshot(plan, args, NULL, snapshot, InvalidSnapshot, true, false, 1);
    if (status != SPI_OK_SELECT || SPI_processed != 1)
        elog(ERROR, "snapshot probe expected exactly one result");
    result = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, isnull);
    SPI_freeplan(plan);
    return result;
}

static void
check_health(Oid index)
{
    Oid types[] = {OIDOID};
    Datum args[] = {ObjectIdGetDatum(index)};
    bool isnull;
    Datum ok = select_one(
        "SELECT EXISTS (SELECT FROM pgos_capture_probe.generations g "
        "JOIN pgos_snapshot_probe.health h USING(generation) "
        "WHERE g.index_oid=$1 AND g.state='capturing' AND h.healthy)",
        1, types, args, GetLatestSnapshot(), &isnull);

    if (isnull || !DatumGetBool(ok))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("snapshot probe index is unavailable or degraded")));
}

static void
pause_at(int key, Snapshot snapshot)
{
    if (key)
    {
        Oid types[] = {INT8OID};
        Datum args[] = {Int64GetDatum(key)};
        bool isnull;
        (void) select_one("SELECT pg_advisory_xact_lock($1)", 1, types, args, snapshot, &isnull);
    }
}

static Jsonb *
run(Oid index, Jsonb *query)
{
    MemoryContext caller = CurrentMemoryContext;
    Snapshot snapshot;
    Relation heap;
    Jsonb *view;
    Jsonb *result;
    Oid types[] = {OIDOID};
    Datum args[] = {ObjectIdGetDatum(index)};
    Datum value;
    bool isnull;

    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("snapshot probe requires superuser")));
    if (!ActiveSnapshotSet())
        elog(ERROR, "snapshot probe requires an active statement snapshot");
    /* Heap then IAM info's index lock, retained to transaction end. Ordinary DML/VACUUM continue. */
    heap = table_open(IndexGetRelation(index, false), AccessShareLock);
    table_close(heap, NoLock);
    snapshot = RegisterSnapshot(GetActiveSnapshot());
    SPI_connect();
    check_health(index); /* Operational health deliberately does not use the reader's old snapshot. */
    pause_at(pause_before_capture, snapshot);
    value = select_one("SELECT pgos_snapshot_probe.assemble($1::regclass)", 1, types, args, snapshot, &isnull);
    if (isnull)
        elog(ERROR, "snapshot probe returned a null view");
    MemoryContextSwitchTo(caller);
    view = DatumGetJsonbPCopy(value);
    pause_at(pause_after_capture, snapshot);
    result = view;
    if (query)
    {
        Oid query_types[] = {JSONBOID, JSONBOID};
        Datum query_args[] = {JsonbPGetDatum(view), JsonbPGetDatum(query)};
        value = select_one("SELECT pgos_snapshot_probe.search_view($1,$2)", 2, query_types, query_args, snapshot, &isnull);
        if (isnull)
            elog(ERROR, "snapshot probe returned a null search result");
        MemoryContextSwitchTo(caller);
        result = DatumGetJsonbPCopy(value);
    }
    check_health(index); /* Bound the promise to this last check, before materialized results return. */
    SPI_finish();
    UnregisterSnapshot(snapshot);
    return result;
}

Datum
pgos_snapshot_view(PG_FUNCTION_ARGS)
{
    PG_RETURN_JSONB_P(run(PG_GETARG_OID(0), NULL));
}

Datum
pgos_snapshot_search(PG_FUNCTION_ARGS)
{
    PG_RETURN_JSONB_P(run(PG_GETARG_OID(0), PG_GETARG_JSONB_P(1)));
}
