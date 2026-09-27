/* Test-only PG18 hook/worker probe. Never load this in an application cluster. */
#include "postgres.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "portability/instr_time.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"

PG_MODULE_MAGIC;

#define NSLOTS 16

typedef struct Waiter
{
    int pid;
    uint64 xid;
    Oid database;
    int phase;                  /* 0 reserved, 1 commit callback, 2 after locks */
    int outcome;                /* 0 pending, 1 published, 2 mock failure */
} Waiter;

typedef struct ProbeShared
{
    slock_t mutex;
    int worker_pid;
    bool starting;
    int mode;                   /* 0 run, 1 paused, 2 fail, 3 before commit, 4 after commit */
    int worker_stage;           /* 0 idle, 1 reading/locking, 2 before commit, 3 before notify */
    Waiter waiters[NSLOTS];
} ProbeShared;

typedef struct Mark
{
    struct Mark *next;
    SubTransactionId subid;
} Mark;

static ProbeShared *shared;
static shmem_request_hook_type previous_request;
static shmem_startup_hook_type previous_startup;
static Mark *marks;
static int my_slot = -1;
static bool committed;
static bool cleanup_registered;
static int wait_phase = 1;
static int wait_ms = 1000;
static uint32 wait_event;
static char last_result[160] = "none";

void _PG_init(void);
PGDLLEXPORT void onesearch_probe_worker(Datum arg);
PG_FUNCTION_INFO_V1(onesearch_probe_mark);
PG_FUNCTION_INFO_V1(onesearch_probe_start);
PG_FUNCTION_INFO_V1(onesearch_probe_start_publication);
PG_FUNCTION_INFO_V1(onesearch_probe_control);
PG_FUNCTION_INFO_V1(onesearch_probe_status);
PG_FUNCTION_INFO_V1(onesearch_probe_last);

static void publication_iteration(uint32 event);

static void
request_shared(void)
{
    if (previous_request)
        previous_request();
    RequestAddinShmemSpace(sizeof(ProbeShared));
}

static void
startup_shared(void)
{
    bool found;

    if (previous_startup)
        previous_startup();
    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    shared = ShmemInitStruct("pg_onesearch commit probe", sizeof(ProbeShared), &found);
    if (!found)
    {
        memset(shared, 0, sizeof(ProbeShared));
        SpinLockInit(&shared->mutex);
        shared->mode = 1;
    }
    LWLockRelease(AddinShmemInitLock);
}

static void
release_slot(void)
{
    if (my_slot >= 0)
    {
        SpinLockAcquire(&shared->mutex);
        memset(&shared->waiters[my_slot], 0, sizeof(Waiter));
        SpinLockRelease(&shared->mutex);
        my_slot = -1;
    }
}

static void
exit_cleanup(int code, Datum arg)
{
    release_slot();
    SpinLockAcquire(&shared->mutex);
    if (shared->worker_pid == MyProcPid)
        shared->worker_pid = 0;
    SpinLockRelease(&shared->mutex);
}

/*
 * No SPI, snapshots, network, resource acquisition or CHECK_FOR_INTERRUPTS in
 * the commit path. PG holds interrupts here. Bound the wait independently and
 * observe cancel/die flags without consuming them or inventing a rollback.
 * This is a feasibility probe, not an approved use of the cleanup callback.
 */
static void
wait_for_publication(int phase)
{
    instr_time start, now;
    uint64 xid;
    int outcome = 0;
    int elapsed = 0;
    const char *reason = "timeout";

    SpinLockAcquire(&shared->mutex);
    shared->waiters[my_slot].phase = phase;
    xid = shared->waiters[my_slot].xid;
    SpinLockRelease(&shared->mutex);
    INSTR_TIME_SET_CURRENT(start);
    for (;;)
    {
        ResetLatch(MyLatch);
        SpinLockAcquire(&shared->mutex);
        outcome = shared->waiters[my_slot].outcome;
        SpinLockRelease(&shared->mutex);
        INSTR_TIME_SET_CURRENT(now);
        INSTR_TIME_SUBTRACT(now, start);
        elapsed = (int) INSTR_TIME_GET_MILLISEC(now);
        if (outcome != 0)
        {
            reason = outcome == 1 ? "published" : "mock_failure";
            break;
        }
        if (QueryCancelPending || ProcDiePending)
        {
            reason = "interrupted";
            break;
        }
        if (elapsed >= wait_ms)
            break;
        WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                  Min(10, wait_ms - elapsed), wait_event);
    }
    snprintf(last_result, sizeof(last_result),
             "xid=" UINT64_FORMAT " phase=%d outcome=%s holdoff=%d elapsed_ms=%d",
             xid, phase, reason, InterruptHoldoffCount, elapsed);
    release_slot();
    committed = false;
    if (outcome != 1)
        ereport(WARNING,
                (errcode(ERRCODE_WARNING),
                 errmsg("pg_onesearch probe: source committed; synchronization incomplete"),
                 errdetail("xid=" UINT64_FORMAT "; %s; check durable publication status; unpublished replay retained", xid, reason)));
}

static void
xact_event(XactEvent event, void *arg)
{
    if (event == XACT_EVENT_PRE_PREPARE && marks)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("commit probe does not support prepared source transactions")));
    if (event == XACT_EVENT_PRE_COMMIT && marks)
    {
        int i;
        uint64 xid = U64FromFullTransactionId(GetTopFullTransactionId());

        wait_event = WaitEventExtensionNew("OnesearchProbePublication");
        SpinLockAcquire(&shared->mutex);
        for (i = 0; i < NSLOTS; i++)
            if (shared->waiters[i].pid == 0)
            {
                my_slot = i;
                shared->waiters[i].pid = MyProcPid;
                shared->waiters[i].xid = xid;
                shared->waiters[i].database = MyDatabaseId;
                break;
            }
        SpinLockRelease(&shared->mutex);
        if (my_slot < 0)
            ereport(ERROR, (errmsg("commit probe waiter capacity exceeded")));
    }
    else if (event == XACT_EVENT_COMMIT)
    {
        marks = NULL;
        committed = my_slot >= 0;
        if (committed && wait_phase == 0)
            wait_for_publication(1);
    }
    else if (event == XACT_EVENT_ABORT)
    {
        marks = NULL;
        committed = false;
        release_slot();
    }
}

static void
resource_event(ResourceReleasePhase phase, bool isCommit, bool isTopLevel, void *arg)
{
    /* isTopLevel also applies to descendants: require the actual root owner. */
    if (phase == RESOURCE_RELEASE_AFTER_LOCKS && isCommit && isTopLevel &&
        CurrentResourceOwner == TopTransactionResourceOwner && committed)
        wait_for_publication(2);
}

static void
subxact_event(SubXactEvent event, SubTransactionId subid,
              SubTransactionId parent, void *arg)
{
    Mark **cursor = &marks;

    while (*cursor)
    {
        Mark *mark = *cursor;

        if (mark->subid == subid && event == SUBXACT_EVENT_ABORT_SUB)
        {
            *cursor = mark->next;
            pfree(mark);
            continue;
        }
        if (mark->subid == subid && event == SUBXACT_EVENT_COMMIT_SUB)
            mark->subid = parent;
        cursor = &mark->next;
    }
}

void
_PG_init(void)
{
    static const struct config_enum_entry phases[] = {
        {"commit", 0, false}, {"after_locks", 1, false}, {NULL, 0, false}
    };

    if (!process_shared_preload_libraries_in_progress)
        ereport(ERROR, (errmsg("commit probe must be loaded through shared_preload_libraries")));
    DefineCustomEnumVariable("onesearch_probe.wait_phase", "Test-only wait point.",
                             NULL, &wait_phase, 1, phases, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("onesearch_probe.wait_ms", "Test-only bounded wait.",
                            NULL, &wait_ms, 1000, 10, 10000, PGC_SUSET, 0, NULL, NULL, NULL);
    previous_request = shmem_request_hook;
    shmem_request_hook = request_shared;
    previous_startup = shmem_startup_hook;
    shmem_startup_hook = startup_shared;
    RegisterXactCallback(xact_event, NULL);
    RegisterSubXactCallback(subxact_event, NULL);
    RegisterResourceReleaseCallback(resource_event, NULL);
}

Datum
onesearch_probe_mark(PG_FUNCTION_ARGS)
{
    Mark *mark;

    if (!superuser())
        ereport(ERROR, (errmsg("commit probe is superuser-only")));
    if (!cleanup_registered)
    {
        before_shmem_exit(exit_cleanup, (Datum) 0);
        cleanup_registered = true;
    }
    mark = MemoryContextAlloc(TopTransactionContext, sizeof(Mark));
    mark->subid = GetCurrentSubTransactionId();
    mark->next = marks;
    marks = mark;
    PG_RETURN_VOID();
}

Datum
onesearch_probe_last(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text(last_result));
}

Datum
onesearch_probe_control(PG_FUNCTION_ARGS)
{
    int mode = PG_GETARG_INT32(0);

    if (!superuser() || mode < 0 || mode > 4)
        ereport(ERROR, (errmsg("invalid test-only worker mode")));
    SpinLockAcquire(&shared->mutex);
    shared->mode = mode;
    SpinLockRelease(&shared->mutex);
    PG_RETURN_VOID();
}

Datum
onesearch_probe_status(PG_FUNCTION_ARGS)
{
    int i, count = 0, pid, stage;

    SpinLockAcquire(&shared->mutex);
    pid = shared->worker_pid;
    stage = shared->worker_stage;
    for (i = 0; i < NSLOTS; i++)
        if (shared->waiters[i].pid && shared->waiters[i].phase)
            count++;
    SpinLockRelease(&shared->mutex);
    PG_RETURN_TEXT_P(cstring_to_text(psprintf("%d,%d,%d", pid, stage, count)));
}

static int
start_worker(bool publication)
{
    BackgroundWorker worker = {0};
    BackgroundWorkerHandle *handle;
    Oid role = GetUserId();
    int pid;
    bool exists;

    if (!superuser())
        ereport(ERROR, (errmsg("commit probe is superuser-only")));
    SpinLockAcquire(&shared->mutex);
    exists = shared->starting;
    shared->starting = true;
    SpinLockRelease(&shared->mutex);
    if (exists)
        ereport(ERROR, (errmsg("only one probe worker registration is allowed per cluster")));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 1;
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_onesearch_commit_probe");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "onesearch_probe_worker");
    snprintf(worker.bgw_name, BGW_MAXLEN, "onesearch commit probe");
    snprintf(worker.bgw_type, BGW_MAXLEN, "onesearch commit probe");
    worker.bgw_main_arg = ObjectIdGetDatum(MyDatabaseId);
    memcpy(worker.bgw_extra, &role, sizeof(role));
    memcpy(worker.bgw_extra + sizeof(role), &publication, sizeof(publication));
    worker.bgw_notify_pid = MyProcPid;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle) ||
        WaitForBackgroundWorkerStartup(handle, &pid) != BGWH_STARTED)
    {
        SpinLockAcquire(&shared->mutex);
        shared->starting = false;
        SpinLockRelease(&shared->mutex);
        ereport(ERROR, (errmsg("could not start probe worker")));
    }
    return pid;
}

Datum
onesearch_probe_start(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT32(start_worker(false));
}

Datum
onesearch_probe_start_publication(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT32(start_worker(true));
}

static int
worker_mode(void)
{
    int mode;

    SpinLockAcquire(&shared->mutex);
    mode = shared->mode;
    SpinLockRelease(&shared->mutex);
    return mode;
}

static void
set_worker_stage(int stage)
{
    SpinLockAcquire(&shared->mutex);
    shared->worker_stage = stage;
    SpinLockRelease(&shared->mutex);
}

static void
worker_wait(uint32 event)
{
    WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 20, event);
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
}

void
onesearch_probe_worker(Datum arg)
{
    Oid role;
    uint32 event;
    bool publication;

    memcpy(&publication, MyBgworkerEntry->bgw_extra + sizeof(role), sizeof(publication));
    memcpy(&role, MyBgworkerEntry->bgw_extra, sizeof(role));
    pqsignal(SIGTERM, die);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnectionByOid(DatumGetObjectId(arg), role, 0);
    before_shmem_exit(exit_cleanup, (Datum) 0);
    SpinLockAcquire(&shared->mutex);
    shared->worker_pid = MyProcPid;
    shared->worker_stage = 0;
    SpinLockRelease(&shared->mutex);
    event = WaitEventExtensionNew("OnesearchProbeWorker");
    for (;;)
    {
        uint64 xids[NSLOTS];
        uint64 count, i;
        int mode;

        worker_wait(event);
        /* Atomically claim an iteration so pause+idle is a real test barrier. */
        SpinLockAcquire(&shared->mutex);
        mode = shared->mode;
        if (mode != 1)
            shared->worker_stage = 1;
        SpinLockRelease(&shared->mutex);
        if (mode == 1)
            continue;
        if (publication)
        {
            publication_iteration(event);
            set_worker_stage(0);
            continue;
        }
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());
        set_worker_stage(1);
        SPI_execute("SELECT DISTINCT xid::text FROM onesearch_probe.outbox LIMIT 16", true, 0);
        count = SPI_processed;
        for (i = 0; i < count; i++)
            xids[i] = strtoull(SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1), NULL, 10);
        if (count && mode != 2)
        {
            /* Deliberate dependency for the early/after-lock comparison. */
            pgstat_report_activity(STATE_RUNNING, "probe: lock source and publish");
            SPI_execute("LOCK TABLE onesearch_probe.source IN ACCESS SHARE MODE", false, 0);
            for (i = 0; i < count; i++)
            {
                char query[512];

                snprintf(query, sizeof(query),
                         "WITH applied AS (DELETE FROM onesearch_probe.outbox "
                         "WHERE xid = '" UINT64_FORMAT "'::xid8 RETURNING *) "
                         "INSERT INTO onesearch_probe.receipts SELECT * FROM applied", xids[i]);
                SPI_execute(query, false, 0);
            }
            set_worker_stage(2);
            while (worker_mode() == 3)
                worker_wait(event);
        }
        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
        if (count && mode == 4)
        {
            set_worker_stage(3);
            while (worker_mode() == 4)
                worker_wait(event);
        }
        /* Notify only AFTER durable local publication, never before commit. */
        SpinLockAcquire(&shared->mutex);
        for (i = 0; i < count; i++)
        {
            int j;

            for (j = 0; j < NSLOTS; j++)
                if (shared->waiters[j].pid && shared->waiters[j].database == MyDatabaseId &&
                    shared->waiters[j].xid == xids[i])
                    shared->waiters[j].outcome = mode == 2 ? 2 : 1;
        }
        SpinLockRelease(&shared->mutex);
        set_worker_stage(0);
        pgstat_report_activity(STATE_IDLE, NULL);
    }
}

/* Observe committed replay publication outside the writer's cleanup callback.
 * Poll durable exact membership; adapter notifications are never authority.
 * The same SQL predicate is available after a timeout or postmaster restart.
 */
static void
publication_iteration(uint32 event)
{
    Waiter candidates[NSLOTS];
    bool ready[NSLOTS] = {false};
    bool have_ready = false;
    int i;

    SpinLockAcquire(&shared->mutex);
    memcpy(candidates, shared->waiters, sizeof(candidates));
    SpinLockRelease(&shared->mutex);
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());
    for (i = 0; i < NSLOTS; i++)
    {
        char query[256];
        bool isnull;
        Datum result;

        if (!candidates[i].pid || candidates[i].database != MyDatabaseId)
            continue;
        snprintf(query, sizeof(query),
                 "SELECT pgos_completion_probe.is_published('" UINT64_FORMAT "'::xid8)",
                 candidates[i].xid);
        if (SPI_execute(query, true, 1) != SPI_OK_SELECT || SPI_processed != 1)
            elog(ERROR, "completion probe expected one status row");
        result = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
        ready[i] = !isnull && DatumGetBool(result);
        have_ready |= ready[i];
    }
    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
    if (!have_ready)
        return;
    set_worker_stage(3);
    while (worker_mode() == 4)
        worker_wait(event); /* Fault: die after observation but before notification. */
    SpinLockAcquire(&shared->mutex);
    for (i = 0; i < NSLOTS; i++)
        if (ready[i] && shared->waiters[i].pid == candidates[i].pid &&
            shared->waiters[i].database == candidates[i].database &&
            shared->waiters[i].xid == candidates[i].xid)
            shared->waiters[i].outcome = 1;
    SpinLockRelease(&shared->mutex);
}
