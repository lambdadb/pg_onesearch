/* Bounded snapshot-aware Custom Scan experiment, excluded from the product. */
#include "postgres.h"
#include <math.h>
#include <ctype.h>
#include "access/tableam.h"
#include "catalog/pg_type_d.h"
#include "executor/spi.h"
#include "utils/snapmgr.h"
#include "storage/itemptr.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_func.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "catalog/namespace.h"

#if PG_VERSION_NUM < 180000 || PG_VERSION_NUM >= 190000
#error "This executor experiment targets PostgreSQL 18 only"
#endif

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(pgos_executor_match);
PG_FUNCTION_INFO_V1(pgos_executor_score);
void _PG_init(void);

typedef struct ProbeState
{
    CustomScanState css;
    TableScanDesc scan;
    ExprState *query;
    List *score_expressions;
    MemoryContext request_context;
    bool bm25;
    bool fetched;
    bool current_valid;
    int64 current_id;
    double current_score;
    Oid index;
    AttrNumber key_attnum;
    char *tag;
    int nhits;
    int64 ids[64];
    ItemPointerData tids[64];
    double scores[64];
    int calls;
    int rescans;
} ProbeState;

static set_rel_pathlist_hook_type previous_hook;
static planner_hook_type previous_planner;
/* Only a dynamically scoped projection frame; scores live in ProbeState.
 * Never retained in fn_extra, across ExecScan calls, or across executions. */
static ProbeState *projection_frame;
static JsonbValue *field(Jsonb *, const char *);

static Jsonb *
metadata(Oid index)
{
    Oid type = REGCLASSOID;
    Oid fn = LookupFuncName(list_make2(makeString("pgos_lifecycle_probe"), makeString("info")), 1, &type, false);
    return DatumGetJsonbP(OidFunctionCall1(fn, ObjectIdGetDatum(index)));
}

static int64
number(Jsonb *object, const char *name)
{
    JsonbValue *value = field(object, name);
    if (!value || value->type != jbvNumeric)
        elog(ERROR, "invalid snapshot numeric field");
    return DatumGetInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(value->val.numeric)));
}

static Plan *plan_path(PlannerInfo *, RelOptInfo *, CustomPath *, List *, List *, List *);
static Node *create_state(CustomScan *);
static void begin_scan(CustomScanState *, EState *, int);
static TupleTableSlot *exec_scan(CustomScanState *);
static void end_scan(CustomScanState *);
static void rescan(CustomScanState *);
static void explain_scan(CustomScanState *, List *, ExplainState *);
static const CustomPathMethods path_methods = { .CustomName = "OneSearchSnapshot", .PlanCustomPath = plan_path };
static const CustomScanMethods scan_methods = { .CustomName = "OneSearchSnapshot", .CreateCustomScanState = create_state };
static const CustomExecMethods exec_methods = {
    .CustomName = "OneSearchSnapshot", .BeginCustomScan = begin_scan,
    .ExecCustomScan = exec_scan, .EndCustomScan = end_scan,
    .ReScanCustomScan = rescan, .ExplainCustomScan = explain_scan
};

static bool
named_function(Oid oid, const char *name)
{
    Oid ns = get_namespace_oid("pgos_snapshot_executor", true);
    char *actual;

    if (!OidIsValid(ns) || get_func_namespace(oid) != ns)
        return false;
    actual = get_func_name(oid);
    return actual && strcmp(actual, name) == 0;
}

static bool
marker(Node *node)
{
    return IsA(node, FuncExpr) &&
        (named_function(((FuncExpr *) node)->funcid, "match") ||
         named_function(((FuncExpr *) node)->funcid, "vector_match"));
}

static bool
has_marker(Node *node, void *context)
{
    if (!node)
        return false;
    if (marker(node))
        return true;
    return expression_tree_walker(node, has_marker, context);
}

static void
unsupported(void)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("unsupported executor probe query shape")));
}

typedef struct QueryScope { int markers; bool unsupported; } QueryScope;

static bool
check_scope(Node *node, void *context)
{
    QueryScope *scope = context;

    if (!node)
        return false;
    if (IsA(node, Query))
    {
        Query *query = (Query *) node;

        if (query->commandType != CMD_SELECT || list_length(query->rtable) > 1 ||
            query->hasAggs || query->hasWindowFuncs || query->hasTargetSRFs ||
            query->cteList || query->setOperations || query->distinctClause || query->rowMarks)
            scope->unsupported = true;
        return query_tree_walker(query, check_scope, context, 0);
    }
    if (marker(node))
        scope->markers++;
    return expression_tree_walker(node, check_scope, context);
}

static PlannedStmt *
probe_planner(Query *query, const char *text, int options, ParamListInfo params)
{
    QueryScope scope = {0};

    check_scope((Node *) query, &scope);
    if (scope.markers && (scope.unsupported || scope.markers != 1))
        unsupported();
    return previous_planner ? previous_planner(query, text, options, params) :
                              standard_planner(query, text, options, params);
}

typedef struct ScoreBinding { Index rti; Oid relation; AttrNumber key; bool bm25; } ScoreBinding;

static bool
check_scores(Node *node, void *context)
{
    ScoreBinding *binding = context;

    if (!node)
        return false;
    if (IsA(node, FuncExpr) && named_function(((FuncExpr *) node)->funcid, "score"))
    {
        FuncExpr *function = (FuncExpr *) node;
        Node *relation;
        Node *key;

        if (list_length(function->args) != 2 || !binding->bm25)
            unsupported();
        relation = linitial(function->args);
        key = lsecond(function->args);
        if (!IsA(relation, Const) || ((Const *) relation)->constisnull ||
            DatumGetObjectId(((Const *) relation)->constvalue) != binding->relation ||
            !IsA(key, Var) || ((Var *) key)->varno != binding->rti ||
            ((Var *) key)->varattno != binding->key || ((Var *) key)->varlevelsup != 0)
            unsupported();
    }
    return expression_tree_walker(node, check_scores, context);
}

static void
paths(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
    ListCell *lc;
    FuncExpr *match = NULL;
    CustomPath *path;
    Var *column;
    Node *query;
    ScoreBinding binding;
    Const *index;
    Jsonb *meta;
    JsonbValue *mode;

    if (previous_hook)
        previous_hook(root, rel, rti, rte);
    if (rte->rtekind != RTE_RELATION)
        return;
    foreach(lc, rel->baserestrictinfo)
    {
        Node *clause = (Node *) ((RestrictInfo *) lfirst(lc))->clause;

        if (marker(clause))
        {
            if (match)
                unsupported();
            match = (FuncExpr *) clause;
        }
        else if (has_marker(clause, NULL))
            unsupported();
    }
    if (!match)
        return;
    if (list_length(root->parse->rtable) != 1 || root->parse->commandType != CMD_SELECT ||
        root->parse->hasAggs || root->parse->hasWindowFuncs || root->parse->hasTargetSRFs ||
        root->parse->rowMarks || rte->tablesample || rel->lateral_relids ||
        list_length(match->args) != 3)
        unsupported();
    binding.rti = rti;
    index = linitial(match->args);
    if (!IsA(index, Const) || index->constisnull || index->consttype != REGCLASSOID)
        unsupported();
    binding.relation = DatumGetObjectId(index->constvalue);
    meta = metadata(binding.relation);
    binding.key = number(meta, "key_attnum");
    if (number(meta, "heap_oid") != rte->relid)
        unsupported();
    mode = field(meta, "mode");
    binding.bm25 = named_function(match->funcid, "match");
    if (!mode || mode->type != jbvString ||
        mode->val.string.len != (binding.bm25 ? 4 : 6) ||
        memcmp(mode->val.string.val, binding.bm25 ? "text" : "vector", mode->val.string.len))
        unsupported();
    column = lsecond(match->args);
    query = lthird(match->args);
    if (!IsA(column, Var) || column->varno != rti || column->varlevelsup != 0 ||
        column->varattno != number(meta, "value_attnum") ||
        !(IsA(query, Const) || IsA(query, Param)))
        unsupported();
    check_scores((Node *) root->parse->targetList, &binding);
    check_scores((Node *) root->parse->jointree->quals, &binding);
    path = makeNode(CustomPath);
    path->path.pathtype = T_CustomScan;
    path->path.parent = rel;
    path->path.pathtarget = rel->reltarget;
    path->path.rows = 64;
    path->path.startup_cost = 10;
    path->path.total_cost = 20;
    path->flags = CUSTOMPATH_SUPPORT_PROJECTION;
    path->custom_private = list_make4(makeInteger(binding.bm25), match,
                                     makeInteger(binding.key), makeInteger(column->varattno));
    /* Invalidate cached plans when this explicit index definition is replaced. */
    root->glob->relationOids = lappend_oid(root->glob->relationOids, binding.relation);
    path->methods = &path_methods;
    /* Explicit match requests have no pretend local BM25 fallback. Probe costs
     * are placeholders, not a demonstrated production cost model. */
    rel->pathlist = NIL;
    rel->partial_pathlist = NIL;
    rel->consider_parallel = false;
    add_path(rel, &path->path);
}

static Plan *
plan_path(PlannerInfo *root, RelOptInfo *rel, CustomPath *path, List *tlist,
          List *clauses, List *children)
{
    CustomScan *scan = makeNode(CustomScan);
    FuncExpr *match = lsecond(path->custom_private);
    List *quals = extract_actual_clauses(clauses, false);

    scan->scan.plan.targetlist = tlist;
    scan->scan.plan.qual = list_delete(quals, match);
    scan->scan.scanrelid = rel->relid;
    scan->flags = path->flags;
    scan->custom_private = list_make4(copyObject(linitial(path->custom_private)),
                                     copyObject(linitial(match->args)),
                                     copyObject(lthird(path->custom_private)),
                                     copyObject(lfourth(path->custom_private)));
    scan->custom_exprs = list_make1(copyObject(lthird(match->args)));
    scan->methods = &scan_methods;
    return &scan->scan.plan;
}

static Node *
create_state(CustomScan *scan)
{
    ProbeState *state = palloc0(sizeof(ProbeState));

    NodeSetTag(state, T_CustomScanState);
    state->css.methods = &exec_methods;
    state->css.slotOps = &TTSOpsBufferHeapTuple;
    return (Node *) state;
}

static void
check_health(ProbeState *state)
{
    Oid type = REGCLASSOID;
    Oid fn = LookupFuncName(list_make2(makeString("pgos_snapshot_probe"), makeString("check_health")), 1, &type, false);
    OidFunctionCall1(fn, ObjectIdGetDatum(state->index));
}

static bool
collect_scores(Node *node, void *context)
{
    ProbeState *state = context;

    if (!node)
        return false;
    if (IsA(node, FuncExpr) && named_function(((FuncExpr *) node)->funcid, "score"))
        state->score_expressions = lappend(state->score_expressions, node);
    return expression_tree_walker(node, collect_scores, context);
}

static void
begin_scan(CustomScanState *node, EState *estate, int eflags)
{
    ProbeState *state = (ProbeState *) node;
    CustomScan *plan = (CustomScan *) node->ss.ps.plan;

    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("executor probe requires superuser")));
    if (node->ss.ss_currentRelation->rd_rel->relrowsecurity)
        unsupported();
    state->bm25 = intVal(linitial(plan->custom_private));
    state->index = DatumGetObjectId(((Const *) lsecond(plan->custom_private))->constvalue);
    state->key_attnum = intVal(lthird(plan->custom_private));
    {
        Jsonb *meta = metadata(state->index);
        if (number(meta, "heap_oid") != RelationGetRelid(node->ss.ss_currentRelation) ||
            number(meta, "key_attnum") != state->key_attnum ||
            number(meta, "value_attnum") != intVal(lfourth(plan->custom_private)))
            unsupported();
    }
    state->query = ExecInitExpr(linitial(plan->custom_exprs), &node->ss.ps);
    collect_scores((Node *) plan->scan.plan.targetlist, state);
    collect_scores((Node *) plan->scan.plan.qual, state);
    state->request_context = AllocSetContextCreate(estate->es_query_cxt, "onesearch probe request", ALLOCSET_DEFAULT_SIZES);
    if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
    {
        check_health(state);
        state->scan = table_beginscan(node->ss.ss_currentRelation, estate->es_snapshot, 0, NULL);
    }
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

static void
fetch(ProbeState *state)
{
    bool isnull;
    Datum argument;
    StringInfoData query;
    Jsonb *body, *response, *docs;
    JsonbIterator *iterator;
    JsonbValue item;
    JsonbIteratorToken token;
    Oid types[] = {REGCLASSOID, JSONBOID};
    Datum args[2];
    SPIPlanPtr plan;
    MemoryContext old;
    Snapshot snapshot = state->css.ss.ps.state->es_snapshot;

    check_health(state);
    old = MemoryContextSwitchTo(state->request_context);
    argument = ExecEvalExprSwitchContext(state->query, state->css.ss.ps.ps_ExprContext, &isnull);
    state->fetched = true;
    if (isnull)
    {
        MemoryContextSwitchTo(old);
        return;
    }
    initStringInfo(&query);
    if (state->bm25)
        escape_json(&query, TextDatumGetCString(argument));
    else
    {
        Oid type = exprType(linitial(((CustomScan *) state->css.ss.ps.plan)->custom_exprs));
        Oid output;
        bool varlen;
        getTypeOutputInfo(type, &output, &varlen);
        appendStringInfoString(&query, OidOutputFunctionCall(output, argument));
    }
    body = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(query.data)));
    args[0] = ObjectIdGetDatum(state->index);
    args[1] = JsonbPGetDatum(body);
    SPI_connect();
    plan = SPI_prepare("SELECT pgos_snapshot_probe.search($1,$2)", 2, types);
    if (!plan || SPI_execute_snapshot(plan, args, NULL, snapshot, InvalidSnapshot, true, false, 1) != SPI_OK_SELECT || SPI_processed != 1)
        elog(ERROR, "snapshot executor expected one result");
    argument = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
    if (isnull)
        elog(ERROR, "snapshot executor returned null");
    MemoryContextSwitchTo(state->request_context);
    response = DatumGetJsonbPCopy(argument);
    SPI_freeplan(plan);
    SPI_finish();
    state->calls++;
    {
        JsonbValue *tag = field(response, "tag");
        state->tag = pnstrdup(tag->val.string.val, tag->val.string.len);
    }
    docs = JsonbValueToJsonb(field(response, "results"));
    iterator = JsonbIteratorInit(&docs->root);
    while ((token = JsonbIteratorNext(&iterator, &item, true)) != WJB_DONE)
    {
        Jsonb *entry;
        JsonbValue *id, *score, *tid;
        int i;
        int64 key;
        if (token != WJB_ELEM)
            continue;
        if (state->nhits >= 64)
            elog(ERROR, "snapshot executor hit budget exceeded");
        entry = JsonbValueToJsonb(&item);
        id = field(entry, "id");
        tid = field(entry, "tid");
        score = field(entry, state->bm25 ? "score" : "distance");
        if (!id || id->type != jbvString || !tid || tid->type != jbvString || !score || score->type != jbvNumeric)
            elog(ERROR, "invalid snapshot result identity or score");
        key = pg_strtoint64(pnstrdup(id->val.string.val, id->val.string.len));
        for (i = 0; i < state->nhits; i++)
            if (state->ids[i] == key)
                elog(ERROR, "duplicate snapshot result");
        i = state->nhits++;
        state->ids[i] = key;
        state->tids[i] = *(ItemPointer) DatumGetPointer(DirectFunctionCall1(tidin,
            CStringGetDatum(pnstrdup(tid->val.string.val, tid->val.string.len))));
        state->scores[i] = DatumGetFloat8(DirectFunctionCall1(numeric_float8, NumericGetDatum(score->val.numeric)));
        if (!isfinite(state->scores[i]))
            elog(ERROR, "nonfinite snapshot score");
    }
    MemoryContextSwitchTo(old);
}

static TupleTableSlot *
next_tuple(ScanState *scan)
{
    ProbeState *state = (ProbeState *) scan;
    TupleTableSlot *slot = scan->ss_ScanTupleSlot;

    state->current_valid = false;
    if (!state->fetched)
        fetch(state);
    while (table_scan_getnextslot(state->scan, ForwardScanDirection, slot))
    {
        bool isnull;
        int i;
        int64 id = DatumGetInt64(slot_getattr(slot, state->key_attnum, &isnull));
        if (isnull)
            elog(ERROR, "snapshot source key is null");
        for (i = 0; i < state->nhits; i++)
        {
            if (state->ids[i] != id)
                continue;
            /* Identity check only. Never dereference or retain this TID beyond
             * the executor snapshot; the slot is from the actual heap scan. */
            if (!ItemPointerEquals(&state->tids[i], &slot->tts_tid))
                ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                                errmsg("snapshot result and source tuple version differ")));
            state->current_valid = true;
            state->current_id = id;
            state->current_score = state->scores[i];
            return slot;
        }
    }
    return ExecClearTuple(slot);
}

static bool
recheck(ScanState *node, TupleTableSlot *slot)
{
    unsupported();
    return false;
}

static TupleTableSlot *
exec_scan(CustomScanState *node)
{
    ProbeState *state = (ProbeState *) node;
    ProbeState *previous = projection_frame;
    TupleTableSlot *result = NULL;

    check_health(state);
    if (node->ss.ps.state->es_direction != ForwardScanDirection)
        unsupported();
    projection_frame = state;
    PG_TRY();
    {
        result = ExecScan(&node->ss, next_tuple, recheck);
        check_health(state);
    }
    PG_FINALLY();
    {
        projection_frame = previous;
    }
    PG_END_TRY();
    return result;
}

static void
rescan(CustomScanState *node)
{
    ProbeState *state = (ProbeState *) node;

    check_health(state);
    state->rescans++;
    state->fetched = false;
    state->current_valid = false;
    state->nhits = 0;
    state->tag = NULL;
    memset(state->scores, 0, sizeof(state->scores));
    MemoryContextReset(state->request_context);
    if (state->scan)
        table_rescan(state->scan, NULL);
    ExecScanReScan(&node->ss);
}

static void
end_scan(CustomScanState *node)
{
    ProbeState *state = (ProbeState *) node;

    if (state->scan)
        table_endscan(state->scan);
    MemoryContextDelete(state->request_context);
}

static void
explain_scan(CustomScanState *node, List *ancestors, ExplainState *es)
{
    ProbeState *state = (ProbeState *) node;

    ExplainPropertyText("Mode", state->bm25 ? "BM25" : "Vector", es);
    ExplainPropertyText("Scope", "64-row statement snapshot with complete vector delta", es);
    if (es->analyze)
    {
        if (state->tag)
            ExplainPropertyText("Snapshot Tag", state->tag, es);
        ExplainPropertyInteger("Remote Queries", NULL, state->calls, es);
        ExplainPropertyInteger("Rescans", NULL, state->rescans, es);
    }
}

Datum
pgos_executor_match(PG_FUNCTION_ARGS)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("match requires an executor probe Custom Scan")));
    PG_RETURN_BOOL(false);
}

Datum
pgos_executor_score(PG_FUNCTION_ARGS)
{
    ProbeState *state = projection_frame;

    if (!state || !state->bm25 || !state->current_valid ||
        !list_member_ptr(state->score_expressions, fcinfo->flinfo->fn_expr) ||
        PG_GETARG_OID(0) != state->index ||
        PG_GETARG_INT64(1) != state->current_id)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("score has no matching scan projection context")));
    check_health(state);
    PG_RETURN_FLOAT8(state->current_score);
}

void
_PG_init(void)
{
    RegisterCustomScanMethods(&scan_methods);
    previous_hook = set_rel_pathlist_hook;
    set_rel_pathlist_hook = paths;
    previous_planner = planner_hook;
    planner_hook = probe_planner;
}
