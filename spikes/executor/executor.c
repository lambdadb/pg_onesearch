/* Fixed, read-only five-row fixture. This is not an index or MVCC sync protocol. */
#include "postgres.h"
#include <math.h>
#include <ctype.h>
#include "access/tableam.h"
#include "catalog/pg_type_d.h"
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
    bool hits[6];
    double scores[6];
    int calls;
    int rescans;
} ProbeState;

static set_rel_pathlist_hook_type previous_hook;
static planner_hook_type previous_planner;
/* Only a dynamically scoped projection frame; scores live in ProbeState.
 * Never retained in fn_extra, across ExecScan calls, or across executions. */
static ProbeState *projection_frame;
static bool healthy = true;
static char *vector_collection;
static char *bm25_collection;
static char *vector_tag;
static char *bm25_tag;

static Plan *plan_path(PlannerInfo *, RelOptInfo *, CustomPath *, List *, List *, List *);
static Node *create_state(CustomScan *);
static void begin_scan(CustomScanState *, EState *, int);
static TupleTableSlot *exec_scan(CustomScanState *);
static void end_scan(CustomScanState *);
static void rescan(CustomScanState *);
static void explain_scan(CustomScanState *, List *, ExplainState *);
static const CustomPathMethods path_methods = { .CustomName = "OneSearchProbe", .PlanCustomPath = plan_path };
static const CustomScanMethods scan_methods = { .CustomName = "OneSearchProbe", .CreateCustomScanState = create_state };
static const CustomExecMethods exec_methods = {
    .CustomName = "OneSearchProbe", .BeginCustomScan = begin_scan,
    .ExecCustomScan = exec_scan, .EndCustomScan = end_scan,
    .ReScanCustomScan = rescan, .ExplainCustomScan = explain_scan
};

static bool
named_function(Oid oid, const char *name)
{
    Oid ns = get_namespace_oid("pgos_executor_probe", true);
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

typedef struct ScoreBinding { Index rti; Oid relation; bool bm25; } ScoreBinding;

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
            ((Var *) key)->varattno != 1 || ((Var *) key)->varlevelsup != 0)
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
    if (get_rel_namespace(rte->relid) != get_namespace_oid("pgos_executor_probe", true) ||
        strcmp(get_rel_name(rte->relid), "documents") != 0 ||
        list_length(root->parse->rtable) != 1 || root->parse->commandType != CMD_SELECT ||
        root->parse->hasAggs || root->parse->hasWindowFuncs || root->parse->hasTargetSRFs ||
        root->parse->rowMarks || rte->tablesample || rel->lateral_relids ||
        list_length(match->args) != 2)
        unsupported();
    binding.rti = rti;
    binding.relation = rte->relid;
    binding.bm25 = named_function(match->funcid, "match");
    column = linitial(match->args);
    query = lsecond(match->args);
    if (!IsA(column, Var) || column->varno != rti || column->varlevelsup != 0 ||
        column->varattno != (binding.bm25 ? 2 : 3) ||
        !(IsA(query, Const) || IsA(query, Param)))
        unsupported();
    check_scores((Node *) root->parse->targetList, &binding);
    check_scores((Node *) root->parse->jointree->quals, &binding);
    path = makeNode(CustomPath);
    path->path.pathtype = T_CustomScan;
    path->path.parent = rel;
    path->path.pathtarget = rel->reltarget;
    path->path.rows = 5;
    path->path.startup_cost = 10;
    path->path.total_cost = 20;
    path->flags = CUSTOMPATH_SUPPORT_PROJECTION;
    path->custom_private = list_make2(makeInteger(binding.bm25), match);
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
    scan->custom_private = list_make1(copyObject(linitial(path->custom_private)));
    scan->custom_exprs = list_make1(copyObject(lsecond(match->args)));
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
check_health(void)
{
    if (!healthy)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("executor probe source is unavailable")));
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
    state->query = ExecInitExpr(linitial(plan->custom_exprs), &node->ss.ps);
    collect_scores((Node *) plan->scan.plan.targetlist, state);
    collect_scores((Node *) plan->scan.plan.qual, state);
    state->request_context = AllocSetContextCreate(estate->es_query_cxt, "onesearch probe request", ALLOCSET_DEFAULT_SIZES);
    if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
    {
        check_health();
        LockRelationOid(RelationGetRelid(node->ss.ss_currentRelation), ShareLock);
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
    Jsonb *body;
    Jsonb *response;
    Jsonb *docs;
    JsonbIterator *iterator;
    JsonbValue item;
    JsonbIteratorToken token;
    Oid types[4] = {TEXTOID, TEXTOID, JSONBOID, INT4OID};
    Oid remote;
    MemoryContext old;
    int count = 0;

    check_health();
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
    {
        char *query_text = TextDatumGetCString(argument);
        const unsigned char *p = (unsigned char *) query_text;

        while (*p && isspace(*p))
            p++;
        if (!*p)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("BM25 query must not be empty")));
        appendStringInfoString(&query, "{\"queryString\":{\"defaultField\":\"content\",\"skipSyntax\":true,\"query\":");
        escape_json(&query, query_text);
        appendStringInfoString(&query, "}}");
    }
    else
    {
        Oid type = exprType(linitial(((CustomScan *) state->css.ss.ps.plan)->custom_exprs));
        Oid output, input, param;
        Oid signature[2] = {type, type};
        bool varlen;
        Datum anchor;

        getTypeInputInfo(type, &input, &param);
        getTypeOutputInfo(type, &output, &varlen);
        anchor = OidInputFunctionCall(input, "[1,0,0]", param, 3);
        OidFunctionCall2(LookupFuncName(list_make2(makeString("onesearch"), makeString("cosine_distance")), 2, signature, false), argument, anchor);
        appendStringInfo(&query, "{\"knn\":{\"field\":\"embedding\",\"k\":100,\"queryVector\":%s}}",
                         OidOutputFunctionCall(output, argument));
    }
    body = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(query.data)));
    remote = LookupFuncName(list_make2(makeString("pgos_remote_probe"), makeString("query")), 4, types, false);
    state->calls++;
    response = DatumGetJsonbP(OidFunctionCall4(remote,
        CStringGetTextDatum(state->bm25 ? bm25_collection : vector_collection),
        CStringGetTextDatum(state->bm25 ? bm25_tag : vector_tag), JsonbPGetDatum(body), Int32GetDatum(100)));
    docs = JsonbValueToJsonb(field(response, "docs"));
    iterator = JsonbIteratorInit(&docs->root);
    while ((token = JsonbIteratorNext(&iterator, &item, true)) != WJB_DONE)
    {
        Jsonb *entry;
        JsonbValue *id;
        JsonbValue *score;
        int key;
        double value;

        if (token != WJB_ELEM)
            continue;
        entry = JsonbValueToJsonb(&item);
        id = field(JsonbValueToJsonb(field(entry, "doc")), "id");
        score = field(entry, "score");
        if (id->val.string.len != 1 || id->val.string.val[0] < '1' || id->val.string.val[0] > '5')
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("remote ID is outside the frozen fixture")));
        key = id->val.string.val[0] - '0';
        value = DatumGetFloat8(DirectFunctionCall1(numeric_float8, NumericGetDatum(score->val.numeric)));
        if (!isfinite(value) || state->hits[key])
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid remote fixture score or identity")));
        state->hits[key] = true;
        state->scores[key] = value;
        count++;
    }
    if (!state->bm25 && count != 5)
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("vector result does not cover the frozen five-row fixture")));
    MemoryContextSwitchTo(old);
}

static TupleTableSlot *
next_tuple(ScanState *scan)
{
    ProbeState *state = (ProbeState *) scan;
    TupleTableSlot *slot = scan->ss_ScanTupleSlot;

    if (!state->fetched)
        fetch(state);
    while (table_scan_getnextslot(state->scan, ForwardScanDirection, slot))
    {
        bool isnull;
        int64 id = DatumGetInt64(slot_getattr(slot, 1, &isnull));

        if (isnull || id < 1 || id > 5)
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("local row is outside the frozen fixture")));
        if (state->hits[id])
        {
            state->current_valid = true;
            state->current_id = id;
            state->current_score = state->scores[id];
            return slot;
        }
    }
    state->current_valid = false;
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

    check_health();
    if (node->ss.ps.state->es_direction != ForwardScanDirection)
        unsupported();
    projection_frame = state;
    PG_TRY();
    {
        result = ExecScan(&node->ss, next_tuple, recheck);
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

    check_health();
    state->rescans++;
    state->fetched = false;
    state->current_valid = false;
    memset(state->hits, 0, sizeof(state->hits));
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
    ExplainPropertyText("Scope", "frozen five-row fixture", es);
    if (es->analyze)
    {
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

    check_health();
    if (!state || !state->bm25 || !state->current_valid ||
        !list_member_ptr(state->score_expressions, fcinfo->flinfo->fn_expr) ||
        PG_GETARG_OID(0) != RelationGetRelid(state->css.ss.ss_currentRelation) ||
        PG_GETARG_INT64(1) != state->current_id)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("score has no matching scan projection context")));
    PG_RETURN_FLOAT8(state->current_score);
}

void
_PG_init(void)
{
    DefineCustomBoolVariable("pgos_executor_probe.healthy", "Injected fixture health, not production status.", NULL, &healthy, true, PGC_SUSET, 0, NULL, NULL, NULL);
#define STRING_GUC(name, variable, env, fallback) \
    DefineCustomStringVariable("pgos_executor_probe." name, "Fixture mapping.", NULL, &variable, getenv(env) ? getenv(env) : fallback, PGC_SUSET, 0, NULL, NULL, NULL)
    STRING_GUC("vector_collection", vector_collection, "PGOS_EXECUTOR_VECTOR_COLLECTION", "vector");
    STRING_GUC("bm25_collection", bm25_collection, "PGOS_EXECUTOR_BM25_COLLECTION", "bm25");
    STRING_GUC("vector_tag", vector_tag, "PGOS_EXECUTOR_VECTOR_TAG", "checkpoint-fixture");
    STRING_GUC("bm25_tag", bm25_tag, "PGOS_EXECUTOR_BM25_TAG", "checkpoint-fixture");
    RegisterCustomScanMethods(&scan_methods);
    previous_hook = set_rel_pathlist_hook;
    set_rel_pathlist_hook = paths;
    previous_planner = planner_hook;
    planner_hook = probe_planner;
}
