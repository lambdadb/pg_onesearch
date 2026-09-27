/* Independent, bounded Lucene experiment. No LambdaDB implementation or product API. */
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

public final class OverlayProbe {
    private static final String BODY = "content";
    private static final int LIMIT = 64;
    private static final float TOLERANCE = 0.000001f;

    record Overlay(Map<String, String> upserts, Set<String> deletes) {
        Overlay {
            upserts = Map.copyOf(upserts);
            deletes = Set.copyOf(deletes);
            if (upserts.size() + deletes.size() > LIMIT) {
                throw new IllegalArgumentException("overlay exceeds 64 operations");
            }
            for (String key : upserts.keySet()) {
                validate(key, upserts.get(key));
                if (deletes.contains(key)) {
                    throw new IllegalArgumentException("conflicting final operations");
                }
            }
            for (String key : deletes) {
                validate(key, "");
            }
        }
        Set<String> changedKeys() {
            Set<String> result = new HashSet<>(deletes);
            result.addAll(upserts.keySet());
            return result;
        }
    }

    record Hit(String id, float score) {}
    record Counts(long docs, long terms) {}

    static void validate(String id, String body) {
        if (!id.matches("[a-zA-Z0-9_-]{1,64}") ||
            body.getBytes(StandardCharsets.UTF_8).length > 4096) {
            throw new IllegalArgumentException("fixture identity or document budget exceeded");
        }
    }

    /* Fixtures deliberately retain deleted postings: merge-dependent statistics
       must not accidentally make a visibility-only negative control pass. */
    static final class Fixture implements AutoCloseable {
        final Directory directory = new ByteBuffersDirectory();
        final DirectoryReader reader;
        Fixture(Map<String, String> docs, Set<String> hardDeletes) throws IOException {
            if (docs.size() > LIMIT) {
                throw new IllegalArgumentException("base exceeds 64 documents");
            }
            try (var analyzer = new StandardAnalyzer();
                 var writer = new IndexWriter(directory, new IndexWriterConfig(analyzer)
                     .setMergePolicy(NoMergePolicy.INSTANCE))) {
                for (var entry : new TreeMap<>(docs).entrySet()) {
                    validate(entry.getKey(), entry.getValue());
                    Document document = new Document();
                    document.add(new StringField("id", entry.getKey(), Field.Store.YES));
                    document.add(new TextField(BODY, entry.getValue(), Field.Store.NO));
                    writer.addDocument(document);
                }
                writer.commit();
                for (String key : hardDeletes) {
                    writer.deleteDocuments(new Term("id", key));
                }
                writer.commit();
            }
            reader = DirectoryReader.open(directory);
        }
        Fixture(Map<String, String> docs) throws IOException {
            this(docs, Set.of());
        }
        @Override public void close() throws IOException {
            reader.close();
            directory.close();
        }
    }

    /* Request-local visibility. Own one reference; never modify the base reader. */
    static final class VisibleLeaf extends FilterLeafReader {
        final Bits visible;
        final int count;
        VisibleLeaf(LeafReader base, Set<String> masked) throws IOException {
            super(base);
            base.incRef();
            BitSet bits = new BitSet(base.maxDoc());
            Bits live = base.getLiveDocs();
            try {
                var stored = base.storedFields();
                for (int doc = 0; doc < base.maxDoc(); doc++) {
                    if ((live == null || live.get(doc)) &&
                        !masked.contains(stored.document(doc).get("id"))) {
                        bits.set(doc);
                    }
                }
            } catch (IOException | RuntimeException failure) {
                base.decRef();
                throw failure;
            }
            count = bits.cardinality();
            visible = new Bits() {
                @Override public boolean get(int index) { return bits.get(index); }
                @Override public int length() { return base.maxDoc(); }
            };
        }
        @Override public Bits getLiveDocs() { return visible; }
        @Override public int numDocs() { return count; }
        // A shared cache key would let a different request borrow this visibility.
        @Override public CacheHelper getCoreCacheHelper() { return null; }
        @Override public CacheHelper getReaderCacheHelper() { return null; }
    }

    /* All terms/postings are scanned intentionally. This is a small-corpus
       correctness oracle for statistics, not a scalable server algorithm. */
    static final class EffectiveStats {
        final Map<BytesRef, Counts> terms = new HashMap<>();
        final CollectionStatistics collection;
        EffectiveStats(IndexReader reader) throws IOException {
            long docCount = 0;
            long sumDocFreq = 0;
            long sumTotalTermFreq = 0;
            for (var context : reader.leaves()) {
                LeafReader leaf = context.reader();
                Bits live = leaf.getLiveDocs();
                Terms vocabulary = leaf.terms(BODY);
                if (vocabulary == null) { continue; }
                BitSet fieldDocs = new BitSet(leaf.maxDoc());
                TermsEnum iterator = vocabulary.iterator();
                BytesRef term;
                while ((term = iterator.next()) != null) {
                    long df = 0;
                    long tf = 0;
                    PostingsEnum postings = iterator.postings(null, PostingsEnum.FREQS);
                    for (int doc = postings.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS;
                         doc = postings.nextDoc()) {
                        if (live == null || live.get(doc)) {
                            fieldDocs.set(doc);
                            df++;
                            tf += postings.freq();
                        }
                    }
                    if (df != 0) {
                        terms.merge(BytesRef.deepCopyOf(term), new Counts(df, tf),
                            (a, b) -> new Counts(a.docs + b.docs, a.terms + b.terms));
                        sumDocFreq += df;
                        sumTotalTermFreq += tf;
                    }
                }
                docCount += fieldDocs.cardinality();
            }
            collection = docCount == 0 ? null : new CollectionStatistics(
                BODY, reader.numDocs(), docCount, sumTotalTermFreq, sumDocFreq);
        }
    }

    static IndexSearcher searcher(IndexReader reader, EffectiveStats stats) {
        IndexSearcher searcher = stats == null ? new IndexSearcher(reader) : new IndexSearcher(reader) {
            @Override public CollectionStatistics collectionStatistics(String field) {
                if (!BODY.equals(field)) { throw new IllegalArgumentException("unsupported field"); }
                return stats.collection;
            }
            @Override public TermStatistics termStatistics(Term term, int df, long tf) {
                Counts count = stats.terms.get(term.bytes());
                if (count == null) { throw new IllegalStateException("term escaped effective query rewrite"); }
                return new TermStatistics(term.bytes(), count.docs, count.terms);
            }
        };
        searcher.setSimilarity(new BM25Similarity());
        searcher.setQueryCache(null);
        return searcher;
    }

    static Query effectiveQuery(Query query, EffectiveStats stats) {
        if (query instanceof TermQuery term) {
            return stats.terms.containsKey(term.getTerm().bytes()) ? query :
                new MatchNoDocsQuery("term absent from effective corpus");
        }
        if (query instanceof BooleanQuery bool) {
            BooleanQuery.Builder result = new BooleanQuery.Builder();
            for (BooleanClause clause : bool) {
                if (clause.occur() != BooleanClause.Occur.SHOULD) {
                    throw new IllegalArgumentException("probe supports term disjunctions only");
                }
                result.add(effectiveQuery(clause.query(), stats), clause.occur());
            }
            return result.build();
        }
        throw new IllegalArgumentException("probe supports term disjunctions only");
    }

    static List<Hit> search(IndexReader reader, Query query, EffectiveStats stats) throws IOException {
        List<Hit> hits = new ArrayList<>();
        if (stats != null) {
            if (stats.collection == null) { return hits; }
            query = effectiveQuery(query, stats);
        }
        // Enumerate all admitted candidates before tie sorting or caller-side filtering.
        IndexSearcher searcher = searcher(reader, stats);
        Set<String> seen = new HashSet<>();
        for (var hit : searcher.search(query, LIMIT).scoreDocs) {
            String id = reader.storedFields().document(hit.doc).get("id");
            require(seen.add(id) && Float.isFinite(hit.score), "duplicate identity or invalid score");
            hits.add(new Hit(id, hit.score));
        }
        hits.sort(Comparator.comparingDouble(Hit::score).reversed().thenComparing(Hit::id));
        return hits;
    }

    static Query query(String... terms) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (String term : terms) {
            builder.add(new TermQuery(new Term(BODY, term)), BooleanClause.Occur.SHOULD);
        }
        return builder.build();
    }

    static final class Request implements AutoCloseable {
        final Fixture delta;
        final MultiReader reader;
        final EffectiveStats stats;
        Request(List<Fixture> bases, Overlay overlay) throws IOException {
            if (bases.stream().mapToInt(f -> f.reader.maxDoc()).sum() > LIMIT) {
                throw new IllegalArgumentException("base exceeds 64 physical documents");
            }
            delta = new Fixture(overlay.upserts);
            List<IndexReader> parts = new ArrayList<>();
            try {
                for (Fixture base : bases) {
                    for (var leaf : base.reader.leaves()) {
                        parts.add(new VisibleLeaf(leaf.reader(), overlay.changedKeys()));
                    }
                }
                delta.reader.incRef();
                parts.add(delta.reader);
                reader = new MultiReader(parts.toArray(IndexReader[]::new), true);
            } catch (IOException | RuntimeException failure) {
                for (IndexReader part : parts) { part.close(); }
                delta.close();
                throw failure;
            }
            try {
                if (reader.numDocs() > LIMIT) {
                    throw new IllegalArgumentException("effective corpus exceeds 64 documents");
                }
                stats = new EffectiveStats(reader);
            } catch (IOException | RuntimeException failure) {
                reader.close();
                delta.close();
                throw failure;
            }
        }
        List<Hit> run(Query query) throws IOException { return search(reader, query, stats); }
        @Override public void close() throws IOException {
            try { reader.close(); } finally { delta.close(); }
        }
    }

    static Map<String, String> apply(Map<String, String> base, Set<String> hardDeletes, Overlay overlay) {
        Map<String, String> docs = new HashMap<>(base);
        hardDeletes.forEach(docs::remove);
        overlay.deletes.forEach(docs::remove);
        docs.putAll(overlay.upserts);
        return docs;
    }

    static void require(boolean condition, String message) {
        if (!condition) { throw new AssertionError(message); }
    }
    static boolean equal(List<Hit> actual, List<Hit> expected) {
        if (actual.size() != expected.size()) { return false; }
        for (int i = 0; i < actual.size(); i++) {
            if (!actual.get(i).id.equals(expected.get(i).id) ||
                Math.abs(actual.get(i).score - expected.get(i).score) > TOLERANCE) { return false; }
        }
        return true;
    }
    static List<String> ids(List<Hit> hits) { return hits.stream().map(Hit::id).toList(); }
    static String json(List<Hit> hits) {
        return "[" + String.join(",", hits.stream().map(h ->
            "{\"id\":\"" + h.id + "\",\"score\":" + h.score + "}").toList()) + "]";
    }

    static void compare(String name, Map<String, String> docs, Set<String> hardDeletes,
                        Overlay overlay, Query query, boolean expectNaiveMismatch) throws Exception {
        try (Fixture base = new Fixture(docs, hardDeletes);
             Fixture oracle = new Fixture(apply(docs, hardDeletes, overlay));
             Request request = new Request(List.of(base), overlay)) {
            List<Hit> before = search(base.reader, query, null);
            List<Hit> actual = request.run(query);
            List<Hit> expected = search(oracle.reader, query, null);
            List<Hit> naive = search(request.reader, query, null);
            require(equal(actual, expected), name + ": effective scores/order differ from rebuilt oracle");
            require(ids(naive).stream().sorted().toList().equals(ids(expected).stream().sorted().toList()),
                name + ": negative-control membership differs");
            require(!expectNaiveMismatch || !equal(naive, expected), name + ": missing negative control");
            require(equal(search(base.reader, query, null), before), name + ": base mutated");
            System.out.println("{\"case\":\"" + name + "\",\"status\":\"passed\",\"actual\":" + json(actual) +
                ",\"oracle\":" + json(expected) + ",\"visibility_only\":" + json(naive) +
                ",\"naive_mismatch\":" + !equal(naive, expected) + "}");
        }
    }

    static void rankChange() throws Exception {
        Map<String, String> docs = new LinkedHashMap<>();
        docs.put("a", "alpha alpha"); docs.put("b", "beta");
        Set<String> deleted = new HashSet<>();
        for (int i = 0; i < 10; i++) { docs.put("x" + i, "alpha"); deleted.add("x" + i); }
        docs.put("y1", "beta"); docs.put("y2", "beta");
        Query query = query("alpha", "beta");
        Overlay overlay = new Overlay(Map.of(), deleted);
        try (Fixture base = new Fixture(docs);
             Fixture oracle = new Fixture(apply(docs, Set.of(), overlay));
             Request request = new Request(List.of(base), overlay)) {
            List<Hit> old = search(base.reader, query, null);
            List<Hit> actual = request.run(query);
            List<Hit> expected = search(oracle.reader, query, null);
            require(!old.getFirst().id.equals("a") && actual.getFirst().id.equals("a"), "rank did not flip");
            require(equal(actual, expected), "rank-change oracle mismatch");
            require(!ids(old.subList(0, 1)).contains(actual.getFirst().id), "old top-1 contains new winner");
            System.out.println("{\"case\":\"outside_old_top_k\",\"status\":\"passed\",\"old_top1\":" +
                json(old.subList(0, 1)) + ",\"new_top1\":" + json(actual.subList(0, 1)) + "}");
        }
    }

    static void partitionsAndIsolation() throws Exception {
        Map<String, String> docs = Map.of("a", "alpha alpha", "b", "beta", "c", "alpha beta", "d", "gamma");
        Overlay first = new Overlay(Map.of("a", "omega", "e", "alpha"), Set.of("b"));
        Overlay second = new Overlay(Map.of("a", "alpha alpha alpha"), Set.of("c"));
        Query query = query("alpha", "omega");
        try (Fixture whole = new Fixture(docs);
             Fixture left = new Fixture(Map.of("a", docs.get("a"), "b", docs.get("b")));
             Fixture right = new Fixture(Map.of("c", docs.get("c"), "d", docs.get("d")));
             Fixture oracle1 = new Fixture(apply(docs, Set.of(), first));
             Fixture oracle2 = new Fixture(apply(docs, Set.of(), second));
             Request one = new Request(List.of(whole), first);
             Request split = new Request(List.of(left, right), first);
             Request other = new Request(List.of(whole), second)) {
            List<Hit> expected1 = search(oracle1.reader, query, null);
            List<Hit> expected2 = search(oracle2.reader, query, null);
            require(equal(one.run(query), expected1) && equal(split.run(query), expected1), "partition layout drift");
            // Each leaf searches independently with the same effective global statistics.
            List<Hit> merged = new ArrayList<>();
            for (var leaf : split.reader.leaves()) { merged.addAll(search(leaf.reader(), query, split.stats)); }
            merged.sort(Comparator.comparingDouble(Hit::score).reversed().thenComparing(Hit::id));
            require(equal(merged, expected1), "split shared statistics differ");
            List<Hit> original = search(whole.reader, query, null);
            try (var pool = Executors.newFixedThreadPool(2)) {
                List<Callable<Boolean>> tasks = List.of(
                    () -> equal(one.run(query), expected1), () -> equal(other.run(query), expected2));
                for (var result : pool.invokeAll(tasks)) { require(result.get(), "request contamination"); }
            }
            require(equal(search(whole.reader, query, null), original), "concurrent overlay mutated base");
            require(whole.reader.getRefCount() > 0, "overlay closed borrowed base");
            System.out.println("{\"case\":\"partition_and_private_requests\",\"status\":\"passed\",\"first\":" +
                json(expected1) + ",\"second\":" + json(expected2) + "}");
        }
    }

    static void rejects() throws Exception {
        int rejected = 0;
        try { new Overlay(Map.of("a", "alpha"), Set.of("a")); }
        catch (IllegalArgumentException expected) { rejected++; }
        try { new Overlay(Map.of("a", "x".repeat(4097)), Set.of()); }
        catch (IllegalArgumentException expected) { rejected++; }
        Map<String, String> docs = new HashMap<>();
        for (int i = 0; i < LIMIT; i++) { docs.put("d" + i, "alpha"); }
        try (Fixture base = new Fixture(docs)) {
            int refs = base.reader.leaves().getFirst().reader().getRefCount();
            try (Request ignored = new Request(List.of(base), new Overlay(Map.of("extra", "beta"), Set.of()))) {
                throw new AssertionError("oversized effective corpus accepted: " + ignored.reader.numDocs());
            } catch (IllegalArgumentException expected) { rejected++; }
            require(base.reader.leaves().getFirst().reader().getRefCount() == refs, "failure leaked base reference");
            require(search(base.reader, query("alpha"), null).size() == LIMIT, "failure damaged base");
        }
        require(rejected == 3, "invalid request accepted");
        System.out.println("{\"case\":\"bounds_and_failure_cleanup\",\"status\":\"passed\",\"rejections\":3}");
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> base = Map.of("a", "alpha alpha beta", "b", "alpha", "c", "beta gamma", "d", "delta");
        compare("empty_overlay", base, Set.of(), new Overlay(Map.of(), Set.of()), query("alpha"), false);
        compare("delete_only", base, Set.of(), new Overlay(Map.of(), Set.of("b")), query("alpha"), true);
        compare("nonmatching_replacement", base, Set.of(), new Overlay(Map.of("a", "omega"), Set.of()), query("alpha"), true);
        compare("insert_new_term", base, Set.of(), new Overlay(Map.of("e", "omega omega"), Set.of()), query("omega"), false);
        compare("replace_frequency_length", base, Set.of(), new Overlay(Map.of("b", "alpha alpha alpha beta gamma gamma"), Set.of()), query("alpha", "beta"), true);
        compare("mixed_changes", base, Set.of(), new Overlay(Map.of("a", "alpha", "e", "beta beta"), Set.of("c")), query("alpha", "beta"), true);
        compare("retained_hard_deletes", base, Set.of("b"), new Overlay(Map.of(), Set.of()), query("alpha"), true);
        compare("all_deleted", base, Set.of(), new Overlay(Map.of(), base.keySet()), query("alpha"), false);
        compare("empty_text_and_unknown_delete", base, Set.of(), new Overlay(Map.of("a", ""), Set.of("absent")), query("alpha"), true);
        compare("removed_only_term", base, Set.of(), new Overlay(Map.of(), Set.of("d")), query("delta"), false);
        compare("no_match", base, Set.of(), new Overlay(Map.of(), Set.of()), query("missing"), false);
        rankChange();
        partitionsAndIsolation();
        rejects();
    }
}
