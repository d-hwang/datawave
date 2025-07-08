package datawave.query.index.lookup;

import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.filter;
import static com.google.common.collect.Iterators.transform;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.DELAYED;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.DROPPED;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EVALUATION_ONLY;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_TERM;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_VALUE;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.INDEX_HOLE;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.data.type.Type;
import datawave.ingest.mapreduce.handler.shard.NumShards;
import datawave.query.CloseableIterable;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.index.lookup.IndexStream.StreamContext;
import datawave.query.iterator.QueryOptions;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlASTHelper.IdentifierOpLiteral;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.DepthVisitor;
import datawave.query.jexl.visitors.EvaluationRendering;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import datawave.query.jexl.visitors.order.OrderByCostVisitor;
import datawave.query.planner.QueryPlan;
import datawave.query.tables.RangeStreamScanner;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.SessionOptions;
import datawave.query.util.MetadataHelper;
import datawave.query.util.QueryScannerHelper;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuples;
import datawave.util.StringUtils;
import datawave.util.TableName;
import datawave.util.time.DateHelper;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.PreConditionFailedQueryException;
import datawave.webservice.query.exception.QueryException;

public class RangeStream extends BaseVisitor implements CloseableIterable<QueryPlan> {

    private static final int MAX_MEDIAN = 20;

    private static final Logger log = ThreadConfigurableLogger.getLogger(RangeStream.class);

    /**
     * An assignment to this variable can be used to specify a stream of shards and days anywhere in the query. Used by the date function index query creation.
     */

    protected final ShardQueryConfiguration config;
    protected final ScannerFactory scanners;
    protected final MetadataHelper metadataHelper;
    protected Iterator<QueryPlan> itr;
    protected StreamContext context;
    protected BaseIndexStream queryStream;
    protected boolean limitScanners = false;
    protected Class<? extends SortedKeyValueIterator<Key,Value>> createUidsIteratorClass = CreateUidsIterator.class;
    protected Multimap<String,Type<?>> fieldDataTypes;

    protected BlockingQueue<Runnable> runnables;

    protected JexlNode tree = null;

    protected UidIntersector uidIntersector = new IndexInfo();

    /**
     * Intended to reduce the cost of repeated calls to helper.getAllFields
     */
    protected Set<String> helperAllFieldsCache = new HashSet<>();

    private int maxScannerBatchSize;

    protected ExecutorService executor;

    protected ExecutorService streamExecutor;

    protected boolean collapseUids = false;
    protected boolean fieldCounts = false;
    protected boolean termCounts = false;

    protected Set<String> indexOnlyFields = Sets.newHashSet();

    protected NumShardFinder numShardFinder;

    public RangeStream(ShardQueryConfiguration config, ScannerFactory scanners, MetadataHelper metadataHelper) {
        this.config = config;
        this.scanners = scanners;
        this.metadataHelper = metadataHelper;
        int maxLookup = (int) Math.max(config.getNumIndexLookupThreads(), 1);
        executor = Executors.newFixedThreadPool(maxLookup);
        runnables = new LinkedBlockingDeque<>();
        int executeLookupMin = Math.max(maxLookup / 2, 1);
        streamExecutor = new ThreadPoolExecutor(executeLookupMin, maxLookup, 100, TimeUnit.MILLISECONDS, runnables);
        fieldDataTypes = config.getQueryFieldsDatatypes();
        collapseUids = config.getCollapseUids();
        fieldCounts = config.isSortQueryPostIndexWithFieldCounts();
        termCounts = config.isSortQueryPostIndexWithTermCounts();
        try {
            Set<String> ioFields = metadataHelper.getIndexOnlyFields(null);
            if (null != ioFields) {
                indexOnlyFields.addAll(ioFields);
            }
        } catch (TableNotFoundException e) {
            // ignore
        }
    }

    public CloseableIterable<QueryPlan> streamPlans(JexlNode script) {
        JexlNode node = TreeFlatteningRebuildingVisitor.flatten(script);

        tree = node;

        if (!collapseUids && config.getParseTldUids()) {
            collapseUids = !(EvaluationRendering.canDisableEvaluation(script, config, metadataHelper, true));

            if (log.isTraceEnabled()) {
                log.trace("New query is " + JexlStringBuildingVisitor.buildQuery(tree));
                log.trace("Collapse UIDs is now " + collapseUids + " because we have a TLD Query with an ivarator");
            }
        }

        // check the query depth (up to config.getMaxDepthThreshold() + 1)
        int depth = DepthVisitor.getDepth(node, config.getMaxDepthThreshold());
        if (depth > config.getMaxDepthThreshold()) {
            PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_DEPTH_THRESHOLD_EXCEEDED,
                            MessageFormat.format("{0} > {1}, last operation: {2}", depth, config.getMaxDepthThreshold(), "RangeStreamLookup"));
            throw new DatawaveFatalQueryException(qe);
        }

        if (log.isTraceEnabled()) {
            log.trace(JexlStringBuildingVisitor.buildQuery(node));
        }

        BaseIndexStream ranges = (BaseIndexStream) node.jjtAccept(this, null);

        // Guards against the case of a very oddly formed JEXL query, e.g. ("foo")
        if (null == ranges) {
            this.context = StreamContext.ABSENT;
            this.itr = Collections.emptyIterator();
        } else {
            // we can build the iterator at a later point, grabbing the top most
            // context. This will usually provide us a hint about the context
            // within our stream.
            context = ranges.context();
            this.itr = null;
        }
        if (log.isDebugEnabled()) {
            log.debug("Query returned a stream with a context of " + this.context);
            if (queryStream != null) {
                for (String line : StringUtils.split(queryStream.getContextDebug(), '\n')) {
                    log.debug(line);
                }
            }
        }

        this.queryStream = ranges;

        return this;
    }

    /**
     * Call shutdownNow() on the underlying {@link ExecutorService}
     */
    protected void shutdownThreads() {
        executor.shutdownNow();
    }

    @Override
    public Iterator<QueryPlan> iterator() {
        try {
            if (null == itr) {
                if (queryStream.context() == StreamContext.INITIALIZED) {
                    List<ConcurrentScannerInitializer> todo = Lists.newArrayList();
                    todo.add(new ConcurrentScannerInitializer(queryStream));
                    Collection<BaseIndexStream> streams = ConcurrentScannerInitializer.initializeScannerStreams(todo, executor);
                    if (streams.size() == 1) {
                        queryStream = streams.iterator().next();
                    }
                }

                if (queryStream instanceof Intersection) {
                    switch (queryStream.context()) {
                        case VARIABLE:
                            // a union with a mix of executable and non-executable terms is still executable
                            // touch up the context to reflect this
                            this.context = StreamContext.PRESENT;
                            break;
                        case ABSENT:
                        case DELAYED:
                        case INITIALIZED:
                        case NO_OP:
                        case PRESENT:
                        default:
                            this.context = queryStream.context();
                    }
                } else if (queryStream instanceof Union) {
                    switch (queryStream.context()) {
                        case VARIABLE:
                            // all terms in a top level union must be executable
                            this.context = StreamContext.ABSENT;
                            this.itr = Collections.emptyIterator();
                            return itr;
                        case ABSENT:
                        case DELAYED:
                        case INITIALIZED:
                        case NO_OP:
                        case PRESENT:
                        default:
                            this.context = queryStream.context();
                    }
                } else {
                    // use the delegate context
                    context = queryStream.context();
                }

                if (log.isDebugEnabled()) {
                    log.debug("Query returned a stream with a context of " + this.context);
                    for (String line : StringUtils.split(queryStream.getContextDebug(), '\n')) {
                        log.debug(line);
                    }
                }

                this.itr = filter(concat(transform(queryStream, new TupleToRange(config.getShardTableName(), queryStream.currentNode(), config))),
                                getEmptyPlanPruner());

                if (config.isSortQueryPostIndexWithFieldCounts() || config.isSortQueryPostIndexWithTermCounts()) {
                    this.itr = transform(itr, new OrderingTransform(config.isSortQueryPostIndexWithFieldCounts(), config.isSortQueryPostIndexWithTermCounts()));
                }
            }
        } finally {
            // shut down the executor as all threads have completed
            shutdownThreads();
        }
        return itr;
    }

    public EmptyPlanPruner getEmptyPlanPruner() {
        return new EmptyPlanPruner();
    }

    /**
     * This class will prune a QueryPlan if the ranges are empty
     */
    public static class EmptyPlanPruner implements Predicate<QueryPlan> {

        public boolean apply(QueryPlan plan) {
            return plan.getRanges().iterator().hasNext();
        }
    }

    /**
     * Transform that reorders a query tree according to field or term counts.
     * <p>
     * If both flags are set then the more precise term counts are used.
     */
    public static class OrderingTransform implements Function<QueryPlan,QueryPlan> {

        private final boolean useFieldCounts;
        private final boolean useTermCounts;

        public OrderingTransform(boolean useFieldCounts, boolean useTermCounts) {
            this.useFieldCounts = useFieldCounts;
            this.useTermCounts = useTermCounts;
        }

        @Override
        public QueryPlan apply(QueryPlan plan) {
            if (useTermCounts) {
                Map<String,Long> counts = plan.getTermCounts().getCounts();
                OrderByCostVisitor.orderByTermCount(plan.getQueryTree(), counts);
            } else if (useFieldCounts) {
                Map<String,Long> counts = plan.getFieldCounts().getCounts();
                OrderByCostVisitor.orderByFieldCount(plan.getQueryTree(), counts);
            }
            return plan;
        }
    }

    public StreamContext context() {
        return context;
    }

    @Override
    public IndexStream visit(ASTOrNode node, Object data) {
        Union.Builder builder = Union.builder();
        List<ConcurrentScannerInitializer> todo = Lists.newArrayList();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            BaseIndexStream child = (BaseIndexStream) node.jjtGetChild(i).jjtAccept(this, builder);
            if (null != child) {
                todo.add(new ConcurrentScannerInitializer(child));
            }
        }

        builder.addChildren(todo);

        if (data instanceof Union.Builder) {
            log.debug("[ASTOrNode] Propagating children up to parent because nodes of the same type.");
            Union.Builder parent = (Union.Builder) data;
            parent.consume(builder);
            return ScannerStream.noOp(node);

        } else if (builder.size() == 0) {
            return ScannerStream.noData(node);
        } else {

            Union union = builder.build(executor);

            switch (union.context()) {
                case ABSENT:
                    return ScannerStream.noData(union.currentNode(), union);
                case DELAYED:
                    return ScannerStream.delayed(union.currentNode());
                case PRESENT:
                case VARIABLE:
                    return union;
                case INITIALIZED:
                default:
                    throw new RuntimeException("unhandled scanner context");
            }
        }
    }

    @Override
    public IndexStream visit(ASTAndNode node, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        // if we have a term threshold marker, then we simply could not expand an _ANYFIELD_ identifier, so return EXCEEDED_THRESHOLD
        if (instance.isType(EXCEEDED_TERM)) {
            return ScannerStream.delayed(node);
        } else if (instance.isAnyTypeOf(EXCEEDED_VALUE, EXCEEDED_OR)) {
            try {
                // When we exceeded the expansion threshold for a regex, the field is an index-only field, and we can't
                // hook up the hdfs-sorted-set iterator (Ivarator), we can't run the query via the index or
                // full-table-scan, so we throw an Exception
                if (!config.canHandleExceededValueThreshold() && containsIndexOnlyFields(node)) {
                    QueryException qe = new QueryException(DatawaveErrorCode.EXPAND_QUERY_TERM_SYSTEM_LIMITS);
                    throw new DatawaveFatalQueryException(qe);
                }
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.NODE_PROCESSING_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            }

            // create a list of tuples for each shard
            if (log.isDebugEnabled()) {
                LiteralRange<?> range = JexlASTHelper.findRange().indexedOnly(config.getDatatypeFilter(), metadataHelper).getRange(node);
                if (range != null) {
                    log.debug("{\"" + range.getFieldName() + "\": \"" + range.getLower() + " - " + range.getUpper() + "\"} requires a full field index scan.");
                } else {
                    log.debug("{\"" + JexlASTHelper.getLiterals(node) + "\"} requires a full field index scan.");
                }
            }

            JexlNode wrapped = JexlNodes.wrap(node);
            ShardSpecificIndexIterator iter = new ShardSpecificIndexIterator(wrapped, getNumShardFinder(), config.getBeginDate(), config.getEndDate());
            return ScannerStream.withData(iter, wrapped);

        } else if (instance.isAnyTypeOf(DELAYED, EVALUATION_ONLY)) {
            return ScannerStream.delayed(node);
        } else if (instance.isType(DROPPED)) {
            return ScannerStream.noOp(node);
        } else if (instance.isType(INDEX_HOLE)) {
            return ScannerStream.delayed(node);
        } else if (instance.isType(BOUNDED_RANGE)) {
            LiteralRange<?> range = JexlASTHelper.findRange().getRange(node);
            if (range == null) {
                throw new RuntimeException("BoundedRange was null");
            }

            if (isIndexed(range.getFieldName(), config.getIndexedFields())) {
                JexlNode wrapped = JexlNodes.wrap(node);
                ShardSpecificIndexIterator iter = new ShardSpecificIndexIterator(wrapped, getNumShardFinder(), config.getBeginDate(), config.getEndDate());
                return ScannerStream.withData(iter, wrapped);
            }

            // here we must have a bounded range that was not expanded, so it must not be expandable via the index
            return ScannerStream.delayed(node);
        } else {
            Intersection.Builder builder = Intersection.builder();
            builder.setUidIntersector(uidIntersector);

            // join the index streams
            List<ConcurrentScannerInitializer> todo = Lists.newArrayList();
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                BaseIndexStream child = (BaseIndexStream) node.jjtGetChild(i).jjtAccept(this, builder);
                if (null != child) {
                    todo.add(new ConcurrentScannerInitializer(child));
                }
            }

            builder.addChildren(todo);

            if (data instanceof Intersection.Builder) {
                log.debug("[ASTAndNode] Propagating children up to parent because nodes of the same type.");
                Intersection.Builder parent = (Intersection.Builder) data;
                parent.consume(builder);

                return ScannerStream.noOp(node);

            } else if (builder.size() == 0) {
                return ScannerStream.noData(node);
            } else {
                Intersection build = builder.build(executor);
                switch (build.context()) {
                    case ABSENT:
                        return ScannerStream.noData(build.currentNode(), build);
                    case PRESENT:
                    case VARIABLE:
                    case DELAYED:
                        return build;
                    case INITIALIZED:
                    default:
                        // if the intersection's context is still INITIALIZED after the initializer is run then something is very, very wrong
                        throw new RuntimeException("unhandled stream context: " + build.context());
                }
            }
        }
    }

    @Override
    public ScannerStream visit(ASTEQNode node, Object data) {

        if (isUnOrNotFielded(node)) {
            return ScannerStream.noData(node);
        }

        // We are looking for identifier = literal
        IdentifierOpLiteral op = JexlASTHelper.getIdentifierOpLiteral(node);
        if (op == null) {
            return ScannerStream.delayed(node);
        }

        final String fieldName = op.deconstructIdentifier();

        // Null literals cannot be resolved against the index.
        if (op.getLiteralValue() == null) {
            return ScannerStream.delayed(node);
        }

        // toString of String returns the String
        String literal = op.getLiteralValue().toString();

        if (QueryOptions.DEFAULT_DATATYPE_FIELDNAME.equals(fieldName)) {
            return ScannerStream.delayed(node);
        }

        // Check if field is not indexed
        if (!isIndexed(fieldName, config.getIndexedFields())) {
            try {
                if (this.getAllFieldsFromHelper().contains(fieldName)) {
                    log.debug("{\"" + fieldName + "\": \"" + literal + "\"} is not indexed.");
                    return ScannerStream.delayed(node);
                }
            } catch (TableNotFoundException e) {
                log.error(e);
                throw new RuntimeException(e);
            }
            log.debug("{\"" + fieldName + "\": \"" + literal + "\"} is not an observed field.");

            // even though the field is not indexed it may still be valuable when evaluating an event. mark this scanner stream as delayed, so it is correctly
            // propagated
            return ScannerStream.delayed(node);
        }

        // Final case, field is indexed
        log.debug("\"" + fieldName + "\" is indexed. for " + literal);
        try {

            int stackStart = config.getBaseIteratorPriority();

            RangeStreamScanner scannerSession;

            SessionOptions options = new SessionOptions();
            options.fetchColumnFamily(new Text(fieldName));
            options.addScanIterator(makeDataTypeFilter(config, stackStart++));

            final IteratorSetting uidSetting;

            // Create the range for the term from the provided config.
            Range range = rangeForTerm(literal, fieldName, config);

            if (limitScanners) {
                // Setup the CreateUidsIterator
                scannerSession = scanners.newRangeScanner(config.getIndexTableName(), config.getAuthorizations(), config.getQuery());

                uidSetting = new IteratorSetting(stackStart++, createUidsIteratorClass);
                uidSetting.addOption(CreateUidsIterator.COLLAPSE_UIDS, Boolean.toString(collapseUids));
                uidSetting.addOption(CreateUidsIterator.PARSE_TLD_UIDS, Boolean.toString(config.getParseTldUids()));
                uidSetting.addOption(CreateUidsIterator.FIELD_COUNTS, Boolean.toString(fieldCounts));
                uidSetting.addOption(CreateUidsIterator.TERM_COUNTS, Boolean.toString(termCounts));

            } else {
                // Setup so this is a pass-through
                scannerSession = scanners.newRangeScanner(config.getIndexTableName(), config.getAuthorizations(), config.getQuery());

                uidSetting = new IteratorSetting(stackStart++, createUidsIteratorClass);
                uidSetting.addOption(CreateUidsIterator.COLLAPSE_UIDS, Boolean.toString(false));
                uidSetting.addOption(CreateUidsIterator.PARSE_TLD_UIDS, Boolean.toString(false));
                uidSetting.addOption(CreateUidsIterator.FIELD_COUNTS, Boolean.toString(false));
                uidSetting.addOption(CreateUidsIterator.TERM_COUNTS, Boolean.toString(false));
            }

            /*
             * Create a scanner in the initialized state so that we can scan immediately
             */
            if (log.isTraceEnabled()) {
                log.trace("Building delayed scanner for " + fieldName + ", literal= " + literal);
            }

            // Configure common settings on the ScannerSession
            options.addScanIterator(uidSetting);

            String queryString = fieldName + "=='" + literal + "'";
            options.addScanIterator(QueryScannerHelper.getQueryInfoIterator(config.getQuery(), false, queryString));

            // easier to apply hints to new options than deal with copying existing hints between
            options.applyExecutionHints(config.getIndexTableName(), config.getTableHints());
            options.applyConsistencyLevel(config.getIndexTableName(), config.getTableConsistencyLevels());

            scannerSession.setOptions(options);
            scannerSession.setMaxResults(config.getMaxIndexBatchSize());
            scannerSession.setExecutor(streamExecutor);
            scannerSession.setRanges(Collections.singleton(range));

            // Create the EntryParser prior to ScannerStream.
            EntryParser entryParser = new EntryParser(node, fieldName, literal, indexOnlyFields);

            return ScannerStream.initialized(scannerSession, entryParser, node);

        } catch (Exception e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

    /*
     * Presume that functions have already been expanded with their index query parts @see QueryIndexQueryExpandingVisitor
     */
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        if (log.isTraceEnabled()) {
            log.trace("building delayed expression for function");
        }
        return ScannerStream.delayed(node);
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return ScannerStream.delayed(node);
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        if (log.isTraceEnabled()) {
            log.trace("NOT FIELD " + JexlStringBuildingVisitor.buildQuery(node));
        }
        return ScannerStream.delayed(node);
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        IdentifierOpLiteral op = JexlASTHelper.getIdentifierOpLiteral(node);
        if (op == null) {
            return ScannerStream.delayed(node);
        }

        final String fieldName = op.deconstructIdentifier();

        // HACK to make EVENT_DATATYPE queries work
        if (QueryOptions.DEFAULT_DATATYPE_FIELDNAME.equals(fieldName)) {
            return ScannerStream.delayed(node);
        }

        if (isUnOrNotFielded(node)) {
            return ScannerStream.delayed(node);
        }

        if (isUnindexed(node)) {
            return ScannerStream.delayed(node);
        }

        try {
            if (!this.getAllFieldsFromHelper().contains(fieldName)) {
                return ScannerStream.delayed(node);
            }
        } catch (TableNotFoundException e) {
            log.error(e);
            throw new RuntimeException(e);
        }

        return ScannerStream.noData(node);
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return ScannerStream.delayed(node);
    }

    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return ScannerStream.delayed(node);
    }

    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return ScannerStream.noData(node);
    }

    private boolean isUnOrNotFielded(JexlNode node) {
        List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(node);
        for (ASTIdentifier identifier : identifiers) {
            if (identifier.getName().equals(Constants.ANY_FIELD) || identifier.getName().equals(Constants.NO_FIELD)) {
                return true;
            }
        }
        return false;
    }

    private boolean isUnindexed(JexlNode node) {
        List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(node);
        for (ASTIdentifier identifier : identifiers) {
            try {
                if (!(identifier.getName().equals(Constants.ANY_FIELD) || identifier.getName().equals(Constants.NO_FIELD))) {
                    if (!metadataHelper.isIndexed(JexlASTHelper.deconstructIdentifier(identifier), config.getDatatypeFilter())) {
                        return true;
                    }
                }
            } catch (TableNotFoundException e) {
                log.error("Could not determine whether field is indexed", e);
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        if (isUnOrNotFielded(node)) {
            return ScannerStream.noData(node);
        }

        if (isUnindexed(node)) {
            return ScannerStream.delayed(node);
        }

        return ScannerStream.delayed(node);
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        if (isUnOrNotFielded(node)) {
            return ScannerStream.noData(node);
        }

        if (isUnindexed(node)) {
            return ScannerStream.delayed(node);
        }

        return ScannerStream.delayed(node);
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        if (isUnOrNotFielded(node)) {
            return ScannerStream.noData(node);
        }

        if (isUnindexed(node)) {
            return ScannerStream.delayed(node);
        }

        return ScannerStream.delayed(node);
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        if (isUnOrNotFielded(node)) {
            return ScannerStream.noData(node);
        }

        if (isUnindexed(node)) {
            return ScannerStream.delayed(node);
        }

        return ScannerStream.delayed(node);
    }

    public Object descend(JexlNode node, Object data) {
        if (node.jjtGetNumChildren() > 1) {

            QueryException qe = new QueryException(DatawaveErrorCode.MORE_THAN_ONE_CHILD, MessageFormat.format("Class: {0}", node.getClass().getSimpleName()));
            throw new DatawaveFatalQueryException(qe);
        }
        if (node.jjtGetNumChildren() == 1) {
            return node.jjtGetChild(0).jjtAccept(this, data);
        } else {
            return data;
        }
    }

    @Override
    public Object visit(ASTJexlScript node, Object data) {
        return descend(node, data);
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        return descend(node, data);
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return descend(node, data);
    }

    @Override
    public Object visit(ASTAssignment node, Object data) {
        // If we have an assignment of shards/days, then generate a stream of shards/days
        String identifier = JexlASTHelper.getIdentifier(node);
        if (Constants.SHARD_DAY_HINT.equals(identifier)) {
            JexlNode myNode = JexlNodeFactory.createExpression(node);
            String[] shardsAndDays = StringUtils.split(JexlASTHelper.getLiteralValue(node).toString(), ',');

            if (shardsAndDays.length == 0) {
                return ScannerStream.noData(myNode);
            }

            // it is important that we check for a day range. in that case we need to build a different iterator
            // to preserve search parallelism.
            boolean hintContainsDays = checkHintForDays(shardsAndDays);
            if (hintContainsDays) {
                // need to create special purpose iterator
                HintToShardIterator shim = new HintToShardIterator(shardsAndDays, getNumShardFinder());
                return ScannerStream.withData(shim, myNode);
            }

            return ScannerStream.withData(createIndexScanList(shardsAndDays).iterator(), myNode);
        }
        return null;
    }

    /**
     * Checks an array of shard and day hints to see if it contains a day (i.e., no underscore)
     *
     * @param shardsAndDays
     *            an array of shards and days hints
     * @return true if a day hint is present
     */
    private boolean checkHintForDays(String[] shardsAndDays) {
        for (String hint : shardsAndDays) {
            if (!hint.contains("_")) {
                return true;
            }
        }
        return false;
    }

    protected synchronized NumShardFinder getNumShardFinder() {
        if (numShardFinder == null) {
            numShardFinder = new NumShardFinder(config.getClient());
        }
        return numShardFinder;
    }

    /**
     * Minimal code required to populate the num shards cache
     */
    public static class NumShardFinder {

        protected final AccumuloClient client;
        protected final TreeMap<String,Integer> cache = new TreeMap<>();

        public NumShardFinder(AccumuloClient client) {
            this.client = client;
            // prepopulate the cache
            populateCache();
        }

        public int getNumShards(String day) {
            // this object could be called from multiple threads via the concurrent scanner initializer
            // so synchronize for safety
            synchronized (cache) {
                Map.Entry<String,Integer> entry = cache.floorEntry(day);
                if (entry != null) {
                    return entry.getValue();
                }
                return 0;
            }
        }

        private void populateCache() {
            if (client == null) {
                log.warn("no client configured, will not populate num shards");
                return;
            }
            try (Scanner scanner = client.createScanner(TableName.METADATA)) {
                scanner.setRange(Range.exact(NumShards.NUM_SHARDS, NumShards.NUM_SHARDS_CF));
                int scannedKeys = 0;
                for (Map.Entry<Key,Value> entry : scanner) {
                    // num_shards ns:date_shards
                    // num_shards ns:20050207_17
                    String cq = entry.getKey().getColumnQualifier().toString();
                    if (!cq.contains("_")) {
                        log.warn("invalid num_shards entry");
                        continue;
                    }

                    scannedKeys++;
                    String[] parts = cq.split("_");
                    cache.put(parts[0], Integer.parseInt(parts[1]));
                }

                if (scannedKeys == 0) {
                    log.fatal("no entries in num_shards cache");
                }
            } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
                // an exception here shouldn't kill the query
                log.warn("exception thrown while trying to scan num shards cache: " + e.getMessage());
            }
        }

    }

    public Range rangeForTerm(String term, String field, ShardQueryConfiguration config) {
        return rangeForTerm(term, field, config.getBeginDate(), config.getEndDate());
    }

    public Range rangeForTerm(String term, String field, Date start, Date end) {
        return new Range(new Key(term, field, DateHelper.format(start) + "_"), true, new Key(term, field, DateHelper.format(end) + "_" + '\uffff'), false);
    }

    public static IteratorSetting makeDataTypeFilter(ShardQueryConfiguration config, int stackPosition) {
        IteratorSetting is = new IteratorSetting(stackPosition, DataTypeFilter.class);
        is.addOption(DataTypeFilter.TYPES, config.getDatatypeFilterAsString());
        return is;
    }

    public static boolean isIndexed(String field, Multimap<String,Type<?>> ctx) {
        Collection<Type<?>> norms = ctx.get(field);

        return !norms.isEmpty();
    }

    public static boolean isIndexed(String field, Set<String> ctx) {

        return ctx.contains(field);
    }

    public static boolean isNormalized(String field, Set<String> ctx) {
        return ctx.contains(field);
    }

    /**
     * This will create a list of index info (ranges) of the form yyyyMMdd for each day is the specified query date range. Each IndexInfo will have a count of
     * -1 (unknown, assumed infinite)
     *
     * @param config
     *            a ShardQueryConfiguration
     * @param node
     *            a JexlNode
     * @return The list of index info ranges
     */
    @Deprecated(forRemoval = true)
    public static List<Tuple2<String,IndexInfo>> createFullFieldIndexScanList(ShardQueryConfiguration config, JexlNode node) {
        List<Tuple2<String,IndexInfo>> list = new ArrayList<>();

        Calendar start = getCalendarStartOfDay(config.getBeginDate());
        Calendar end = getCalendarStartOfDay(config.getEndDate());

        while (start.compareTo(end) <= 0) {
            String day = DateHelper.format(start.getTime());
            IndexInfo info = new IndexInfo(-1);
            info.setNode(node);
            list.add(Tuples.tuple(day, info));
            start.add(Calendar.DAY_OF_YEAR, 1);
        }
        return list;
    }

    private static Calendar getCalendarStartOfDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar;
    }

    /**
     * This will create a list of index info (ranges) for the specified array of shards and days.
     *
     * @param shardsAndDays
     *            of shards and days
     * @return The list of index info ranges
     */
    public static List<Tuple2<String,IndexInfo>> createIndexScanList(String[] shardsAndDays) {
        List<Tuple2<String,IndexInfo>> list = new ArrayList<>();
        Arrays.sort(shardsAndDays);
        for (String shardOrDay : shardsAndDays) {
            IndexInfo info = new IndexInfo(-1);
            // create a new assignment node with just this shardOrDay
            JexlNode newNode = JexlNodeFactory.createExpression(JexlNodeFactory.createAssignment(Constants.SHARD_DAY_HINT, shardOrDay));
            info.setNode(newNode);
            list.add(Tuples.tuple(shardOrDay, info));
        }
        return list;
    }

    /**
     * Setter for limit scanners
     *
     * @param limitScanners
     *            flag for the limit scanners
     * @return the range stream
     */
    public RangeStream setLimitScanners(final boolean limitScanners) {
        this.limitScanners = limitScanners;
        return this;
    }

    public boolean limitedScanners() {
        return limitScanners;
    }

    public void setMaxScannerBatchSize(int maxScannerBatchSize) {
        this.maxScannerBatchSize = maxScannerBatchSize;
    }

    public int getMaxScannerBatchSize() {
        return maxScannerBatchSize;
    }

    public UidIntersector getUidIntersector() {
        return uidIntersector;
    }

    public RangeStream setUidIntersector(UidIntersector uidIntersector) {
        this.uidIntersector = uidIntersector;
        return this;
    }

    public Class<? extends SortedKeyValueIterator<Key,Value>> getCreateUidsIteratorClass() {
        return createUidsIteratorClass;
    }

    public RangeStream setCreateUidsIteratorClass(Class<? extends SortedKeyValueIterator<Key,Value>> createUidsIteratorClass) {
        this.createUidsIteratorClass = createUidsIteratorClass;
        return this;
    }

    protected Set<String> getAllFieldsFromHelper() throws TableNotFoundException {
        if (this.helperAllFieldsCache.isEmpty()) {
            this.helperAllFieldsCache = this.metadataHelper.getAllFields(this.config.getDatatypeFilter());
        }
        return this.helperAllFieldsCache;
    }

    public static boolean isEventSpecific(Range range) {
        Text holder = new Text();
        Key startKey = range.getStartKey();
        startKey.getColumnFamily(holder);
        if (holder.getLength() > 0) {
            if (holder.find("\0") > 0) {
                return true;
            }
        }
        return false;
    }

    protected boolean containsIndexOnlyFields(JexlNode node) throws TableNotFoundException {
        List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(node);

        Set<String> indexOnlyFields = metadataHelper.getIndexOnlyFields(config.getDatatypeFilter());

        // Hack to get around the extra ASTIdentifier left in the AST by the threshold marker node
        Iterator<ASTIdentifier> iter = identifiers.iterator();
        while (iter.hasNext()) {
            ASTIdentifier id = iter.next();
            if (EXCEEDED_VALUE.getLabel().equals(id.getName()) || EXCEEDED_TERM.getLabel().equals(id.getName())
                            || EXCEEDED_OR.getLabel().equals(id.getName())) {
                iter.remove();
            }
        }

        for (ASTIdentifier identifier : identifiers) {
            String fieldName = JexlASTHelper.deconstructIdentifier(identifier);

            if (indexOnlyFields.contains(fieldName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        streamExecutor.shutdownNow();
        executor.shutdownNow();
    }
}
