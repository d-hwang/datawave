package nsa.datawave.query.index.lookup;

import nsa.datawave.query.util.Tuple2;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Specialty AncestorIndexStream implementation
 */
public class AncestorIndexStream implements IndexStream {
    private final IndexStream delegate;
    private final JexlNode parent;
    
    public AncestorIndexStream(IndexStream delegate) {
        this(delegate, null);
    }
    
    public AncestorIndexStream(IndexStream delegate, JexlNode parent) {
        this.delegate = delegate;
        this.parent = parent;
    }
    
    @Override
    public StreamContext context() {
        return delegate.context();
    }
    
    @Override
    public String getContextDebug() {
        return delegate.getContextDebug();
    }
    
    @Override
    public JexlNode currentNode() {
        return delegate.currentNode();
    }
    
    @Override
    public Tuple2<String,IndexInfo> peek() {
        return removeOverlappingRanges(delegate.peek());
    }
    
    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }
    
    @Override
    public Tuple2<String,IndexInfo> next() {
        return removeOverlappingRanges(delegate.next());
    }
    
    @Override
    public void remove() {
        delegate.remove();
    }
    
    /**
     * For a given tuple, remove any overlapping ranges defined by documents that are ancestors of each other
     * 
     * @param tuple
     * @return
     */
    private Tuple2<String,IndexInfo> removeOverlappingRanges(Tuple2<String,IndexInfo> tuple) {
        IndexInfo info = tuple.second();
        Set<IndexMatch> matches = info.uids();
        
        Map<JexlNode,Set<IndexMatch>> nodeMap = new HashMap<>();
        for (IndexMatch match : matches) {
            JexlNode current = match.getNode();
            
            Set<IndexMatch> existing = nodeMap.get(current);
            if (existing == null) {
                existing = new TreeSet<>();
                nodeMap.put(current, existing);
            }
            
            boolean add = true;
            for (IndexMatch indexMatch : existing) {
                if (match.getUid().indexOf(indexMatch.getUid()) > -1) {
                    add = false;
                }
            }
            
            if (add) {
                existing.add(match);
            }
        }
        
        // aggregate all the TreeSets into a single set
        Set<IndexMatch> allMatches = new TreeSet<>();
        for (Set<IndexMatch> nodeMatches : nodeMap.values()) {
            allMatches.addAll(nodeMatches);
        }
        IndexInfo newInfo = new IndexInfo(allMatches);
        newInfo.myNode = info.myNode;
        
        return new Tuple2<>(tuple.first(), newInfo);
    }
}
