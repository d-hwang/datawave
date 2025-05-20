package datawave.query.tables.keyword;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.easymock.PowerMock.expectLastCall;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;

import com.google.common.collect.Sets;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.config.KeywordQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.keyword.KeywordResults;
import datawave.webservice.query.exception.QueryException;

@SuppressWarnings("rawtypes")
public class KeywordQueryLogicTest {
    private KeywordQueryLogic keywordQueryLogic;
    private ScannerFactory mockScannerFactory;
    private BatchScanner mockScanner;
    private GenericQueryConfiguration mockGenericConfig;
    private KeywordQueryConfiguration mockKeywordConfig;
    @Mock
    Query query;

    @Before
    public void setup() throws TableNotFoundException {
        keywordQueryLogic = new KeywordQueryLogic();
        mockScannerFactory = mock(ScannerFactory.class);
        mockScanner = mock(BatchScanner.class);
        mockGenericConfig = mock(GenericQueryConfiguration.class);
        mockKeywordConfig = mock(KeywordQueryConfiguration.class);
        final KeywordQueryState mockKeywordState = mock(KeywordQueryState.class);

        keywordQueryLogic.scannerFactory = mockScannerFactory;
        when(mockScannerFactory.newScanner(any(), any(), anyInt(), any())).thenReturn(mockScanner);
        when(mockKeywordConfig.getState()).thenReturn(mockKeywordState);
    }

    @Test
    public void setupQueryInvalidConfigurationThrowsException() {
        assertThrows(QueryException.class, () -> keywordQueryLogic.setupQuery(mockGenericConfig));
    }

    @Test
    public void setupQueryValidConfigurationSetsUpScanner() throws Exception {
        keywordQueryLogic.setupQuery(mockKeywordConfig);
        verify(mockScanner).setRanges(any());
    }

    @Test
    public void setupQueryWithViewNameSetsIteratorSetting() throws Exception {
        keywordQueryLogic.setupQuery(mockKeywordConfig);
        keywordQueryLogic.getConfig().getState().getPreferredViews().add("FOO");
        verify(mockScanner).addScanIterator(any());
    }

    @Test
    public void setupQueryTableNotFoundThrowsRuntimeException() throws Exception {
        when(mockScannerFactory.newScanner(any(), any(), anyInt(), any())).thenThrow(TableNotFoundException.class);
        assertThrows(RuntimeException.class, () -> keywordQueryLogic.setupQuery(mockKeywordConfig));
    }

    @Test
    public void testConstructorCopy() {
        // borrowed from TestBaseQueryLogic.java
        KeywordQueryLogic subject = new TestKeywordQuery();
        int result1 = subject.getMaxPageSize();
        long result2 = subject.getPageByteTrigger();
        TransformIterator result3 = subject.getTransformIterator(this.query);
        verifyAll();

        // Verify results
        assertEquals("Incorrect max page size", 0, result1);
        assertEquals("Incorrect page byte trigger", 0, result2);
        assertNotNull("Iterator should not be null", result3);
    }

    @Test
    public void testContainsDnWithAccess() {
        // borrowed from TestBaseQueryLogic.java
        Set<String> dns = Sets.newHashSet("dn=user", "dn=user chain 1", "dn=user chain 2");
        KeywordQueryLogic logic = new TestKeywordQuery();

        // Assert cases given allowedDNs == null. Access should not be blocked at all.
        assertTrue(logic.containsDNWithAccess(dns));
        assertTrue(logic.containsDNWithAccess(null));
        assertTrue(logic.containsDNWithAccess(Collections.emptySet()));

        // Assert cases given allowedDNs == empty set. Access should not be blocked at all.
        logic.setAuthorizedDNs(Collections.emptySet());
        assertTrue(logic.containsDNWithAccess(dns));
        assertTrue(logic.containsDNWithAccess(null));
        assertTrue(logic.containsDNWithAccess(Collections.emptySet()));

        // Assert cases given allowedDNs == non-empty set with matching DN. Access should only be granted where DN is present.
        logic.setAuthorizedDNs(Sets.newHashSet("dn=user", "dn=other user"));
        assertTrue(logic.containsDNWithAccess(dns));
        assertFalse(logic.containsDNWithAccess(null));
        assertFalse(logic.containsDNWithAccess(Collections.emptySet()));

        // Assert cases given allowedDNs == non-empty set with no matching DN. All access should be blocked.
        logic.setAuthorizedDNs(Sets.newHashSet("dn=other user", "dn=other user chain"));
        assertFalse(logic.containsDNWithAccess(dns));
        assertFalse(logic.containsDNWithAccess(null));
        assertFalse(logic.containsDNWithAccess(Collections.emptySet()));
    }

    @Test
    public void testWithLanguages() throws Exception {
        AccumuloClient mockClient = PowerMock.createMock(AccumuloClient.class);
        MarkingFunctions mockMarkingFunctions = PowerMock.createMock(MarkingFunctions.class);

        KeywordQueryLogic logic = new KeywordQueryLogic();
        logic.setMarkingFunctions(mockMarkingFunctions);
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        Authorizations auths = new Authorizations("A");

        Query settings = new QueryImpl();
        settings.setQuery(
                        "event:20241218_0/sampleCsv/1.2.3%LANGUAGE:ENGLISH event:20241218_0/sampleCsv/1.2.4!PAGE_ID:12345%LANGUAGE:UNKNOWN event:20241218_0/sampleCsv/1.2.5");
        settings.setQueryAuthorizations("A");

        logic.initialize(mockClient, settings, Set.of(auths));
        Map<String,String> documentLanguageMap = logic.getConfig().getState().getLanguageMap();
        String threeLanguage = documentLanguageMap.get("20241218_0/sampleCsv/1.2.3");
        String fourLanguage = documentLanguageMap.get("20241218_0/sampleCsv/1.2.4");
        String fiveLanguage = documentLanguageMap.get("20241218_0/sampleCsv/1.2.5");

        assertEquals("ENGLISH", threeLanguage);
        assertEquals("UNKNOWN", fourLanguage);
        assertNull(fiveLanguage);
    }

    @Test
    public void testDecodeQueryAndValue() throws Exception {
        AccumuloClient mockClient = PowerMock.createMock(AccumuloClient.class);
        MarkingFunctions mockMarkingFunctions = PowerMock.createMock(MarkingFunctions.class);

        KeywordQueryLogic logic = new KeywordQueryLogic();
        logic.setMarkingFunctions(mockMarkingFunctions);
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        Authorizations auths = new Authorizations("A");

        Query settings = new QueryImpl();
        settings.setQuery("event:20241218_0/sampleCsv/1.2.3!PAGE_ID:1234%LANGUAGE:ENGLISH");
        settings.setQueryAuthorizations("A");

        LinkedHashMap<String,Double> baseResults = new LinkedHashMap<>();
        baseResults.put("get much", 0.5903);
        baseResults.put("kind", 0.2546);
        baseResults.put("kind word", 0.2052);

        KeywordResults results = new KeywordResults("someSource", "someView", "someLanguage", baseResults);

        Key dataKey = new Key("20241218_0", "d", "sampleCsv" + '\u0000' + "1.2.3" + '\u0000' + "someView", "A");
        Value viewValue = new Value(KeywordResults.serialize(results));
        Map.Entry<Key,Value> entry = new AbstractMap.SimpleImmutableEntry<>(dataKey, viewValue);

        mockMarkingFunctions.translateFromColumnVisibilityForAuths(new ColumnVisibility("A"), auths);
        expectLastCall().andReturn(Map.of("A", "A")).anyTimes();

        replayAll();

        logic.initialize(mockClient, settings, Set.of(auths));
        QueryLogicTransformer<Map.Entry<Key,Value>,KeywordResults> transformer = logic.getTransformer(settings);
        KeywordResults base = transformer.transform(entry);
        String json = base.toJson();
        assertEquals("{\"source\":\"someSource\",\"view\":\"someView\",\"language\":\"someLanguage\",\"keywords\":{\"get much\":0.5903,\"kind\":0.2546,\"kind word\":0.2052}}",
                        json);
        verifyAll();
    }

    private static class TestKeywordQuery extends KeywordQueryLogic {
        // borrowed from TestBaseQueryLogic.java
        public TestKeywordQuery() {
            super();
        }

        public TestKeywordQuery(TestKeywordQuery other) {
            super(other);
        }

        public TestKeywordQuery(BaseQueryLogic<Object> copy) {}

        @SuppressWarnings("rawtypes")
        @Override
        public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set runtimeQueryAuthorizations) {
            return null;
        }

        @Override
        public void setupQuery(GenericQueryConfiguration configuration) {
            // No op
        }

        @Override
        public String getPlan(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields,
                        boolean expandValues) {
            return "";
        }

        @Override
        public AccumuloConnectionFactory.Priority getConnectionPriority() {
            return null;
        }

        @Override
        public QueryLogicTransformer<Map.Entry<Key,Value>,KeywordResults> getTransformer(Query settings) {
            return null;
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return null;
        }

        @Override
        public Set<String> getOptionalQueryParameters() {
            return null;
        }

        @Override
        public Set<String> getRequiredQueryParameters() {
            return null;
        }

        @Override
        public Set<String> getExampleQueries() {
            return null;
        }
    }
}
