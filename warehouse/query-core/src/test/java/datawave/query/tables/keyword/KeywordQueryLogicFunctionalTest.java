package datawave.query.tables.keyword;

import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import datawave.configuration.spring.SpringBean;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.microservice.query.QueryImpl;
import datawave.query.ExcerptTest;
import datawave.query.QueryTestTableHelper;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.WiseGuysIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.util.TypedValue;

@RunWith(Arquillian.class)
public class KeywordQueryLogicFunctionalTest {
    protected static AccumuloClient connector = null;

    private static final Logger log = Logger.getLogger(KeywordQueryLogicFunctionalTest.class);
    protected Authorizations auths = new Authorizations("ALL");
    protected Set<Authorizations> authSet = Set.of(auths);

    @Inject
    @SpringBean(name = "KeywordQuery")
    protected KeywordQueryLogic logic;

    private final Map<String,String> extraParameters = new HashMap<>();
    private final Map<String,String> expectedResults = new HashMap<>();

    @Deployment
    public static JavaArchive createDeployment() throws Exception {

        return ShrinkWrap.create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class).deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .addAsManifestResource(new StringAsset(
                                        "<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" + "</alternatives>"),
                                        "beans.xml");
    }

    @BeforeClass
    public static void setUp() throws Exception {
        QueryTestTableHelper qtth = new QueryTestTableHelper(ExcerptTest.DocumentRangeTest.class.toString(), log);
        connector = qtth.client;

        Logger.getLogger(PrintUtility.class).setLevel(Level.DEBUG);

        WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.DOCUMENT);
        Authorizations auths = new Authorizations("ALL");
        PrintUtility.printTable(connector, auths, TableName.SHARD);
        PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }

    @Before
    public void setup() throws ParseException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        log.setLevel(Level.TRACE);
    }

    @Test
    public void simpleTest() throws Exception {
        String queryString = "DOCUMENT:20130101_0/test/-cvy0gj.tlf59s.-duxzua";

        addExpectedResult("20130101_0:test:-cvy0gj.tlf59s.-duxzua:CONTENT2_KEYWORDS",
                        "{get much=0.5903, kind=0.2546, kind word=0.2052, kind word alone=0.4375, much farther=0.5903, word=0.2857, word alone=0.534}");

        runTestQuery(queryString);
    }

    @SuppressWarnings("SameParameterValue")
    protected void addExpectedResult(String result, String keywords) {
        if (StringUtils.isNotBlank(result)) {
            expectedResults.put(result, keywords);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected void runTestQuery(String queryString) throws Exception {
        QueryImpl settings = new QueryImpl();
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(queryString);
        settings.setParameters(extraParameters);
        settings.setId(UUID.randomUUID());

        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());

        GenericQueryConfiguration config = logic.initialize(connector, settings, authSet);
        logic.setupQuery(config);

        QueryLogicTransformer<Map.Entry<Key,Value>,EventBase> transformer = logic.getTransformer(config.getQuery());
        Map<String,String> unexpectedFields = new HashMap<>();

        for (Map.Entry<Key,Value> entry : logic) {
            EventBase event = transformer.transform(entry);
            List<FieldBase> fields = event.getFields();
            Metadata md = event.getMetadata();

            for (FieldBase field : fields) {
                String name = field.getName();
                String toFind = md.getRow() + ":" + md.getDataType() + ":" + md.getInternalId() + ":" + name;
                TypedValue tv = field.getTypedValue();

                String content = extractContent(tv);
                String found = expectedResults.get(toFind);
                if (found != null && found.equals(content)) {
                    expectedResults.remove(toFind);
                } else {
                    unexpectedFields.put(toFind, content);
                }
            }
        }

        assertTrue("unexpected fields returned: " + unexpectedFields, unexpectedFields.isEmpty());
        assertTrue(expectedResults + " was not empty", expectedResults.isEmpty());
    }

    public static String extractContent(TypedValue tv) {
        if (tv.getType().equals(TypedValue.XSD_BASE64BINARY)) {
            return new String((byte[]) tv.getValue());
        } else if (tv.getType().equals(TypedValue.XSD_STRING)) {
            return tv.getValue().toString();
        } else {
            log.info("Encountered unexpected type in test " + tv.getType());
            return "UNEXPECTED_TYPE";
        }
    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

}
