package datawave.query.planner;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import datawave.data.type.DateType;
import datawave.data.type.LcNoDiacriticsListType;
import datawave.ingest.mapreduce.handler.dateindex.DateIndexUtil;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryParameters;
import datawave.query.attributes.TemporalGranularity;
import datawave.query.attributes.UniqueFields;
import datawave.query.common.grouping.GroupFields;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MockDateIndexHelper;
import datawave.query.util.TypeMetadata;
import datawave.test.JexlNodeAssert;
import datawave.util.time.DateHelper;

class DefaultQueryPlannerTest {

    /**
     * Contains tests for
     * {@link DefaultQueryPlanner#addDateFilters(ASTJexlScript, ScannerFactory, MetadataHelper, DateIndexHelper, ShardQueryConfiguration, Query)}
     */
    @Nested
    class DateFilterTests {

        private final SimpleDateFormat filterFormat = new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSSZ");

        private DefaultQueryPlanner planner;
        private ShardQueryConfiguration config;
        private QueryImpl settings;
        private MockDateIndexHelper dateIndexHelper;
        private ASTJexlScript queryTree;

        @BeforeEach
        void setUp() {
            planner = new DefaultQueryPlanner();
            config = new ShardQueryConfiguration();
            settings = new QueryImpl();
            dateIndexHelper = new MockDateIndexHelper();
        }

        /**
         * Verify that when the date type is the default date type, and is part of the noExpansionIfCurrentDateTypes types, and the query's end date is the
         * current date, that no date filters are added and SHARDS_AND_DAYS hints are forbidden.
         */
        @Test
        void testDefaultDateTypeMarkedForNoExpansionAndEndDateIsCurrDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("EVENT"));

            Date beginDate = DateHelper.parse("20241001");
            config.setBeginDate(beginDate);
            Date endDate = new Date();
            config.setEndDate(endDate);

            ASTJexlScript actual = addDateFilters();

            // no hints or date filter required in this case
            JexlNodeAssert.assertThat(actual).isEqualTo("FOO == 'bar'");
            Assertions.assertEquals(beginDate, config.getBeginDate());
            Assertions.assertEquals(endDate, config.getEndDate());
        }

        /**
         * Verify that when a date type is given via parameters that is part of the noExpansionIfCurrentDateTypes types, and the query's end date is the current
         * date, that no date filters are added and SHARDS_AND_DAYS hints are forbidden.
         */
        @Test
        void testParamDateTypeMarkedForNoExpansionAndEndDateIsCurrDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("SPECIAL_EVENT"));

            Date beginDate = DateHelper.parse("20241001");
            config.setBeginDate(beginDate);
            Date endDate = new Date();
            config.setEndDate(endDate);

            settings.addParameter(QueryParameters.DATE_RANGE_TYPE, "SPECIAL_EVENT");
            dateIndexHelper.addEntry("20241201", "SPECIAL_EVENT", "wiki", "FOO", "20240101_shard");
            dateIndexHelper.addEntry("20250101", "SPECIAL_EVENT", "wiki", "FOO", "20250101_shard");

            ASTJexlScript actual = addDateFilters();

            // no hints but the date filter is still used
            JexlNodeAssert.assertThat(actual).isEqualTo(
                            "(FOO == 'bar') && filter:betweenDates(FOO, '" + filterFormat.format(beginDate) + "', '" + filterFormat.format(endDate) + "')");
            // begin date is not pushed farther back
            Assertions.assertEquals(DateIndexUtil.getBeginDate("20241001"), config.getBeginDate());
            // end date not pushed farther back either
            Assertions.assertEquals(endDate, config.getEndDate());
        }

        /**
         * Verify that when the date type is the default date type, and is part of the noExpansionIfCurrentDateTypes types, but the query's end date is not the
         * current date, that no date filters are added and SHARDS_AND_DAYS hints are allowed.
         */
        @Test
        void testDefaultDateTypeMarkedForNoExpansionAndEndDateIsNotCurrDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("EVENT"));

            Date beginDate = DateHelper.parse("20241001");
            config.setBeginDate(beginDate);
            Date endDate = DateHelper.parse("20241010");
            config.setEndDate(endDate);

            ASTJexlScript actual = addDateFilters();

            // no hints or date filter required in this case
            JexlNodeAssert.assertThat(actual).isEqualTo("FOO == 'bar'");
            Assertions.assertEquals(beginDate, config.getBeginDate());
            Assertions.assertEquals(endDate, config.getEndDate());
        }

        /**
         * Verify that when a date type is given via parameters that is part of the noExpansionIfCurrentDateTypes types, but the query's end date is not the
         * current date, that date filters are added and SHARDS_AND_DAYS hints are allowed.
         */
        @Test
        void testParamDateTypeMarkedForNoExpansionAndEndDateIsNotCurrDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("SPECIAL_EVENT"));
            Date beginDate = DateHelper.parse("20241009");
            config.setBeginDate(beginDate);
            Date endDate = DateHelper.parse("20241011");
            config.setEndDate(endDate);
            settings.addParameter(QueryParameters.DATE_RANGE_TYPE, "SPECIAL_EVENT");
            dateIndexHelper.addEntry("20241010", "SPECIAL_EVENT", "wiki", "FOO", "20241010_shard");

            ASTJexlScript actual = addDateFilters();

            // hints and date filter used in this case
            JexlNodeAssert.assertThat(actual).hasExactQueryString(
                            "(FOO == 'bar') && filter:betweenDates(FOO, '" + filterFormat.format(beginDate) + "', '" + filterFormat.format(endDate) + "')");
            // only the end date is adjusted
            Assertions.assertEquals(beginDate, config.getBeginDate());
            Assertions.assertEquals(DateIndexUtil.getEndDate("20241010"), config.getEndDate());
        }

        /**
         * Verify that when the date type is the default date type, and is not part of the noExpansionIfCurrentDateTypes types, and the query's end date is the
         * current date, that no date filters are added and SHARDS_AND_DAYS hints are allowed.
         */
        @Test
        void testDefaultDateTypeIsNotMarkedForNoExpansionAndEndDateNotCurrDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("OTHER_EVENT"));

            Date beginDate = DateHelper.parse("20241001");
            config.setBeginDate(beginDate);
            Date endDate = DateHelper.parse("20241010");
            config.setEndDate(endDate);

            ASTJexlScript actual = addDateFilters();

            // no hints or date filter required in this case
            JexlNodeAssert.assertThat(actual).isEqualTo("FOO == 'bar'");
            Assertions.assertEquals(beginDate, config.getBeginDate());
            Assertions.assertEquals(endDate, config.getEndDate());
        }

        /**
         * Verify that when a date type is given via parameters that is not part of the noExpansionIfCurrentDateTypes types, and the query's end date is the
         * current date, that date filters are added and SHARDS_AND_DAYS hints are allowed.
         */
        @Test
        void testParamDateTypeIsNotMarkedForNoExpansionAndEndDateIsCurrDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("OTHER_EVENT"));
            config.setBeginDate(DateHelper.parse("20241009"));
            Date beginDate = DateHelper.parse("20241001");
            config.setBeginDate(beginDate);
            Date endDate = new Date();
            config.setEndDate(endDate);

            settings.addParameter(QueryParameters.DATE_RANGE_TYPE, "SPECIAL_EVENT");
            dateIndexHelper.addEntry("20241010", "SPECIAL_EVENT", "wiki", "FOO", "20241010_shard");

            ASTJexlScript actual = addDateFilters();

            // hints and date filter used in this case
            JexlNodeAssert.assertThat(actual).hasExactQueryString(
                            "(FOO == 'bar') && filter:betweenDates(FOO, '" + filterFormat.format(beginDate) + "', '" + filterFormat.format(endDate) + "')");
            Assertions.assertEquals(DateIndexUtil.getBeginDate("20241010"), config.getBeginDate());
            Assertions.assertEquals(DateIndexUtil.getEndDate("20241010"), config.getEndDate());
        }

        private ASTJexlScript addDateFilters() throws TableNotFoundException, DatawaveQueryException {
            return planner.addDateFilters(queryTree, null, null, dateIndexHelper, config, settings);
        }

        /**
         * Verify that no exception is thrown when validating a {@link UniqueFields} instance with temporal granularities for datetime fields.
         */
        @Test
        void testValidateUniqueFieldsGivenValidFields() {
            TypeMetadata metadata = new TypeMetadata();
            metadata.put("NAME", "hr", LcNoDiacriticsListType.class.getName());
            metadata.put("ROLE", "hr", LcNoDiacriticsListType.class.getName());
            metadata.put("HIRE_DATE", "hr", DateType.class.getName());
            planner.setTypeMetadata(metadata);

            UniqueFields uniqueFields = new UniqueFields();
            uniqueFields.put("NAME", TemporalGranularity.ALL);
            uniqueFields.put("ROLE", TemporalGranularity.ALL);
            uniqueFields.put("HIRE_DATE", TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY);

            planner.validateUniqueFields(uniqueFields);
        }

        /**
         * Verify that an exception is thrown when validating a {@link UniqueFields} instance with temporal granularities for non-datetime fields.
         */
        @Test
        void testValidateUniqueFieldsGivenInvalidFields() {
            TypeMetadata metadata = new TypeMetadata();
            metadata.put("NAME", "hr", LcNoDiacriticsListType.class.getName());
            metadata.put("ROLE", "hr", LcNoDiacriticsListType.class.getName());
            metadata.put("HIRE_DATE", "hr", DateType.class.getName());
            planner.setTypeMetadata(metadata);

            UniqueFields uniqueFields = new UniqueFields();
            uniqueFields.put("NAME", TemporalGranularity.ALL);
            uniqueFields.put("ROLE", TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY);
            uniqueFields.put("HIRE_DATE", TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY);

            Assertions.assertThrows(DatawaveFatalQueryException.class, () -> planner.validateUniqueFields(uniqueFields),
                            "The following unique fields are not date fields and cannot be used with UNIQUE_BY_X: ROLE");
        }

        /**
         * Verify that no exception is thrown when validating a {@link GroupFields} instance with temporal granularities for datetime fields.
         */
        @Test
        void testValidateGroupFieldsGivenValidFields() {
            TypeMetadata metadata = new TypeMetadata();
            metadata.put("NAME", "hr", LcNoDiacriticsListType.class.getName());
            metadata.put("ROLE", "hr", LcNoDiacriticsListType.class.getName());
            metadata.put("HIRE_DATE", "hr", DateType.class.getName());
            planner.setTypeMetadata(metadata);

            GroupFields groupFields = GroupFields.from("NAME,ROLE,HIRE_DATE[DAY]");

            planner.validateGroupFields(groupFields);
        }

        /**
         * Verify that an exception is thrown when validating a {@link GroupFields} instance with temporal granularities for non-datetime fields.
         */
        @Test
        void testValidateGroupFieldsGivenInvalidFields() {
            TypeMetadata metadata = new TypeMetadata();
            metadata.put("NAME", "hr", LcNoDiacriticsListType.class.getName());
            metadata.put("ROLE", "hr", LcNoDiacriticsListType.class.getName());
            metadata.put("HIRE_DATE", "hr", DateType.class.getName());
            planner.setTypeMetadata(metadata);

            GroupFields groupFields = GroupFields.from("NAME,ROLE[DAY],HIRE_DATE[DAY]");

            Assertions.assertThrows(DatawaveFatalQueryException.class, () -> planner.validateGroupFields(groupFields),
                            "The following group-by fields are not date fields and cannot be used with temporal truncation: ROLE");
        }
    }
}
