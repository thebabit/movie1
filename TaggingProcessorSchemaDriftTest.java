
import org.apache.spark.sql.*;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class TaggingProcessorSchemaDriftTest {

    private SparkSession spark;

    @Mock
    private DeltaDataReader deltaDataReader;

    @InjectMocks
    private TaggingProcessor taggingProcessor;

    private AppConfig appConfig;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        spark = SparkSession.builder().master("local[*]").appName("SchemaDriftTest").getOrCreate();
        appConfig = new AppConfig(); // can be a real or mock instance
    }

    @AfterEach
    void tearDown() {
        spark.stop();
    }

    @Test
    void processTagging_shouldIgnoreNewFieldIfConfigured() {
        InclusionReasonValueText newReason = InclusionReasonValueText.builder()
                .productCode("P1")
                .entryTimestamp("2024-01-01")
                .marketingProductCode("NEW_MKT")
                .build();

        Population newPop = Population.builder()
                .enterprisePartyIdentifier("001")
                .audiencePopulationTypeCode("A")
                .audienceDataFeatures(List.of(new PopulationDf("PRODUCT", newReason)))
                .build();

        InclusionReasonValueText oldReason = InclusionReasonValueText.builder()
                .productCode("P1")
                .entryTimestamp("2024-01-01")
                .build();

        Population oldPop = Population.builder()
                .enterprisePartyIdentifier("001")
                .audiencePopulationTypeCode("A")
                .audienceDataFeatures(List.of(new PopulationDf("PRODUCT", oldReason)))
                .build();

        Dataset<Row> untaggedDataset = spark.createDataset(List.of(newPop), Encoders.bean(Population.class)).toDF();

        AudienceOutputTagged taggedOld = new AudienceOutputTagged();
        taggedOld.setEnterprisePartyIdentifier("001");
        taggedOld.setAudiencePopulationTypeCode("A");
        taggedOld.setPopulation(oldPop);
        taggedOld.setResultTag("NO_CHANGE");

        Dataset<AudienceOutputTagged> previousTagged = spark.createDataset(List.of(taggedOld), Encoders.bean(AudienceOutputTagged.class));
        when(deltaDataReader.readLatestPreviousTaggedOutput(appConfig)).thenReturn(previousTagged);

        Dataset<AudienceOutputTagged> result = taggingProcessor.processTagging(untaggedDataset, appConfig);
        List<String> resultTags = result.select("resultTag").as(Encoders.STRING()).collectAsList();

        assertEquals(List.of("NO_CHANGE"), resultTags);
    }

    @Test
    void processTagging_shouldDetectMissingFieldAsUpdate() {
        InclusionReasonValueText oldReason = InclusionReasonValueText.builder()
                .productCode("P1")
                .marketingProductCode("OLD_MKT")
                .build();

        Population oldPop = Population.builder()
                .enterprisePartyIdentifier("002")
                .audiencePopulationTypeCode("A")
                .audienceDataFeatures(List.of(new PopulationDf("PRODUCT", oldReason)))
                .build();

        InclusionReasonValueText newReason = InclusionReasonValueText.builder()
                .productCode("P1")
                .build();

        Population newPop = Population.builder()
                .enterprisePartyIdentifier("002")
                .audiencePopulationTypeCode("A")
                .audienceDataFeatures(List.of(new PopulationDf("PRODUCT", newReason)))
                .build();

        Dataset<Row> untaggedDataset = spark.createDataset(List.of(newPop), Encoders.bean(Population.class)).toDF();

        AudienceOutputTagged taggedOld = new AudienceOutputTagged();
        taggedOld.setEnterprisePartyIdentifier("002");
        taggedOld.setAudiencePopulationTypeCode("A");
        taggedOld.setPopulation(oldPop);
        taggedOld.setResultTag("NO_CHANGE");

        Dataset<AudienceOutputTagged> previousTagged = spark.createDataset(List.of(taggedOld), Encoders.bean(AudienceOutputTagged.class));
        when(deltaDataReader.readLatestPreviousTaggedOutput(appConfig)).thenReturn(previousTagged);

        Dataset<AudienceOutputTagged> result = taggingProcessor.processTagging(untaggedDataset, appConfig);
        List<String> resultTags = result.select("resultTag").as(Encoders.STRING()).collectAsList();

        assertEquals(List.of("UPDATED"), resultTags);
    }

    @Test
    void processTagging_shouldHandleFieldRenameByTreatingAsDifference() {
        Row row = RowFactory.create("003", "A", "P1", "2024-01-01", "RENAMED_FIELD_VALUE");
        StructType schema = new StructType()
                .add("enterprisePartyIdentifier", "string")
                .add("audiencePopulationTypeCode", "string")
                .add("productCode", "string")
                .add("entryTimestamp", "string")
                .add("entryTimeRenamed", "string");

        Dataset<Row> untaggedDataset = spark.createDataFrame(List.of(row), schema);

        InclusionReasonValueText oldReason = InclusionReasonValueText.builder()
                .productCode("P1")
                .entryTimestamp("2024-01-01")
                .build();

        Population oldPop = Population.builder()
                .enterprisePartyIdentifier("003")
                .audiencePopulationTypeCode("A")
                .audienceDataFeatures(List.of(new PopulationDf("PRODUCT", oldReason)))
                .build();

        AudienceOutputTagged taggedOld = new AudienceOutputTagged();
        taggedOld.setEnterprisePartyIdentifier("003");
        taggedOld.setAudiencePopulationTypeCode("A");
        taggedOld.setPopulation(oldPop);
        taggedOld.setResultTag("NO_CHANGE");

        Dataset<AudienceOutputTagged> previousTagged = spark.createDataset(List.of(taggedOld), Encoders.bean(AudienceOutputTagged.class));
        when(deltaDataReader.readLatestPreviousTaggedOutput(appConfig)).thenReturn(previousTagged);

        Dataset<AudienceOutputTagged> result = taggingProcessor.processTagging(untaggedDataset, appConfig);
        List<String> resultTags = result.select("resultTag").as(Encoders.STRING()).collectAsList();

        assertEquals(List.of("UPDATED"), resultTags);
    }
}
