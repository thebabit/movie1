
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
        appConfig = new AppConfig();
    }

    @AfterEach
    void tearDown() {
        spark.stop();
    }

    @Test
    void processTagging_shouldIgnoreNewSchemaFieldIfNotUsedInComparison() {
        InclusionReasonValueText newReason = InclusionReasonValueText.builder()
                .productCode("P1")
                .entryTimestamp("2024-01-01")
                .build(); // Skip marketingChannelCode

        Population newPop = Population.builder()
                .enterprisePartyIdentifier("004")
                .audiencePopulationTypeCode("A")
                .audienceDataFeatures(List.of(new PopulationDf("PRODUCT", newReason)))
                .build();

        InclusionReasonValueText oldReason = InclusionReasonValueText.builder()
                .productCode("P1")
                .entryTimestamp("2024-01-01")
                .build();

        Population oldPop = Population.builder()
                .enterprisePartyIdentifier("004")
                .audiencePopulationTypeCode("A")
                .audienceDataFeatures(List.of(new PopulationDf("PRODUCT", oldReason)))
                .build();

        Dataset<Population> untaggedDataset = spark.createDataset(List.of(newPop), Encoders.bean(Population.class));

        AudienceOutputTagged taggedOld = new AudienceOutputTagged();
        taggedOld.setEnterprisePartyIdentifier("004");
        taggedOld.setAudiencePopulationTypeCode("A");
        taggedOld.setPopulation(oldPop);
        taggedOld.setResultTag("NO_CHANGE");

        Dataset<AudienceOutputTagged> previousTagged = spark.createDataset(List.of(taggedOld), Encoders.bean(AudienceOutputTagged.class));
        when(deltaDataReader.readLatestPreviousTaggedOutput(appConfig)).thenReturn(previousTagged);

        Dataset<AudienceOutputTagged> result = taggingProcessor.processTagging(untaggedDataset, appConfig);
        List<String> tags = result.select("resultTag").as(Encoders.STRING()).collectAsList();

        assertEquals(List.of("NO_CHANGE"), tags);
    }

    @Test
    void processTagging_shouldDetectMissingFieldInNewSchema() {
        InclusionReasonValueText newReason = InclusionReasonValueText.builder()
                .entryTimestamp("2024-01-01")
                .build(); // Skip productCode

        Population newPop = Population.builder()
                .enterprisePartyIdentifier("005")
                .audiencePopulationTypeCode("A")
                .audienceDataFeatures(List.of(new PopulationDf("PRODUCT", newReason)))
                .build();

        InclusionReasonValueText oldReason = InclusionReasonValueText.builder()
                .productCode("P2")
                .entryTimestamp("2024-01-01")
                .build();

        Population oldPop = Population.builder()
                .enterprisePartyIdentifier("005")
                .audiencePopulationTypeCode("A")
                .audienceDataFeatures(List.of(new PopulationDf("PRODUCT", oldReason)))
                .build();

        Dataset<Population> untaggedDataset = spark.createDataset(List.of(newPop), Encoders.bean(Population.class));

        AudienceOutputTagged taggedOld = new AudienceOutputTagged();
        taggedOld.setEnterprisePartyIdentifier("005");
        taggedOld.setAudiencePopulationTypeCode("A");
        taggedOld.setPopulation(oldPop);
        taggedOld.setResultTag("NO_CHANGE");

        Dataset<AudienceOutputTagged> previousTagged = spark.createDataset(List.of(taggedOld), Encoders.bean(AudienceOutputTagged.class));
        when(deltaDataReader.readLatestPreviousTaggedOutput(appConfig)).thenReturn(previousTagged);

        Dataset<AudienceOutputTagged> result = taggingProcessor.processTagging(untaggedDataset, appConfig);
        List<String> tags = result.select("resultTag").as(Encoders.STRING()).collectAsList();

        assertEquals(List.of("UPDATED"), tags);
    }
}
