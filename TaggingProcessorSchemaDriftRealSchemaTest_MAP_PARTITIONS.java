
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class TaggingProcessorSchemaDriftRealSchemaTest {

    private SparkSession spark;
    private DeltaDataReader deltaDataReader;
    private TaggingProcessor taggingProcessor;
    private AppConfig appConfig;

    @BeforeEach
    void setup() {
        spark = SparkSession.builder().master("local[*]").appName("SchemaDriftRealTest").getOrCreate();
        deltaDataReader = mock(DeltaDataReader.class);
        taggingProcessor = new TaggingProcessor(deltaDataReader);
        appConfig = new AppConfig();
    }

    @AfterEach
    void tearDown() {
        spark.stop();
    }

    @Test
    void shouldDetectFieldAddedToSchema() {
        StructType schema = new StructType()
            .add("enterprisePartyIdentifier", "string")
            .add("audiencePopulationTypeCode", "string")
            .add("audienceName", "string")
            .add("audienceMemberStatusCode", "string")
            .add("audienceMemberUuid", "string")
            .add("audienceMemberEntryTimestamp", "string")
            .add("audienceMemberExpirationTimestamp", "string")
            .add("accountIdentifier", "string")
            .add("experimentIdentifier", "string")
            .add("campaignIdentifier", "string")
            .add("campaignChannels", DataTypes.createArrayType(DataTypes.StringType))
            .add("audienceDataFeatures", DataTypes.createArrayType(
                new StructType()
                    .add("inclusionReasonTypeCode", "string")
                    .add("inclusionReasonValueText", new StructType()
                        .add("audienceDataFeatureTypeCode", "string")
                        .add("hashedAccountNumber", "string")
                        .add("accountReferenceNumber", "string")
                        .add("accountReferenceOwner", "string")
                        .add("accountIdentifier", "string")
                        .add("productCode", "string")
                        .add("productCheck", "string")
                        .add("productCode2", "string")
                        .add("subProductCode", "string")
                        .add("marketingProductCode", "string")
                        .add("audienceDataFeatureStatusCode", "string")
                        .add("entryTimestamp", "string")
                        .add("expirationTimestamp", "string")
                        .add("experimentIdentifier", "string")
                        .add("campaignIdentifier", "string")
                        .add("campaignChannels", DataTypes.createArrayType(DataTypes.StringType))
                    )
            ));

        Row valueText = RowFactory.create(null, null, null, null, null, "P1", "CHECKED", null, null, null, null, "2024-01-01", null, null, null, List.of());
        Row feature = RowFactory.create("PRODUCT", valueText);
        Row row = RowFactory.create(
            "002", "A", "audName", "ACTIVE", "uuid123", "2024-01-01", "2025-01-01",
            "accId", "expId", "campId", List.of("email"), List.of(feature)
        );
        Dataset<Row> untaggedRowDataset = spark.createDataFrame(List.of(row), schema);

        // Convert Row -> Population manually via mapPartitions to avoid Spark AnalysisException
        Dataset<Population> untaggedDataset = untaggedRowDataset.mapPartitions(iterator -> {
            List<Population> list = new java.util.ArrayList<>();
            while (iterator.hasNext()) {
                Row r = iterator.next();
                Population p = new Population();
                p.setEnterprisePartyIdentifier(r.getAs("enterprisePartyIdentifier"));
                p.setAudiencePopulationTypeCode(r.getAs("audiencePopulationTypeCode"));
                list.add(p);
            }
            return list.iterator();
        }, Encoders.bean(Population.class));

        Population oldPop = new Population();
        oldPop.setEnterprisePartyIdentifier("002");
        oldPop.setAudiencePopulationTypeCode("A");
        oldPop.setAudienceDataFeatures(List.of(
            new PopulationDf("PRODUCT", InclusionReasonValueText.builder()
                .productCode("P1")
                .entryTimestamp("2024-01-01")
                .build()
            )
        ));

        AudienceOutputTagged taggedOld = new AudienceOutputTagged();
        taggedOld.setEnterprisePartyIdentifier("002");
        taggedOld.setAudiencePopulationTypeCode("A");
        taggedOld.setPopulation(oldPop);
        taggedOld.setResultTag("NO_CHANGE");

        when(deltaDataReader.readLatestPreviousTaggedOutput(appConfig))
            .thenReturn(spark.createDataset(List.of(taggedOld), Encoders.bean(AudienceOutputTagged.class)));

        Dataset<AudienceOutputTagged> result = taggingProcessor.processTagging(untaggedDataset, appConfig);
        List<String> tags = result.select("resultTag").as(Encoders.STRING()).collectAsList();
        assertEquals(List.of("NO_CHANGE"), tags); // added field not used
    }
}
