
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
    void shouldDetectFieldRemovedFromSchema() {
        StructType schema = new StructType()
            .add("enterprisePartyIdentifier", "string")
            .add("audiencePopulationTypeCode", "string")
            .add("audienceDataFeatures", DataTypes.createArrayType(
                new StructType()
                    .add("inclusionReasonTypeCode", "string")
                    .add("inclusionReasonValueText", new StructType()
                        .add("productCode", "string") // entryTimestamp is removed here
                    )
            ));

        Row valueText = RowFactory.create("P1");
        Row feature = RowFactory.create("PRODUCT", valueText);
        Row row = RowFactory.create("001", "A", List.of(feature));
        Dataset<Row> untaggedDataset = spark.createDataFrame(List.of(row), schema);

        Population oldPop = new Population();
        oldPop.setEnterprisePartyIdentifier("001");
        oldPop.setAudiencePopulationTypeCode("A");
        oldPop.setAudienceDataFeatures(List.of(
            new PopulationDf("PRODUCT", InclusionReasonValueText.builder()
                .productCode("P1")
                .entryTimestamp("2024-01-01")
                .build()
            )
        ));

        AudienceOutputTagged taggedOld = new AudienceOutputTagged();
        taggedOld.setEnterprisePartyIdentifier("001");
        taggedOld.setAudiencePopulationTypeCode("A");
        taggedOld.setPopulation(oldPop);
        taggedOld.setResultTag("NO_CHANGE");

        when(deltaDataReader.readLatestPreviousTaggedOutput(appConfig))
            .thenReturn(spark.createDataset(List.of(taggedOld), Encoders.bean(AudienceOutputTagged.class)));

        Dataset<AudienceOutputTagged> result = taggingProcessor.processTagging(
            untaggedDataset.as(Encoders.bean(Population.class)), appConfig
        );

        List<String> tags = result.select("resultTag").as(Encoders.STRING()).collectAsList();
        assertEquals(List.of("UPDATED"), tags);
    }

    @Test
    void shouldDetectFieldAddedToSchema() {
        StructType schema = new StructType()
            .add("enterprisePartyIdentifier", "string")
            .add("audiencePopulationTypeCode", "string")
            .add("audienceDataFeatures", DataTypes.createArrayType(
                new StructType()
                    .add("inclusionReasonTypeCode", "string")
                    .add("inclusionReasonValueText", new StructType()
                        .add("productCode", "string")
                        .add("entryTimestamp", "string")
                        .add("productCheck", "string") // new field added
                    )
            ));

        Row valueText = RowFactory.create("P1", "2024-01-01", "CHECKED");
        Row feature = RowFactory.create("PRODUCT", valueText);
        Row row = RowFactory.create("002", "A", List.of(feature));
        Dataset<Row> untaggedDataset = spark.createDataFrame(List.of(row), schema);

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

        Dataset<AudienceOutputTagged> result = taggingProcessor.processTagging(
            untaggedDataset.as(Encoders.bean(Population.class)), appConfig
        );

        List<String> tags = result.select("resultTag").as(Encoders.STRING()).collectAsList();
        assertEquals(List.of("NO_CHANGE"), tags); // because added field is not used in logic
    }

    @Test
    void shouldDetectFieldRenamedInSchema() {
        StructType schema = new StructType()
            .add("enterprisePartyIdentifier", "string")
            .add("audiencePopulationTypeCode", "string")
            .add("audienceDataFeatures", DataTypes.createArrayType(
                new StructType()
                    .add("inclusionReasonTypeCode", "string")
                    .add("inclusionReasonValueText", new StructType()
                        .add("productCode2", "string") // renamed from productCode
                        .add("entryTimestamp", "string")
                    )
            ));

        Row valueText = RowFactory.create("P1", "2024-01-01");
        Row feature = RowFactory.create("PRODUCT", valueText);
        Row row = RowFactory.create("003", "A", List.of(feature));
        Dataset<Row> untaggedDataset = spark.createDataFrame(List.of(row), schema);

        Population oldPop = new Population();
        oldPop.setEnterprisePartyIdentifier("003");
        oldPop.setAudiencePopulationTypeCode("A");
        oldPop.setAudienceDataFeatures(List.of(
            new PopulationDf("PRODUCT", InclusionReasonValueText.builder()
                .productCode("P1")
                .entryTimestamp("2024-01-01")
                .build()
            )
        ));

        AudienceOutputTagged taggedOld = new AudienceOutputTagged();
        taggedOld.setEnterprisePartyIdentifier("003");
        taggedOld.setAudiencePopulationTypeCode("A");
        taggedOld.setPopulation(oldPop);
        taggedOld.setResultTag("NO_CHANGE");

        when(deltaDataReader.readLatestPreviousTaggedOutput(appConfig))
            .thenReturn(spark.createDataset(List.of(taggedOld), Encoders.bean(AudienceOutputTagged.class)));

        Dataset<AudienceOutputTagged> result = taggingProcessor.processTagging(
            untaggedDataset.as(Encoders.bean(Population.class)), appConfig
        );

        List<String> tags = result.select("resultTag").as(Encoders.STRING()).collectAsList();
        assertEquals(List.of("UPDATED"), tags); // field renamed = treated as change
    }
}
