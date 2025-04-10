
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.util.ArrayList;
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
    void shouldIgnoreNewFieldInInclusionReasonValueText() {
        StructType schema = new StructType()
            .add("enterprisePartyIdentifier", "string")
            .add("audiencePopulationTypeCode", "string")
            .add("audienceDataFeatures", DataTypes.createArrayType(
                new StructType()
                    .add("inclusionReasonTypeCode", "string")
                    .add("inclusionReasonValueText", new StructType()
                        .add("productCode", "string")
                        .add("entryTimestamp", "string")
                        .add("productCheck", "string") // This field is NOT in the Java class
                    )
            ));

        Row valueText = RowFactory.create("P1", "2024-01-01", "EXTRA"); // "EXTRA" is ignored
        Row feature = RowFactory.create("PRODUCT", valueText);
        Row row = RowFactory.create("002", "A", List.of(feature));
        Dataset<Row> untaggedRowDataset = spark.createDataFrame(List.of(row), schema);

        Dataset<Population> untaggedDataset = untaggedRowDataset.mapPartitions(
            (MapPartitionsFunction<Row, Population>) iterator -> {
                List<Population> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    Row r = iterator.next();
                    Population p = new Population();
                    p.setEnterprisePartyIdentifier(r.getAs("enterprisePartyIdentifier"));
                    p.setAudiencePopulationTypeCode(r.getAs("audiencePopulationTypeCode"));

                    List<Row> features = r.getList(r.fieldIndex("audienceDataFeatures"));
                    List<PopulationDf> featureObjs = new ArrayList<>();
                    for (Row f : features) {
                        String typeCode = f.getAs("inclusionReasonTypeCode");
                        Row reason = f.getStruct(f.fieldIndex("inclusionReasonValueText"));

                        InclusionReasonValueText text = InclusionReasonValueText.builder()
                            .productCode(reason.getAs("productCode"))
                            .entryTimestamp(reason.getAs("entryTimestamp"))
                            .build(); // productCheck is ignored since not in Java class

                        featureObjs.add(new PopulationDf(typeCode, text));
                    }

                    p.setAudienceDataFeatures(featureObjs);
                    list.add(p);
                }
                return list.iterator();
            },
            Encoders.bean(Population.class)
        );

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
        assertEquals(List.of("NO_CHANGE"), tags); // added field not used in logic
    }
}
