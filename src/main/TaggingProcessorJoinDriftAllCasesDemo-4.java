
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.util.*;

public class TaggingProcessorJoinDriftAllCasesDemo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Join Schema Drift Demo").master("local[*]").getOrCreate();

        StructType oldSchema = new StructType()
            .add("enterprisePartyIdentifier", "string")
            .add("audiencePopulationTypeCode", "string")
            .add("inclusionReasonValueText", new StructType()
                .add("productCode", "string")
                .add("entryTimestamp", "string")
            );

        StructType newSchema = new StructType()
            .add("enterprisePartyIdentifier", "string")
            .add("audiencePopulationTypeCode", "string")
            .add("inclusionReasonValueText", new StructType()
                .add("productCode2", "string") // Renamed field
                .add("campaignCheck", "string") // Added field
                // Removed "entryTimestamp"
            );

        Row oldReason = RowFactory.create("P1", "2024-01-01");
        Row oldRow = RowFactory.create("001", "A", oldReason);

        Row newReason = RowFactory.create("P1", "CHECKED");
        Row newRow = RowFactory.create("001", "A", newReason);

        Dataset<Row> previous = spark.createDataFrame(List.of(oldRow), oldSchema);
        Dataset<Row> current = spark.createDataFrame(List.of(newRow), newSchema);

        runJoinAndCompare(current, previous, spark);

        spark.stop();
    }

    private static void runJoinAndCompare(Dataset<Row> current, Dataset<Row> previous, SparkSession spark) {
        Dataset<Row> joined = current.join(previous,
            current.col("enterprisePartyIdentifier").equalTo(previous.col("enterprisePartyIdentifier"))
            .and(current.col("audiencePopulationTypeCode").equalTo(previous.col("audiencePopulationTypeCode"))),
            "full_outer"
        );

        Dataset<String> result = joined.flatMap((FlatMapFunction<Row, String>) row -> {
            Row newReason = row.getAs("inclusionReasonValueText");
            Row oldReason = row.getAs("inclusionReasonValueText");

            String newCode = newReason != null ? getSafe(newReason, "productCode2") : null;
            String oldCode = oldReason != null ? getSafe(oldReason, "productCode") : null;

            String newTimestamp = newReason != null ? getSafe(newReason, "entryTimestamp") : null;
            String oldTimestamp = oldReason != null ? getSafe(oldReason, "entryTimestamp") : null;

            boolean equal = Objects.equals(newCode, oldCode) && Objects.equals(newTimestamp, oldTimestamp);
            return List.of(equal ? "NO_CHANGE" : "UPDATED").iterator();
        }, Encoders.STRING());

        result.show(false);
    }

    private static String getSafe(Row row, String fieldName) {
        try {
            return row.getAs(fieldName);
        } catch (Exception e) {
            return null;
        }
    }
}
