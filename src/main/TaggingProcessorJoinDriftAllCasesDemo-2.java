
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.javalang.typed;
import org.apache.spark.sql.types.StructType;
import java.util.*;

public class TaggingProcessorJoinDriftAllCasesDemo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SchemaDriftJoinDemo")
                .master("local[*]")
                .getOrCreate();

        // Simulating old and new schema datasets
        List<Row> previousData = Arrays.asList(
                RowFactory.create("001", "A", RowFactory.create("P1")),
                RowFactory.create("002", "A", RowFactory.create("P2"))
        );
        List<Row> currentData = Arrays.asList(
                RowFactory.create("001", "A", RowFactory.create("P1"), "2024-01-01"), // same
                RowFactory.create("002", "A", RowFactory.create("P3"), "2024-02-01")  // changed
        );

        StructType previousSchema = new StructType()
                .add("enterprisePartyIdentifier", "string")
                .add("audiencePopulationTypeCode", "string")
                .add("inclusionReasonValueText", new StructType().add("productCode", "string"));

        StructType currentSchema = new StructType()
                .add("enterprisePartyIdentifier", "string")
                .add("audiencePopulationTypeCode", "string")
                .add("inclusionReasonValueText", new StructType().add("productCode", "string"))
                .add("entryTimestamp", "string");

        Dataset<Row> prev = spark.createDataFrame(previousData, previousSchema);
        Dataset<Row> curr = spark.createDataFrame(currentData, currentSchema);

        runJoinAndCompare(curr, prev, spark);
        spark.stop();
    }

    static void runJoinAndCompare(Dataset<Row> current, Dataset<Row> previous, SparkSession spark) {
        Dataset<Row> joined = current.join(previous,
                current.col("enterprisePartyIdentifier").equalTo(previous.col("enterprisePartyIdentifier"))
                        .and(current.col("audiencePopulationTypeCode").equalTo(previous.col("audiencePopulationTypeCode"))),
                "full_outer");

        joined.printSchema();
        joined.show(false);

        Dataset<String> result = joined.flatMap((FlatMapFunction<Row, String>) row -> {
            Row newReason = row.getAs("inclusionReasonValueText");
            Row oldReason = row.getAs("inclusionReasonValueText");

            String newCode = newReason != null ? newReason.getAs("productCode") : null;
            String oldCode = oldReason != null ? oldReason.getAs("productCode") : null;

            String newTimestamp = row.getAs("entryTimestamp");
            String oldTimestamp = row.getAs("entryTimestamp");

            boolean equal = Objects.equals(newCode, oldCode) && Objects.equals(newTimestamp, oldTimestamp);
            return Collections.singletonList(equal ? "NO_CHANGE" : "UPDATED").iterator();
        }, Encoders.STRING());

        result.show();
    }
}
