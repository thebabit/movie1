
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.*;

public class TaggingProcessorJoinDriftAllCasesDemo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .master("local[*]")
            .appName("TaggingProcessorJoinDriftAllCasesDemo")
            .getOrCreate();

        // Previous snapshot: full schema
        StructType fullSchema = new StructType()
            .add("enterprisePartyIdentifier", "string")
            .add("audiencePopulationTypeCode", "string")
            .add("inclusionReasonValueText", new StructType()
                .add("productCode", "string")
                .add("entryTimestamp", "string")
            );

        Row oldRow = RowFactory.create("001", "A", RowFactory.create("P1", "2024-01-01"));
        Dataset<Row> previous = spark.createDataFrame(List.of(oldRow), fullSchema);

        // Case 1: Removed field (entryTimestamp is missing)
        StructType removedSchema = new StructType()
            .add("enterprisePartyIdentifier", "string")
            .add("audiencePopulationTypeCode", "string")
            .add("inclusionReasonValueText", new StructType()
                .add("productCode", "string")
            );
        Row rowRemoved = RowFactory.create("001", "A", RowFactory.create("P1"));
        Dataset<Row> currentRemoved = spark.createDataFrame(List.of(rowRemoved), removedSchema);

        // Case 2: Renamed field (entryTimestamp -> timestampRenamed)
        StructType renamedSchema = new StructType()
            .add("enterprisePartyIdentifier", "string")
            .add("audiencePopulationTypeCode", "string")
            .add("inclusionReasonValueText", new StructType()
                .add("productCode", "string")
                .add("timestampRenamed", "string")
            );
        Row rowRenamed = RowFactory.create("001", "A", RowFactory.create("P1", "2024-01-01"));
        Dataset<Row> currentRenamed = spark.createDataFrame(List.of(rowRenamed), renamedSchema);

        // Case 3: Added field (entryTimestamp exists + extra productCheck)
        StructType addedSchema = new StructType()
            .add("enterprisePartyIdentifier", "string")
            .add("audiencePopulationTypeCode", "string")
            .add("inclusionReasonValueText", new StructType()
                .add("productCode", "string")
                .add("entryTimestamp", "string")
                .add("productCheck", "string")
            );
        Row rowAdded = RowFactory.create("001", "A", RowFactory.create("P1", "2024-01-01", "YES"));
        Dataset<Row> currentAdded = spark.createDataFrame(List.of(rowAdded), addedSchema);

        System.out.println("=== CASE 1: Field Removed ===");
        runJoinAndCompare(currentRemoved, previous, spark);

        System.out.println("=== CASE 2: Field Renamed ===");
        runJoinAndCompare(currentRenamed, previous, spark);

        System.out.println("=== CASE 3: Field Added ===");
        runJoinAndCompare(currentAdded, previous, spark);

        spark.stop();
    }

    static void runJoinAndCompare(Dataset<Row> current, Dataset<Row> previous, SparkSession spark) {
        Dataset<Row> joined = current.as("new")
            .join(
                previous.as("prev"),
                functions.col("new.enterprisePartyIdentifier").equalTo(functions.col("prev.enterprisePartyIdentifier"))
                    .and(functions.col("new.audiencePopulationTypeCode").equalTo(functions.col("prev.audiencePopulationTypeCode"))),
                "full_outer"
            );

        joined.printSchema();
        joined.show(false);

        Dataset<String> result = joined.flatMap((FlatMapFunction<Row, String>) row -> {
            Row newReason = row.getAs("new.inclusionReasonValueText");
            Row oldReason = row.getAs("prev.inclusionReasonValueText");

            String newCode = newReason != null ? getSafe(newReason, "productCode") : null;
            String oldCode = oldReason != null ? getSafe(oldReason, "productCode") : null;

            String newTimestamp = newReason != null ? getSafe(newReason, "entryTimestamp") : null;
            String oldTimestamp = oldReason != null ? getSafe(oldReason, "entryTimestamp") : null;

            boolean equal = Objects.equals(newCode, oldCode) && Objects.equals(newTimestamp, oldTimestamp);
            return List.of(equal ? "NO_CHANGE" : "UPDATED").iterator();
        }, Encoders.STRING());

        result.show();
    }

    static String getSafe(Row row, String fieldName) {
        try {
            if (row.schema().getFieldIndex(fieldName).isDefined()) {
                return row.getAs(fieldName);
            }
        } catch (Exception ignored) {}
        return null;
    }
}
