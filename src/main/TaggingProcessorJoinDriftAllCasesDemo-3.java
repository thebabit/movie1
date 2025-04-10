
package com.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.*;

import static org.apache.spark.sql.functions.col;

public class TaggingProcessorJoinDriftAllCasesDemo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SchemaDriftJoinDemo")
                .master("local[*]")
                .getOrCreate();

        StructType oldSchema = new StructType()
                .add("enterprisePartyIdentifier", "string")
                .add("audiencePopulationTypeCode", "string")
                .add("inclusionReasonValueText", new StructType()
                        .add("productCode", "string")
                        .add("entryTimestamp", "string"));

        StructType newSchemaAddedField = new StructType()
                .add("enterprisePartyIdentifier", "string")
                .add("audiencePopulationTypeCode", "string")
                .add("inclusionReasonValueText", new StructType()
                        .add("productCode", "string")
                        .add("entryTimestamp", "string")
                        .add("extraField", "string"));

        StructType newSchemaRemovedField = new StructType()
                .add("enterprisePartyIdentifier", "string")
                .add("audiencePopulationTypeCode", "string")
                .add("inclusionReasonValueText", new StructType()
                        .add("productCode", "string"));

        StructType newSchemaRenamedField = new StructType()
                .add("enterprisePartyIdentifier", "string")
                .add("audiencePopulationTypeCode", "string")
                .add("inclusionReasonValueText", new StructType()
                        .add("productCodeRenamed", "string")
                        .add("entryTimestamp", "string"));

        Row prevRow = RowFactory.create("001", "A",
                RowFactory.create("P1", "2024-01-01"));

        Row rowAdded = RowFactory.create("001", "A",
                RowFactory.create("P1", "2024-01-01", "extra"));

        Row rowRemoved = RowFactory.create("001", "A",
                RowFactory.create("P1"));

        Row rowRenamed = RowFactory.create("001", "A",
                RowFactory.create("P1", "2024-01-01"));

        Dataset<Row> previous = spark.createDataFrame(Collections.singletonList(prevRow), oldSchema);
        Dataset<Row> added = spark.createDataFrame(Collections.singletonList(rowAdded), newSchemaAddedField);
        Dataset<Row> removed = spark.createDataFrame(Collections.singletonList(rowRemoved), newSchemaRemovedField);
        Dataset<Row> renamed = spark.createDataFrame(Collections.singletonList(rowRenamed), newSchemaRenamedField);

        System.out.println("=== Join with Added Field ===");
        tryJoin(previous, added).show(false);

        System.out.println("=== Join with Removed Field ===");
        tryJoin(previous, removed).show(false);

        System.out.println("=== Join with Renamed Field ===");
        tryJoin(previous, renamed).show(false);

        spark.stop();
    }

    private static Dataset<Row> tryJoin(Dataset<Row> prev, Dataset<Row> current) {
        return prev.join(current,
                prev.col("enterprisePartyIdentifier").equalTo(current.col("enterprisePartyIdentifier"))
                        .and(prev.col("audiencePopulationTypeCode").equalTo(current.col("audiencePopulationTypeCode"))),
                "full_outer");
    }
}
