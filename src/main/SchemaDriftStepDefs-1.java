
package com.jpmc.pandi.cucumber.stepdefs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;

public class SchemaDriftStepDefs {

    // Case 1: Add a new field (productCheck)
    public static Dataset<Row> addFieldToInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn(
            "inclusionReasonValueText",
            struct(
                col("inclusionReasonValueText.audienceDataFeatureTypeCode"),
                col("inclusionReasonValueText.hashedAccountNumber"),
                col("inclusionReasonValueText.accountReferenceNumber"),
                col("inclusionReasonValueText.accountIdentifier"),
                col("inclusionReasonValueText.productCode"),
                col("inclusionReasonValueText.subProductCode"),
                col("inclusionReasonValueText.marketingProductCode"),
                col("inclusionReasonValueText.audienceDataFeatureStatusCode"),
                col("inclusionReasonValueText.entryTimestamp"),
                col("inclusionReasonValueText.expirationTimestamp"),
                lit("YES").alias("productCheck")
            )
        );
    }

    // Case 2: Remove a field (remove entryTimestamp)
    public static Dataset<Row> removeFieldFromInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn(
            "inclusionReasonValueText",
            struct(
                col("inclusionReasonValueText.audienceDataFeatureTypeCode"),
                col("inclusionReasonValueText.hashedAccountNumber"),
                col("inclusionReasonValueText.accountReferenceNumber"),
                col("inclusionReasonValueText.accountIdentifier"),
                col("inclusionReasonValueText.productCode"),
                col("inclusionReasonValueText.subProductCode"),
                col("inclusionReasonValueText.marketingProductCode"),
                col("inclusionReasonValueText.audienceDataFeatureStatusCode"),
                col("inclusionReasonValueText.expirationTimestamp")
            )
        );
    }

    // Case 3: Rename a field (productCode -> productCodeRenamed)
    public static Dataset<Row> renameFieldInInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn(
            "inclusionReasonValueText",
            struct(
                col("inclusionReasonValueText.audienceDataFeatureTypeCode"),
                col("inclusionReasonValueText.hashedAccountNumber"),
                col("inclusionReasonValueText.accountReferenceNumber"),
                col("inclusionReasonValueText.accountIdentifier"),
                col("inclusionReasonValueText.productCode").alias("productCodeRenamed"),
                col("inclusionReasonValueText.subProductCode"),
                col("inclusionReasonValueText.marketingProductCode"),
                col("inclusionReasonValueText.audienceDataFeatureStatusCode"),
                col("inclusionReasonValueText.entryTimestamp"),
                col("inclusionReasonValueText.expirationTimestamp")
            )
        );
    }

    // Case 4: Change data type (entryTimestamp -> cast to long)
    public static Dataset<Row> changeFieldTypeInInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn(
            "inclusionReasonValueText",
            struct(
                col("inclusionReasonValueText.audienceDataFeatureTypeCode"),
                col("inclusionReasonValueText.hashedAccountNumber"),
                col("inclusionReasonValueText.accountReferenceNumber"),
                col("inclusionReasonValueText.accountIdentifier"),
                col("inclusionReasonValueText.productCode"),
                col("inclusionReasonValueText.subProductCode"),
                col("inclusionReasonValueText.marketingProductCode"),
                col("inclusionReasonValueText.audienceDataFeatureStatusCode"),
                col("inclusionReasonValueText.entryTimestamp").cast("long").alias("entryTimestamp"),
                col("inclusionReasonValueText.expirationTimestamp")
            )
        );
    }
}
