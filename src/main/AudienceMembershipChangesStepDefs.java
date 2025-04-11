
package com.jpmc.pandi.cucumber.stepdefs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class AudienceMembershipChangesStepDefs {

    // Case 1: Add a new field to inclusionReasonValueText
    public static Dataset<Row> addFieldToInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn(
                "audienceDataFeatures",
                expr("transform(audienceDataFeatures, x -> struct(" +
                        "x.inclusionReasonTypeCode as inclusionReasonTypeCode, " +
                        "named_struct(" +
                        "  'accountIdentifier', x.inclusionReasonValueText.accountIdentifier, " +
                        "  'accountReferenceNumber', x.inclusionReasonValueText.accountReferenceNumber, " +
                        "  'audienceDataFeatureStatusCode', x.inclusionReasonValueText.audienceDataFeatureStatusCode, " +
                        "  'audienceDataFeatureTypeCode', x.inclusionReasonValueText.audienceDataFeatureTypeCode, " +
                        "  'entryTimestamp', x.inclusionReasonValueText.entryTimestamp, " +
                        "  'expirationTimestamp', x.inclusionReasonValueText.expirationTimestamp, " +
                        "  'hashedAccountNumber', x.inclusionReasonValueText.hashedAccountNumber, " +
                        "  'marketingProductCode', x.inclusionReasonValueText.marketingProductCode, " +
                        "  'productCode', x.inclusionReasonValueText.productCode, " +
                        "  'subProductCode', x.inclusionReasonValueText.subProductCode, " +
                        "  'productCheck', 'YES'" +
                        ") as inclusionReasonValueText" +
                        "))")
        );
    }

    // Case 2: Remove a field (e.g., expirationTimestamp)
    public static Dataset<Row> removeFieldFromInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn(
                "audienceDataFeatures",
                expr("transform(audienceDataFeatures, x -> struct(" +
                        "x.inclusionReasonTypeCode as inclusionReasonTypeCode, " +
                        "named_struct(" +
                        "  'accountIdentifier', x.inclusionReasonValueText.accountIdentifier, " +
                        "  'accountReferenceNumber', x.inclusionReasonValueText.accountReferenceNumber, " +
                        "  'audienceDataFeatureStatusCode', x.inclusionReasonValueText.audienceDataFeatureStatusCode, " +
                        "  'audienceDataFeatureTypeCode', x.inclusionReasonValueText.audienceDataFeatureTypeCode, " +
                        "  'entryTimestamp', x.inclusionReasonValueText.entryTimestamp, " +
                        "  'hashedAccountNumber', x.inclusionReasonValueText.hashedAccountNumber, " +
                        "  'marketingProductCode', x.inclusionReasonValueText.marketingProductCode, " +
                        "  'productCode', x.inclusionReasonValueText.productCode, " +
                        "  'subProductCode', x.inclusionReasonValueText.subProductCode" +
                        ") as inclusionReasonValueText" +
                        "))")
        );
    }

    // Case 3: Rename a field (e.g., productCode to productCodeRenamed)
    public static Dataset<Row> renameFieldInInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn(
                "audienceDataFeatures",
                expr("transform(audienceDataFeatures, x -> struct(" +
                        "x.inclusionReasonTypeCode as inclusionReasonTypeCode, " +
                        "named_struct(" +
                        "  'accountIdentifier', x.inclusionReasonValueText.accountIdentifier, " +
                        "  'accountReferenceNumber', x.inclusionReasonValueText.accountReferenceNumber, " +
                        "  'audienceDataFeatureStatusCode', x.inclusionReasonValueText.audienceDataFeatureStatusCode, " +
                        "  'audienceDataFeatureTypeCode', x.inclusionReasonValueText.audienceDataFeatureTypeCode, " +
                        "  'entryTimestamp', x.inclusionReasonValueText.entryTimestamp, " +
                        "  'expirationTimestamp', x.inclusionReasonValueText.expirationTimestamp, " +
                        "  'hashedAccountNumber', x.inclusionReasonValueText.hashedAccountNumber, " +
                        "  'marketingProductCode', x.inclusionReasonValueText.marketingProductCode, " +
                        "  'productCodeRenamed', x.inclusionReasonValueText.productCode, " +
                        "  'subProductCode', x.inclusionReasonValueText.subProductCode" +
                        ") as inclusionReasonValueText" +
                        "))")
        );
    }

    // Case 4: Change data type (e.g., entryTimestamp from string to long)
    public static Dataset<Row> changeFieldTypeInInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn(
                "audienceDataFeatures",
                expr("transform(audienceDataFeatures, x -> struct(" +
                        "x.inclusionReasonTypeCode as inclusionReasonTypeCode, " +
                        "named_struct(" +
                        "  'accountIdentifier', x.inclusionReasonValueText.accountIdentifier, " +
                        "  'accountReferenceNumber', x.inclusionReasonValueText.accountReferenceNumber, " +
                        "  'audienceDataFeatureStatusCode', x.inclusionReasonValueText.audienceDataFeatureStatusCode, " +
                        "  'audienceDataFeatureTypeCode', x.inclusionReasonValueText.audienceDataFeatureTypeCode, " +
                        "  'entryTimestamp', cast(x.inclusionReasonValueText.entryTimestamp as long), " +
                        "  'expirationTimestamp', x.inclusionReasonValueText.expirationTimestamp, " +
                        "  'hashedAccountNumber', x.inclusionReasonValueText.hashedAccountNumber, " +
                        "  'marketingProductCode', x.inclusionReasonValueText.marketingProductCode, " +
                        "  'productCode', x.inclusionReasonValueText.productCode, " +
                        "  'subProductCode', x.inclusionReasonValueText.subProductCode" +
                        ") as inclusionReasonValueText" +
                        "))")
        );
    }
}
