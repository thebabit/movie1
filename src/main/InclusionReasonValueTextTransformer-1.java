
package net.jpmc.schema;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.expr;

public class InclusionReasonValueTextTransformer {

    public static Dataset<Row> addFieldToInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn(
            "audienceDataFeatures",
            expr(
                "transform(audienceDataFeatures, x -> struct(" +
                    "x.inclusionReasonTypeCode as inclusionReasonTypeCode, " +
                    "x.inclusionReasonValueText.*, " +
                    "'YES' as productCheck" +
                ")) as audienceDataFeatures"
            )
        );
    }

    public static Dataset<Row> removeFieldFromInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn(
            "audienceDataFeatures",
            expr(
                "transform(audienceDataFeatures, x -> struct(" +
                    "x.inclusionReasonTypeCode as inclusionReasonTypeCode, " +
                    "x.inclusionReasonValueText.audienceDataFeatureTypeCode as audienceDataFeatureTypeCode, " +
                    "x.inclusionReasonValueText.hashedAccountNumber as hashedAccountNumber, " +
                    "x.inclusionReasonValueText.accountReferenceNumber as accountReferenceNumber, " +
                    "x.inclusionReasonValueText.accountIdentifier as accountIdentifier, " +
                    "x.inclusionReasonValueText.productCode as productCode, " +
                    "x.inclusionReasonValueText.subProductCode as subProductCode, " +
                    "x.inclusionReasonValueText.marketingProductCode as marketingProductCode, " +
                    "x.inclusionReasonValueText.audienceDataFeatureStatusCode as audienceDataFeatureStatusCode, " +
                    "x.inclusionReasonValueText.expirationTimestamp as expirationTimestamp" +
                ")) as audienceDataFeatures"
            )
        );
    }

    public static Dataset<Row> renameFieldInInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn(
            "audienceDataFeatures",
            expr(
                "transform(audienceDataFeatures, x -> struct(" +
                    "x.inclusionReasonTypeCode as inclusionReasonTypeCode, " +
                    "x.inclusionReasonValueText.audienceDataFeatureTypeCode as audienceDataFeatureTypeCode, " +
                    "x.inclusionReasonValueText.hashedAccountNumber as hashedAccountNumber, " +
                    "x.inclusionReasonValueText.accountReferenceNumber as accountReferenceNumber, " +
                    "x.inclusionReasonValueText.accountIdentifier as accountIdentifier, " +
                    "x.inclusionReasonValueText.productCode as productCodeRenamed, " +
                    "x.inclusionReasonValueText.subProductCode as subProductCode, " +
                    "x.inclusionReasonValueText.marketingProductCode as marketingProductCode, " +
                    "x.inclusionReasonValueText.audienceDataFeatureStatusCode as audienceDataFeatureStatusCode, " +
                    "x.inclusionReasonValueText.entryTimestamp as entryTimestamp, " +
                    "x.inclusionReasonValueText.expirationTimestamp as expirationTimestamp" +
                ")) as audienceDataFeatures"
            )
        );
    }

    public static Dataset<Row> changeFieldTypeInInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn(
            "audienceDataFeatures",
            expr(
                "transform(audienceDataFeatures, x -> struct(" +
                    "x.inclusionReasonTypeCode as inclusionReasonTypeCode, " +
                    "cast(x.inclusionReasonValueText.productCode as int) as productCode, " +
                    "x.inclusionReasonValueText.audienceDataFeatureTypeCode as audienceDataFeatureTypeCode, " +
                    "x.inclusionReasonValueText.hashedAccountNumber as hashedAccountNumber, " +
                    "x.inclusionReasonValueText.accountReferenceNumber as accountReferenceNumber, " +
                    "x.inclusionReasonValueText.accountIdentifier as accountIdentifier, " +
                    "x.inclusionReasonValueText.subProductCode as subProductCode, " +
                    "x.inclusionReasonValueText.marketingProductCode as marketingProductCode, " +
                    "x.inclusionReasonValueText.audienceDataFeatureStatusCode as audienceDataFeatureStatusCode, " +
                    "x.inclusionReasonValueText.entryTimestamp as entryTimestamp, " +
                    "x.inclusionReasonValueText.expirationTimestamp as expirationTimestamp" +
                ")) as audienceDataFeatures"
            )
        );
    }
}
