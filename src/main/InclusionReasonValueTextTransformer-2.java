
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.*;

public class InclusionReasonValueTextTransformer {

    // Add a field to the struct manually by reconstructing the struct
    public static Dataset<Row> addFieldToInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn("inclusionReasonValueText",
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

    // Remove a field by reconstructing the struct without it
    public static Dataset<Row> removeFieldFromInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn("inclusionReasonValueText",
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
                // entryTimestamp is removed
            )
        );
    }

    // Rename a field by copying it with new name and removing old
    public static Dataset<Row> renameFieldInInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn("inclusionReasonValueText",
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

    // Change the data type of a field, by casting it to a new type
    public static Dataset<Row> changeFieldTypeInInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn("inclusionReasonValueText",
            struct(
                col("inclusionReasonValueText.audienceDataFeatureTypeCode"),
                col("inclusionReasonValueText.hashedAccountNumber"),
                col("inclusionReasonValueText.accountReferenceNumber"),
                col("inclusionReasonValueText.accountIdentifier"),
                col("inclusionReasonValueText.productCode"),
                col("inclusionReasonValueText.subProductCode"),
                col("inclusionReasonValueText.marketingProductCode"),
                col("inclusionReasonValueText.audienceDataFeatureStatusCode"),
                col("inclusionReasonValueText.entryTimestamp").cast("timestamp"),
                col("inclusionReasonValueText.expirationTimestamp")
            )
        );
    }
}
