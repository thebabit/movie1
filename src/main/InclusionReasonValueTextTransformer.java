import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.expr;

public class InclusionReasonValueTextTransformer {

    /**
     * Modify inclusionReasonValueText inside audienceDataFeatures:
     * - Add a new field (e.g., productCheck)
     * - Remove a field (e.g., entryTimestamp)
     * - Rename a field (e.g., productCode -> productCodeRenamed)
     */
    public static Dataset<Row> transformInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn(
            "audienceDataFeatures",
            expr("""
                transform(audienceDataFeatures, x -> struct(
                    x.inclusionReasonTypeCode as inclusionReasonTypeCode,
                    struct(
                        x.inclusionReasonValueText.audienceDataFeatureTypeCode as audienceDataFeatureTypeCode,
                        x.inclusionReasonValueText.hashedAccountNumber as hashedAccountNumber,
                        x.inclusionReasonValueText.accountReferenceNumber as accountReferenceNumber,
                        x.inclusionReasonValueText.accountIdentifier as accountIdentifier,
                        x.inclusionReasonValueText.productCode as productCodeRenamed, -- renamed
                        x.inclusionReasonValueText.subProductCode as subProductCode,
                        x.inclusionReasonValueText.marketingProductCode as marketingProductCode,
                        x.inclusionReasonValueText.audienceDataFeatureStatusCode as audienceDataFeatureStatusCode,
                        -- x.inclusionReasonValueText.entryTimestamp removed
                        x.inclusionReasonValueText.expirationTimestamp as expirationTimestamp,
                        'YES' as productCheck -- added
                    ) as inclusionReasonValueText
                ))
            """)
        );
    }
}