
package com.example.spark.schema;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class InclusionReasonSchemaMutator {

    public static Dataset<Row> addFieldToInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn("audienceDataFeatures",
            functions.expr(
                "transform(audienceDataFeatures, x, " +
                "named_struct(" +
                "  'inclusionReasonTypeCode', x.inclusionReasonTypeCode," +
                "  'inclusionReasonValueText', " +
                "    struct(" +
                "      x.inclusionReasonValueText.audienceDataFeatureTypeCode as audienceDataFeatureTypeCode," +
                "      x.inclusionReasonValueText.hashedAccountNumber as hashedAccountNumber," +
                "      x.inclusionReasonValueText.accountReferenceNumber as accountReferenceNumber," +
                "      x.inclusionReasonValueText.accountIdentifier as accountIdentifier," +
                "      x.inclusionReasonValueText.productCode as productCode," +
                "      x.inclusionReasonValueText.subProductCode as subProductCode," +
                "      x.inclusionReasonValueText.marketingProductCode as marketingProductCode," +
                "      x.inclusionReasonValueText.audienceDataFeatureStatusCode as audienceDataFeatureStatusCode," +
                "      x.inclusionReasonValueText.entryTimestamp as entryTimestamp," +
                "      x.inclusionReasonValueText.expirationTimestamp as expirationTimestamp," +
                "      'YES' as productCheck" +
                "    )" +
                ")"
            )
        );
    }

    public static Dataset<Row> removeFieldFromInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn("audienceDataFeatures",
            functions.expr(
                "transform(audienceDataFeatures, x, " +
                "named_struct(" +
                "  'inclusionReasonTypeCode', x.inclusionReasonTypeCode," +
                "  'inclusionReasonValueText', " +
                "    struct(" +
                "      x.inclusionReasonValueText.audienceDataFeatureTypeCode as audienceDataFeatureTypeCode," +
                "      x.inclusionReasonValueText.hashedAccountNumber as hashedAccountNumber," +
                "      x.inclusionReasonValueText.accountReferenceNumber as accountReferenceNumber," +
                "      x.inclusionReasonValueText.accountIdentifier as accountIdentifier," +
                "      x.inclusionReasonValueText.productCode as productCode," +
                "      x.inclusionReasonValueText.subProductCode as subProductCode," +
                "      x.inclusionReasonValueText.marketingProductCode as marketingProductCode," +
                "      x.inclusionReasonValueText.audienceDataFeatureStatusCode as audienceDataFeatureStatusCode," +
                "      x.inclusionReasonValueText.expirationTimestamp as expirationTimestamp" +
                "    )" +
                ")"
            )
        );
    }

    public static Dataset<Row> renameFieldInInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn("audienceDataFeatures",
            functions.expr(
                "transform(audienceDataFeatures, x, " +
                "named_struct(" +
                "  'inclusionReasonTypeCode', x.inclusionReasonTypeCode," +
                "  'inclusionReasonValueText', " +
                "    struct(" +
                "      x.inclusionReasonValueText.audienceDataFeatureTypeCode as audienceDataFeatureTypeCode," +
                "      x.inclusionReasonValueText.hashedAccountNumber as hashedAccountNumber," +
                "      x.inclusionReasonValueText.accountReferenceNumber as accountReferenceNumber," +
                "      x.inclusionReasonValueText.accountIdentifier as accountIdentifier," +
                "      x.inclusionReasonValueText.productCode as productCodeRenamed," +
                "      x.inclusionReasonValueText.subProductCode as subProductCode," +
                "      x.inclusionReasonValueText.marketingProductCode as marketingProductCode," +
                "      x.inclusionReasonValueText.audienceDataFeatureStatusCode as audienceDataFeatureStatusCode," +
                "      x.inclusionReasonValueText.entryTimestamp as entryTimestamp," +
                "      x.inclusionReasonValueText.expirationTimestamp as expirationTimestamp" +
                "    )" +
                ")"
            )
        );
    }

    public static Dataset<Row> changeFieldTypeInInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn("audienceDataFeatures",
            functions.expr(
                "transform(audienceDataFeatures, x, " +
                "named_struct(" +
                "  'inclusionReasonTypeCode', x.inclusionReasonTypeCode," +
                "  'inclusionReasonValueText', " +
                "    struct(" +
                "      x.inclusionReasonValueText.audienceDataFeatureTypeCode as audienceDataFeatureTypeCode," +
                "      x.inclusionReasonValueText.hashedAccountNumber as hashedAccountNumber," +
                "      x.inclusionReasonValueText.accountReferenceNumber as accountReferenceNumber," +
                "      x.inclusionReasonValueText.accountIdentifier as accountIdentifier," +
                "      cast(x.inclusionReasonValueText.productCode as int) as productCode," +
                "      x.inclusionReasonValueText.subProductCode as subProductCode," +
                "      x.inclusionReasonValueText.marketingProductCode as marketingProductCode," +
                "      x.inclusionReasonValueText.audienceDataFeatureStatusCode as audienceDataFeatureStatusCode," +
                "      x.inclusionReasonValueText.entryTimestamp as entryTimestamp," +
                "      x.inclusionReasonValueText.expirationTimestamp as expirationTimestamp" +
                "    )" +
                ")"
            )
        );
    }
}
