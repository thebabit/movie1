
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class InclusionReasonSchemaMutator {

    public static Dataset<Row> addFieldToInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn("audienceDataFeatures",
            expr("transform(audienceDataFeatures, x -> struct(" +
                "x.inclusionReasonTypeCode as inclusionReasonTypeCode, " +
                "struct(" +
                "x.inclusionReasonValueText.audienceDataFeatureTypeCode as audienceDataFeatureTypeCode, " +
                "x.inclusionReasonValueText.hashedAccountNumber as hashedAccountNumber, " +
                "x.inclusionReasonValueText.accountReferenceNumber as accountReferenceNumber, " +
                "x.inclusionReasonValueText.accountIdentifier as accountIdentifier, " +
                "x.inclusionReasonValueText.productCode as productCode, " +
                "x.inclusionReasonValueText.subProductCode as subProductCode, " +
                "x.inclusionReasonValueText.marketingProductCode as marketingProductCode, " +
                "x.inclusionReasonValueText.audienceDataFeatureStatusCode as audienceDataFeatureStatusCode, " +
                "x.inclusionReasonValueText.entryTimestamp as entryTimestamp, " +
                "x.inclusionReasonValueText.expirationTimestamp as expirationTimestamp, " +
                "'YES' as productCheck" +
                ") as inclusionReasonValueText" +
            "))"));
    }

    public static Dataset<Row> removeFieldFromInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn("audienceDataFeatures",
            expr("transform(audienceDataFeatures, x -> struct(" +
                "x.inclusionReasonTypeCode as inclusionReasonTypeCode, " +
                "struct(" +
                "x.inclusionReasonValueText.audienceDataFeatureTypeCode as audienceDataFeatureTypeCode, " +
                "x.inclusionReasonValueText.hashedAccountNumber as hashedAccountNumber, " +
                "x.inclusionReasonValueText.accountReferenceNumber as accountReferenceNumber, " +
                "x.inclusionReasonValueText.accountIdentifier as accountIdentifier, " +
                "x.inclusionReasonValueText.subProductCode as subProductCode, " +
                "x.inclusionReasonValueText.marketingProductCode as marketingProductCode, " +
                "x.inclusionReasonValueText.audienceDataFeatureStatusCode as audienceDataFeatureStatusCode, " +
                "x.inclusionReasonValueText.expirationTimestamp as expirationTimestamp" +
                ") as inclusionReasonValueText" +
            "))"));
    }

    public static Dataset<Row> renameFieldInInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn("audienceDataFeatures",
            expr("transform(audienceDataFeatures, x -> struct(" +
                "x.inclusionReasonTypeCode as inclusionReasonTypeCode, " +
                "struct(" +
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
                ") as inclusionReasonValueText" +
            "))"));
    }
}

    /**
     * Changes the data type of a field in inclusionReasonValueText by casting the field.
     * Note: This is a simulated cast assuming the field is string. Adjust types as needed.
     */
    public static Dataset<Row> changeFieldTypeInInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn("audienceDataFeatures",
            functions.expr(
                "transform(audienceDataFeatures, x -> named_struct(" +
                "  'inclusionReasonTypeCode', x.inclusionReasonTypeCode, " +
                "  'inclusionReasonValueText', named_struct(" +
                "     'audienceDataFeatureTypeCode', x.inclusionReasonValueText.audienceDataFeatureTypeCode, " +
                "     'hashedAccountNumber', x.inclusionReasonValueText.hashedAccountNumber, " +
                "     'accountReferenceNumber', x.inclusionReasonValueText.accountReferenceNumber, " +
                "     'accountIdentifier', x.inclusionReasonValueText.accountIdentifier, " +
                "     'productCode', x.inclusionReasonValueText.productCode, " +
                "     'subProductCode', x.inclusionReasonValueText.subProductCode, " +
                "     'marketingProductCode', x.inclusionReasonValueText.marketingProductCode, " +
                "     'audienceDataFeatureStatusCode', x.inclusionReasonValueText.audienceDataFeatureStatusCode, " +
                "     'entryTimestamp', CAST(x.inclusionReasonValueText.entryTimestamp AS date), " +
                "     'expirationTimestamp', x.inclusionReasonValueText.expirationTimestamp" +
                "  )" +
                "))"
            )
        );
    }

}