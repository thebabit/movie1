
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class SchemaDriftFieldAdder {

    public static Dataset<Row> addFieldToInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn(
            "audienceDataFeatures",
            expr("transform(audienceDataFeatures, x -> struct(" +
                    "x.inclusionReasonTypeCode as inclusionReasonTypeCode, " +
                    "named_struct(" +
                        "'audienceDataFeatureTypeCode', x.inclusionReasonValueText.audienceDataFeatureTypeCode, " +
                        "'hashedAccountNumber', x.inclusionReasonValueText.hashedAccountNumber, " +
                        "'accountReferenceNumber', x.inclusionReasonValueText.accountReferenceNumber, " +
                        "'accountIdentifier', x.inclusionReasonValueText.accountIdentifier, " +
                        "'productCode', x.inclusionReasonValueText.productCode, " +
                        "'subProductCode', x.inclusionReasonValueText.subProductCode, " +
                        "'marketingProductCode', x.inclusionReasonValueText.marketingProductCode, " +
                        "'audienceDataFeatureStatusCode', x.inclusionReasonValueText.audienceDataFeatureStatusCode, " +
                        "'entryTimestamp', x.inclusionReasonValueText.entryTimestamp, " +
                        "'expirationTimestamp', x.inclusionReasonValueText.expirationTimestamp, " +
                        "'productCheck', 'YES'" +
                    ") as inclusionReasonValueText" +
                "))")
        );
    }
}


// Case 2: Remove a field from InclusionReasonValueText
public static Dataset<Row> removeFieldFromInclusionReasonValueText(Dataset<Row> input) {
    return input.withColumn("audienceDataFeatures",
        functions.expr(
            "transform(audienceDataFeatures, x -> named_struct(" +
            "'inclusionReasonTypeCode', x.inclusionReasonTypeCode, " +
            "'inclusionReasonValueText', named_struct(" +
            "'audienceDataFeatureTypeCode', x.inclusionReasonValueText.audienceDataFeatureTypeCode, " +
            "'hashedAccountNumber', x.inclusionReasonValueText.hashedAccountNumber, " +
            "'accountReferenceNumber', x.inclusionReasonValueText.accountReferenceNumber, " +
            "'accountIdentifier', x.inclusionReasonValueText.accountIdentifier, " +
            "'productCode', x.inclusionReasonValueText.productCode, " +
            "'subProductCode', x.inclusionReasonValueText.subProductCode, " +
            "'marketingProductCode', x.inclusionReasonValueText.marketingProductCode, " +
            "'audienceDataFeatureStatusCode', x.inclusionReasonValueText.audienceDataFeatureStatusCode, " +
            "'expirationTimestamp', x.inclusionReasonValueText.expirationTimestamp))" +
            "))"));
}

// Case 3: Rename a field in InclusionReasonValueText
public static Dataset<Row> renameFieldInInclusionReasonValueText(Dataset<Row> input) {
    return input.withColumn("audienceDataFeatures",
        functions.expr(
            "transform(audienceDataFeatures, x -> named_struct(" +
            "'inclusionReasonTypeCode', x.inclusionReasonTypeCode, " +
            "'inclusionReasonValueText', named_struct(" +
            "'audienceDataFeatureTypeCode', x.inclusionReasonValueText.audienceDataFeatureTypeCode, " +
            "'hashedAccountNumber', x.inclusionReasonValueText.hashedAccountNumber, " +
            "'accountReferenceNumber', x.inclusionReasonValueText.accountReferenceNumber, " +
            "'accountIdentifier', x.inclusionReasonValueText.accountIdentifier, " +
            "'productCodeRenamed', x.inclusionReasonValueText.productCode, " +  // renamed field
            "'subProductCode', x.inclusionReasonValueText.subProductCode, " +
            "'marketingProductCode', x.inclusionReasonValueText.marketingProductCode, " +
            "'audienceDataFeatureStatusCode', x.inclusionReasonValueText.audienceDataFeatureStatusCode, " +
            "'entryTimestamp', x.inclusionReasonValueText.entryTimestamp, " +
            "'expirationTimestamp', x.inclusionReasonValueText.expirationTimestamp))" +
            "))"));
}

// Case 4: Change type of a field in InclusionReasonValueText (cast to integer for example)
public static Dataset<Row> changeTypeFieldInInclusionReasonValueText(Dataset<Row> input) {
    return input.withColumn("audienceDataFeatures",
        functions.expr(
            "transform(audienceDataFeatures, x -> named_struct(" +
            "'inclusionReasonTypeCode', x.inclusionReasonTypeCode, " +
            "'inclusionReasonValueText', named_struct(" +
            "'audienceDataFeatureTypeCode', x.inclusionReasonValueText.audienceDataFeatureTypeCode, " +
            "'hashedAccountNumber', x.inclusionReasonValueText.hashedAccountNumber, " +
            "'accountReferenceNumber', x.inclusionReasonValueText.accountReferenceNumber, " +
            "'accountIdentifier', x.inclusionReasonValueText.accountIdentifier, " +
            "'productCode', cast(x.inclusionReasonValueText.productCode as int), " +  // type changed
            "'subProductCode', x.inclusionReasonValueText.subProductCode, " +
            "'marketingProductCode', x.inclusionReasonValueText.marketingProductCode, " +
            "'audienceDataFeatureStatusCode', x.inclusionReasonValueText.audienceDataFeatureStatusCode, " +
            "'entryTimestamp', x.inclusionReasonValueText.entryTimestamp, " +
            "'expirationTimestamp', x.inclusionReasonValueText.expirationTimestamp))" +
            "))"));
}
