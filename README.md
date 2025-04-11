import static org.apache.spark.sql.functions.*;

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
}
