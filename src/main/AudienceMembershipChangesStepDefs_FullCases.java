
package com.jpmc.pandi.cucumber.stepdefs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class AudienceMembershipChangesStepDefs {

    public static Dataset<Row> addFieldToInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn("population",
            functions.expr(
                "named_struct(" +
                    "'audienceDataFeatures', transform(population.audienceDataFeatures, x -> named_struct(" +
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
                            "'entryTimestamp', x.inclusionReasonValueText.entryTimestamp, " +
                            "'expirationTimestamp', x.inclusionReasonValueText.expirationTimestamp, " +
                            "'productCheck', 'YES'" +
                        ")" +
                    "))," +
                    "'enterprisePartyIdentifier', population.enterprisePartyIdentifier, " +
                    "'experimentIdentifier', population.experimentIdentifier, " +
                    "'audienceMemberEntryTimestamp', population.audienceMemberEntryTimestamp, " +
                    "'audienceMemberExpirationTimestamp', population.audienceMemberExpirationTimestamp, " +
                    "'audienceMemberStatusCode', population.audienceMemberStatusCode, " +
                    "'audienceMemberUuid', population.audienceMemberUuid, " +
                    "'audienceName', population.audienceName, " +
                    "'audiencePopulationTypeCode', population.audiencePopulationTypeCode, " +
                    "'campaignChannels', population.campaignChannels" +
                ")"
            )
        );
    }

    public static Dataset<Row> removeFieldFromInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn("population",
            functions.expr(
                "named_struct(" +
                    "'audienceDataFeatures', transform(population.audienceDataFeatures, x -> named_struct(" +
                        "'inclusionReasonTypeCode', x.inclusionReasonTypeCode, " +
                        "'inclusionReasonValueText', named_struct(" +
                            // intentionally omit 'entryTimestamp'
                            "'audienceDataFeatureTypeCode', x.inclusionReasonValueText.audienceDataFeatureTypeCode, " +
                            "'hashedAccountNumber', x.inclusionReasonValueText.hashedAccountNumber, " +
                            "'accountReferenceNumber', x.inclusionReasonValueText.accountReferenceNumber, " +
                            "'accountIdentifier', x.inclusionReasonValueText.accountIdentifier, " +
                            "'productCode', x.inclusionReasonValueText.productCode, " +
                            "'subProductCode', x.inclusionReasonValueText.subProductCode, " +
                            "'marketingProductCode', x.inclusionReasonValueText.marketingProductCode, " +
                            "'audienceDataFeatureStatusCode', x.inclusionReasonValueText.audienceDataFeatureStatusCode, " +
                            "'expirationTimestamp', x.inclusionReasonValueText.expirationTimestamp" +
                        ")" +
                    "))," +
                    "'enterprisePartyIdentifier', population.enterprisePartyIdentifier, " +
                    "'experimentIdentifier', population.experimentIdentifier, " +
                    "'audienceMemberEntryTimestamp', population.audienceMemberEntryTimestamp, " +
                    "'audienceMemberExpirationTimestamp', population.audienceMemberExpirationTimestamp, " +
                    "'audienceMemberStatusCode', population.audienceMemberStatusCode, " +
                    "'audienceMemberUuid', population.audienceMemberUuid, " +
                    "'audienceName', population.audienceName, " +
                    "'audiencePopulationTypeCode', population.audiencePopulationTypeCode, " +
                    "'campaignChannels', population.campaignChannels" +
                ")"
            )
        );
    }

    public static Dataset<Row> renameFieldInInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn("population",
            functions.expr(
                "named_struct(" +
                    "'audienceDataFeatures', transform(population.audienceDataFeatures, x -> named_struct(" +
                        "'inclusionReasonTypeCode', x.inclusionReasonTypeCode, " +
                        "'inclusionReasonValueText', named_struct(" +
                            "'audienceDataFeatureTypeCode', x.inclusionReasonValueText.audienceDataFeatureTypeCode, " +
                            "'hashedAccountNumber', x.inclusionReasonValueText.hashedAccountNumber, " +
                            "'accountReferenceNumber', x.inclusionReasonValueText.accountReferenceNumber, " +
                            "'accountIdentifier', x.inclusionReasonValueText.accountIdentifier, " +
                            "'productCodeRenamed', x.inclusionReasonValueText.productCode, " +
                            "'subProductCode', x.inclusionReasonValueText.subProductCode, " +
                            "'marketingProductCode', x.inclusionReasonValueText.marketingProductCode, " +
                            "'audienceDataFeatureStatusCode', x.inclusionReasonValueText.audienceDataFeatureStatusCode, " +
                            "'entryTimestamp', x.inclusionReasonValueText.entryTimestamp, " +
                            "'expirationTimestamp', x.inclusionReasonValueText.expirationTimestamp" +
                        ")" +
                    "))," +
                    "'enterprisePartyIdentifier', population.enterprisePartyIdentifier, " +
                    "'experimentIdentifier', population.experimentIdentifier, " +
                    "'audienceMemberEntryTimestamp', population.audienceMemberEntryTimestamp, " +
                    "'audienceMemberExpirationTimestamp', population.audienceMemberExpirationTimestamp, " +
                    "'audienceMemberStatusCode', population.audienceMemberStatusCode, " +
                    "'audienceMemberUuid', population.audienceMemberUuid, " +
                    "'audienceName', population.audienceName, " +
                    "'audiencePopulationTypeCode', population.audiencePopulationTypeCode, " +
                    "'campaignChannels', population.campaignChannels" +
                ")"
            )
        );
    }

    public static Dataset<Row> changeFieldTypeInInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn("population",
            functions.expr(
                "named_struct(" +
                    "'audienceDataFeatures', transform(population.audienceDataFeatures, x -> named_struct(" +
                        "'inclusionReasonTypeCode', x.inclusionReasonTypeCode, " +
                        "'inclusionReasonValueText', named_struct(" +
                            "'audienceDataFeatureTypeCode', x.inclusionReasonValueText.audienceDataFeatureTypeCode, " +
                            "'hashedAccountNumber', x.inclusionReasonValueText.hashedAccountNumber, " +
                            "'accountReferenceNumber', x.inclusionReasonValueText.accountReferenceNumber, " +
                            "'accountIdentifier', x.inclusionReasonValueText.accountIdentifier, " +
                            "'productCode', CAST(x.inclusionReasonValueText.productCode AS INT), " +  // <- type changed
                            "'subProductCode', x.inclusionReasonValueText.subProductCode, " +
                            "'marketingProductCode', x.inclusionReasonValueText.marketingProductCode, " +
                            "'audienceDataFeatureStatusCode', x.inclusionReasonValueText.audienceDataFeatureStatusCode, " +
                            "'entryTimestamp', x.inclusionReasonValueText.entryTimestamp, " +
                            "'expirationTimestamp', x.inclusionReasonValueText.expirationTimestamp" +
                        ")" +
                    "))," +
                    "'enterprisePartyIdentifier', population.enterprisePartyIdentifier, " +
                    "'experimentIdentifier', population.experimentIdentifier, " +
                    "'audienceMemberEntryTimestamp', population.audienceMemberEntryTimestamp, " +
                    "'audienceMemberExpirationTimestamp', population.audienceMemberExpirationTimestamp, " +
                    "'audienceMemberStatusCode', population.audienceMemberStatusCode, " +
                    "'audienceMemberUuid', population.audienceMemberUuid, " +
                    "'audienceName', population.audienceName, " +
                    "'audiencePopulationTypeCode', population.audiencePopulationTypeCode, " +
                    "'campaignChannels', population.campaignChannels" +
                ")"
            )
        );
    }
}
