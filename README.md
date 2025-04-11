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
                        "'productCheck', 'YES'" +  // ← Add new field
                    ")" +
                "))," +
                // Copy các field khác trong `population`
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
