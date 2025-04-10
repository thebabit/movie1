Feature: Audience Generation - Schema Drift Detection in Data Feature Delta Processing

  Scenario: Happy path - Two audiences with NO_CHANGE in data function if no difference is detected
    Given delta is turned off
    And a population file exists with the following data for the ACCOUNT_CARD_EARLY_MONTH audience
      | enterprisePartyIdentifier | audiencePopulationTypeCode | inclusionReasonTypeCode | hashedAccountNumber | accountIdentifier | productCode | subProductCode | marketingProductCode | audienceDataFeatureStatusCode | entryOffset | expirationOffset |
      | 123456789                 | ACCOUNT                     | PRODUCT                 | hash1               | acc1              | P1          | sub1           | mp1                   | ACTIVE                        | 10          | 20               |
    When the membership change job is run
    Then the delta file contains the following records for ACCOUNT_CARD_EARLY_MONTH audiences
      | enterprisePartyIdentifier | audiencePopulationTypeCode | resultTag |
      | 123456789                 | ACCOUNT                     | NO_CHANGE |

  Scenario: Schema Drift - Field removed from InclusionReasonValueText
    Given delta is turned off
    And a population file exists with the following data for the ACCOUNT_CARD_EARLY_MONTH audience
      | enterprisePartyIdentifier | audiencePopulationTypeCode | inclusionReasonTypeCode | hashedAccountNumber | accountIdentifier | productCode | audienceDataFeatureStatusCode | campaignIdentifier | campaignChannels |
      | 123456789                 | ACCOUNT                     | PRODUCT                 | hash1               | acc1              | P1          | ACTIVE                        | camp1              | email            |
    When the membership change job is run
    Then the delta file contains the following records for ACCOUNT_CARD_EARLY_MONTH audiences
      | enterprisePartyIdentifier | audiencePopulationTypeCode | resultTag |
      | 123456789                 | ACCOUNT                     | UPDATED   |

  Scenario: Schema Drift - Field renamed in InclusionReasonValueText
    Given delta is turned off
    And a population file exists with the following data for the ACCOUNT_CARD_EARLY_MONTH audience
      | enterprisePartyIdentifier | audiencePopulationTypeCode | inclusionReasonTypeCode | productCodeRenamed | audienceDataFeatureStatusCode | campaignIdentifier | campaignChannels |
      | 123456789                 | ACCOUNT                     | PRODUCT                 | P1                 | ACTIVE                        | camp1              | email            |
    When the membership change job is run
    Then the delta file contains the following records for ACCOUNT_CARD_EARLY_MONTH audiences
      | enterprisePartyIdentifier | audiencePopulationTypeCode | resultTag |
      | 123456789                 | ACCOUNT                     | UPDATED   |

  Scenario: Schema Drift - Field added to InclusionReasonValueText
    Given delta is turned off
    And a population file exists with the following data for the ACCOUNT_CARD_EARLY_MONTH audience
      | enterprisePartyIdentifier | audiencePopulationTypeCode | inclusionReasonTypeCode | productCode | productCheck | audienceDataFeatureStatusCode | campaignIdentifier | campaignChannels |
      | 123456789                 | ACCOUNT                     | PRODUCT                 | P1          | YES          | ACTIVE                        | camp1              | email            |
    When the membership change job is run
    Then the delta file contains the following records for ACCOUNT_CARD_EARLY_MONTH audiences
      | enterprisePartyIdentifier | audiencePopulationTypeCode | resultTag |
      | 123456789                 | ACCOUNT                     | UPDATED   |