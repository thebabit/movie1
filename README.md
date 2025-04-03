data_feature_value:
    _class: SQLTransformer
    sqlText: |
      SELECT 
        entp_prty_id,
        TO_JSON(NAMED_STRUCT(
          'dataFeatures', FILTER(ARRAY(
            CASE 
              WHEN sdi_only IS NOT NULL THEN
                NAMED_STRUCT(
                  'featureCalculationTypeCode', 'SELFDIRECTEDACCOUNTS_ONLY_ACTIVE_OWNERSHIP_INDICATOR',
                  'featureCalculationValueText', CAST(sdi_only AS STRING)
                )
            END,
            CASE 
              WHEN wmi IS NOT NULL THEN
                NAMED_STRUCT(
                  'featureCalculationTypeCode', 'WMPI_ONLY_ACTIVE_OWNERSHIP_INDICATOR',
                  'featureCalculationValueText', CAST(wmi AS STRING)
                )
            END
          ), x -> x IS NOT NULL)
        )) AS data_fetr_val_tx
      FROM sdi_wmi_joined;
