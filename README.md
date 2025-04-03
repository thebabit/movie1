wmi:
    _class: SQLTransformer
    sqlText: |
      SELECT entp_prty_id,
        CASE
          WHEN prod_cd = '120'
            AND sub_prod_cd IN ('065', '066', '067', '030', '031', '050', '051', '054')
            AND acct_actv_sts_cd = '002'
            AND sum_sts_cd = '0'
          THEN 'N'
          WHEN prod_cd = '120'
            AND sub_prod_cd IN ('060', '061', '062', '063')
          THEN 'Y'
          WHEN acct_actv_sts_cd = '002'
            AND sum_sts_cd = '0'
          THEN 'Y'
          WHEN prty_ar_rel_cd IN ('001', '076', '151')
          THEN 'Y'
          ELSE 'N'
        END AS wmi
      FROM cust_acct_rel;























sdi_wmi_joined:
    _class: SQLTransformer
    sqlText: |
      SELECT 
        sdi.entp_prty_id,
        sdi.sdi_only,
        wmi.wmi
      FROM sdi_only sdi
      JOIN wmi wmi
        ON sdi.entp_prty_id = wmi.entp_prty_id;
