-- Translation time: 2023-06-26T06:26:49.401690Z
-- Translation job ID: 6d78a837-9810-4e1a-ac84-7e3f68a12a19
-- Source: tdmigration/storedprocs/SUB_METRIC_MONTH_0_xfm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE PROCEDURE stored_procs.sub_metric_month_0_xfm(iyearmonth STRING)
  BEGIN
    DELETE FROM `data-dev-base-1331`.royalty_dm_dev.SUB_METRIC_MONTH_F WHERE SUB_METRIC_MONTH_F.bucketid IN(
      SELECT
          bucketid
        FROM
          `data-dev-base-1331`.r2d2_dba.SUB_COUNT_WRK
        GROUP BY 1
    )
     AND upper(SUB_METRIC_MONTH_F.yearmonth) = upper(concat('', iyearmonth, ''));
    INSERT INTO `data-dev-base-1331`.royalty_dm_dev.SUB_METRIC_MONTH_F (bucketid, calcruleid, yearmonth, val1, val2, calcmetricvalue)
      SELECT
          sub.bucketid,
          1179,
          -- RegisteredUserPct
          sub.yearmonth,
          cast(sub.registeredusercountmonth as numeric) AS registeredusercountmonth,
          cast(sub.bucketsubscribercount as numeric) AS bucketsubscribercount,
          coalesce(psm.registereduserpct, CASE
            WHEN sub.bucketsubscribercount = 0 THEN CAST(0 as BIGNUMERIC)
            ELSE sub.registeredusercountmonth / sub.bucketsubscribercount
          END) AS calcregisteruserpct
        FROM
          -- Use the royalty bearing rule if it exists
          `data-dev-base-1331`.r2d2_dba.SUB_COUNT_WRK AS sub
          LEFT OUTER JOIN `data-dev-base-1331`.r2d2_dba.PSM_RULE_WRK AS psm ON psm.bucketid = sub.bucketid
    ;
    INSERT INTO `data-dev-base-1331`.royalty_dm_dev.SUB_METRIC_MONTH_F (bucketid, calcruleid, yearmonth, val1, val2, calcmetricvalue)
      SELECT
          sub.bucketid,
          1180,
          -- ActiveUserPct
          sub.yearmonth,
          cast(sub.activecount as numeric) AS activecount,
          cast(sub.bucketsubscribercount as numeric) AS bucketsubscribercount,
          CASE
            WHEN sub.bucketsubscribercount = 0 THEN CAST(0 as BIGNUMERIC)
            ELSE sub.activecount / sub.bucketsubscribercount
          END AS activeuserpct
        FROM
          `data-dev-base-1331`.r2d2_dba.SUB_COUNT_WRK AS sub
    ;
  END;