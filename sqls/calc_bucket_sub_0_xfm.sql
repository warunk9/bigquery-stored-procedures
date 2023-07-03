-- Translation time: 2023-06-26T06:26:49.401690Z
-- Translation job ID: 6d78a837-9810-4e1a-ac84-7e3f68a12a19
-- Source: tdmigration/storedprocs/CALC_BUCKET_SUB_0_xfm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE PROCEDURE stored_procs.calc_bucket_sub_0_xfm(iyearmonth STRING, project_id STRING)
  BEGIN
    DECLARE recordcount INT64;
    DECLARE undefinednumber INT64;
    DECLARE ijobid INT64;
    -- Grab Royalty Job
    SET ijobid = (
      SELECT DISTINCT
          ROYALTY_JOB.royaltyjobid
        FROM
          project_id.r2d2_dba.ROYALTY_JOB
        WHERE upper(ROYALTY_JOB.yearmonth) = upper(concat('', iyearmonth, ''))
         AND ROYALTY_JOB.royaltyjobstatusid = 0
         AND ROYALTY_JOB.royaltyjobtypeid = 1
    );
    -- Pending
    -- Label Royalties
    --  Check job ID
    SET recordcount = (
      SELECT
          count(1)
        FROM
          project_id.r2d2_dba.PER_SUBSCRIBER_CALC_MONTH
        WHERE PER_SUBSCRIBER_CALC_MONTH.jobid = ijobid
    );
    IF recordcount > 0 THEN
      SET recordcount = div(1, 0);
    END IF;
    --  ABORT
    --   Check for any Entities not approved for year month
    --   If all Entities have been approved for year month, then abort
    SET undefinednumber = (
      SELECT
          count(1)
        FROM
          project_id.r2d2_dba.ROYALTY_JOB
        WHERE upper(ROYALTY_JOB.yearmonth) = upper(concat('', iyearmonth, ''))
         AND ROYALTY_JOB.royaltyjobstatusid = 0
    );
    IF undefinednumber = 0 THEN
      SET undefinednumber = div(1, 0);
    END IF;
    --  ABORT
    --  royalty job rerun lookup:
    --        If month is not approved, delete month and run
    IF undefinednumber > 0 THEN
      DELETE FROM project_id.r2d2_dba.PER_SUBSCRIBER_CALC_MONTH AS d WHERE upper(d.yearmonth) = upper(concat('', iyearmonth, ''))
       AND d.bucketid IN(
        SELECT
            bucketid
          FROM
            project_id.r2d2_dba.entity_expired_bucket_v
          WHERE upper(yearmonth) = upper(concat('', iyearmonth, ''))
      )
       AND d.jobid IN(
        SELECT
            royaltyjobid
          FROM
            project_id.r2d2_dba.entity_expired_bucket_v
          WHERE upper(yearmonth) = upper(concat('', iyearmonth, ''))
      );
    END IF;
    --  clear work table
    TRUNCATE TABLE project_id.r2d2_dba.PER_SUB_CALC_MONTH_WRK;
    --  insert subscriber calcs
    INSERT INTO project_id.r2d2_dba.PER_SUB_CALC_MONTH_WRK (bucketid, yearmonth, calcruleid, calctypeid, sharepct, eligiblemonthlycntflg, bucketsubscribercount, bucketsubscribercountrn, calcsubcountmonth, contractpersubrateid, contractsubrate, contractsubratecurrencyid, local_sub_rate, local_currency_id, bucketplaycountentity, bucketplaycountall, marketsharefloorpercent, propsharepct, bucketpayment, royaltypool, jobid, avgbaddebtcount, registereduserpct, familyplanparentsubcount, familyplanchildsubcount, activeuserpct)
      SELECT
          scw.bucketid,
          scw.yearmonth,
          scw.calcruleid,
          scw.calctypeid,
          scw.retailpricesharepct AS sharepct,
          scw.eligiblemonthlycntflg,
          scw.bucketsubscribercount,
          scw.bucketsubscribercountrn,
          scw.calcsubcountmonth,
          pscr.persubrateid,
          psr.rate AS subrate,
          psr.ratecurrencyid AS subratecurrencyid,
          psr.rate * CAST(coalesce(cc.multiplier, 1) as FLOAT64) AS local_sub_rate,
          cc.destinationcurrencyid AS local_currency_id,
          psv.entitystreamcount AS bucketplaycountentity,
          psv.totalstreamcount AS bucketplaycountall,
          psv.marketsharefloorpercent,
          CASE
            WHEN crl.useprorataflg = 1
             AND psv.marketsharefloorpercent IS NULL THEN CASE
              WHEN psv.entitystreamcount = 0 THEN CAST(0 as FLOAT64)
              ELSE psv.entitystreamcount / psv.totalstreamcount
            END
            WHEN crl.useprorataflg = 1
             AND psv.marketsharefloorpercent IS NOT NULL THEN CASE
              WHEN psv.entitystreamcount = 0 THEN CAST(0 as FLOAT64)
              WHEN psv.entitystreamcount / psv.totalstreamcount < psv.marketsharefloorpercent THEN psv.marketsharefloorpercent
              ELSE psv.entitystreamcount / psv.totalstreamcount
            END
            ELSE CAST(1 as FLOAT64)
          END AS propsharepct,
          bqutil.fn.cw_round_half_even(CAST(scw.calcsubcountmonth * (psr.rate * CAST(coalesce(cc.multiplier, 1) as FLOAT64)) * scw.retailpricesharepct * CASE
            WHEN crl.useprorataflg = 1
             AND psv.marketsharefloorpercent IS NULL THEN CASE
              WHEN psv.entitystreamcount = 0 THEN CAST(0 as FLOAT64)
              ELSE psv.entitystreamcount / psv.totalstreamcount
            END
            WHEN crl.useprorataflg = 1
             AND psv.marketsharefloorpercent IS NOT NULL THEN CASE
              WHEN psv.entitystreamcount = 0 THEN CAST(0 as FLOAT64)
              WHEN psv.entitystreamcount / psv.totalstreamcount < psv.marketsharefloorpercent THEN psv.marketsharefloorpercent
              ELSE psv.entitystreamcount / psv.totalstreamcount
            END
            ELSE CAST(1 as FLOAT64)
          END as BIGNUMERIC), 4) AS bucketpayment,
          bqutil.fn.cw_round_half_even(CAST(scw.calcsubcountmonth * (psr.rate * CAST(coalesce(cc.multiplier, 1) as FLOAT64)) * scw.retailpricesharepct as BIGNUMERIC), 4) AS royaltypool,
          ijobid,
          scw.avgbaddebtcount,
          psr.registereduserpct,
          --  fam plan counts: need to determine all use cases & actual meaning of these
          --  hard-code family plan to 0 for now
          0 AS familyplanparentsubcount,
          0 AS familyplanchildsubcount,
          psr.activeuserpct
        FROM
          project_id.r2d2_dba.entity_statement_bucket_v AS esb
          INNER JOIN project_id.r2d2_dba.sub_count_wrk AS scw ON scw.bucketid = esb.buckettierid
          INNER JOIN project_id.r2d2_dba.calendar_dim_month AS cal ON cal.yearmonth = scw.yearmonth
          INNER JOIN project_id.r2d2_dba.contract_tier_x_calc_rule AS cxc ON cxc.contracttierid = esb.buckettierid
           AND cxc.effectivedt <= cal.monthstartdt
           AND cxc.expirationdt >= cal.monthenddt
           AND cxc.calcruleid = scw.calcruleid
          INNER JOIN project_id.r2d2_dba.calc_rule AS crl ON crl.calcruleid = cxc.calcruleid
           AND crl.calctypeid IN(
            2
          )
          INNER JOIN --  ProRata
          project_id.r2d2_dba.psm_rule_wrk AS psr ON psr.calcruleid = crl.calcruleid
           AND psr.bucketid = cxc.contracttierid
          INNER JOIN -- Should have 100% of  calc type 2
          project_id.r2d2_dba.per_sub_calc_rule AS pscr ON pscr.calcruleid = crl.calcruleid
          INNER JOIN project_id.r2d2_dba.bucket_prop_share_v AS psv ON psv.bucketid = esb.buckettierid
           AND scw.yearmonth = psv.yearmonth
          INNER JOIN project_id.r2d2_dba.bucket_country_currency_v AS bcc ON bcc.bucketid = psv.bucketid
          LEFT OUTER JOIN project_id.r2d2_dba.currency_converter AS cc ON bcc.defaultcurrencyid = cc.destinationcurrencyid
           AND psr.ratecurrencyid = cc.sourcecurrencyid
           AND cc.yearmonth = scw.yearmonth
          LEFT OUTER JOIN project_id.r2d2_dba.contract_tier_x_contract_tier AS nofam ON nofam.parentcontracttierid = scw.bucketid
           AND nofam.relationshipid = 1006
    ;
    --  left join; NULL means this may be a combo bucket so calculate the split parent/child counts
    INSERT INTO project_id.r2d2_dba.PER_SUB_CALC_MONTH_WRK (bucketid, yearmonth, calcruleid, calctypeid, sharepct, eligiblemonthlycntflg, bucketsubscribercount, bucketsubscribercountrn, calcsubcountmonth, contractpersubrateid, contractsubrate, contractsubratecurrencyid, local_sub_rate, local_currency_id, bucketplaycountentity, bucketplaycountall, marketsharefloorpercent, propsharepct, bucketpayment, royaltypool, jobid, avgbaddebtcount, familyplanparentsubcount, familyplanchildsubcount, activeuserpct)
      SELECT
          scw.bucketid,
          scw.yearmonth,
          scw.calcruleid,
          scw.calctypeid,
          scw.retailpricesharepct AS sharepct,
          scw.eligiblemonthlycntflg,
          scw.bucketsubscribercount,
          scw.bucketsubscribercountrn,
          scw.calcsubcountmonth,
          psr.persubrateid,
          psr.subrate,
          psr.subratecurrencyid,
          psr.subrate * CAST(coalesce(cc.multiplier, 1) as FLOAT64) AS local_sub_rate,
          cc.destinationcurrencyid AS local_currency_id,
          psv.entitystreamcount AS bucketplaycountentity,
          psv.totalstreamcount AS bucketplaycountall,
          psv.marketsharefloorpercent,
          CASE
            WHEN crl.useprorataflg = 1
             AND psv.marketsharefloorpercent IS NULL THEN CASE
              WHEN psv.entitystreamcount = 0 THEN CAST(0 as FLOAT64)
              ELSE psv.entitystreamcount / psv.totalstreamcount
            END
            WHEN crl.useprorataflg = 1
             AND psv.marketsharefloorpercent IS NOT NULL THEN CASE
              WHEN psv.entitystreamcount = 0 THEN CAST(0 as FLOAT64)
              WHEN psv.entitystreamcount / psv.totalstreamcount < psv.marketsharefloorpercent THEN psv.marketsharefloorpercent
              ELSE psv.entitystreamcount / psv.totalstreamcount
            END
            ELSE CAST(1 as FLOAT64)
          END AS propsharepct,
          bqutil.fn.cw_round_half_even(CAST(scw.calcsubcountmonth * (psr.subrate * CAST(coalesce(cc.multiplier, 1) as FLOAT64)) * pscr.sharepct * CASE
            WHEN crl.useprorataflg = 1
             AND psv.marketsharefloorpercent IS NULL THEN CASE
              WHEN psv.entitystreamcount = 0 THEN CAST(0 as FLOAT64)
              ELSE psv.entitystreamcount / psv.totalstreamcount
            END
            WHEN crl.useprorataflg = 1
             AND psv.marketsharefloorpercent IS NOT NULL THEN CASE
              WHEN psv.entitystreamcount = 0 THEN CAST(0 as FLOAT64)
              WHEN psv.entitystreamcount / psv.totalstreamcount < psv.marketsharefloorpercent THEN psv.marketsharefloorpercent
              ELSE psv.entitystreamcount / psv.totalstreamcount
            END
            ELSE CAST(1 as FLOAT64)
          END as BIGNUMERIC), 4) AS bucketpayment,
          bqutil.fn.cw_round_half_even(CAST(scw.calcsubcountmonth * (psr.subrate * CAST(coalesce(cc.multiplier, 1) as FLOAT64)) * pscr.sharepct as BIGNUMERIC), 4) AS royaltypool,
          ijobid,
          scw.avgbaddebtcount,
          --  hard-code family plan to 0 for now
          0 AS familyplanparentsubcount,
          0 AS familyplanchildsubcount,
          CASE
            WHEN scw.calcsubcountmonth = 0 THEN CAST(0 as FLOAT64)
            ELSE scw.activecount / scw.calcsubcountmonth
          END AS activeuserpct
        FROM
          project_id.r2d2_dba.entity_statement_bucket_v AS esb
          INNER JOIN project_id.r2d2_dba.sub_count_wrk AS scw ON scw.bucketid = esb.buckettierid
          INNER JOIN project_id.r2d2_dba.calendar_dim_month AS cal ON cal.yearmonth = scw.yearmonth
          INNER JOIN project_id.r2d2_dba.contract_tier_x_calc_rule AS cxc ON cxc.contracttierid = esb.buckettierid
           AND cxc.effectivedt <= cal.monthstartdt
           AND cxc.expirationdt >= cal.monthenddt
           AND scw.calcruleid = cxc.calcruleid
          INNER JOIN project_id.r2d2_dba.calc_rule AS crl ON crl.calcruleid = cxc.calcruleid
           AND crl.calctypeid IN(
            3, 5
          )
          INNER JOIN --  Min Fee, Retail Price
          project_id.r2d2_dba.per_sub_calc_rule AS pscr ON pscr.calcruleid = crl.calcruleid
          INNER JOIN project_id.r2d2_dba.per_sub_rate AS psr ON psr.persubrateid = pscr.persubrateid
           AND calcsubcountmonth > psr.subcount
          INNER JOIN project_id.r2d2_dba.bucket_prop_share_v AS psv ON psv.bucketid = esb.buckettierid
           AND scw.yearmonth = psv.yearmonth
          INNER JOIN project_id.r2d2_dba.bucket_country_currency_v AS bcc ON bcc.bucketid = psv.bucketid
          LEFT OUTER JOIN project_id.r2d2_dba.currency_converter AS cc ON bcc.defaultcurrencyid = cc.destinationcurrencyid
           AND psr.subratecurrencyid = cc.sourcecurrencyid
           AND cc.yearmonth = scw.yearmonth
          LEFT OUTER JOIN project_id.r2d2_dba.contract_tier_x_contract_tier AS nofam ON nofam.parentcontracttierid = scw.bucketid
           AND nofam.relationshipid = 1006
        QUALIFY row_number() OVER (PARTITION BY esb.buckettierid, crl.calcruleid, psv.yearmonth ORDER BY subcount DESC) = 1
    ;
    --  left join; NULL means this may be a combo bucket so calculate the split parent/child counts
    BEGIN TRANSACTION;
    INSERT INTO project_id.r2d2_dba.PER_SUBSCRIBER_CALC_MONTH (bucketid, countryid, yearmonth, calcruleid, calctypeid, sharepct, eligiblemonthlycntflg, bucketsubscribercount, bucketsubscribercountrn, calcsubcountmonth, contractsubrate, contractsubratecurrencyid, persubrateid, subrate, subratecurrencyid, bucketplaycountentity, bucketplaycountall, propsharepct, bucketpayment, royaltypool, jobid, avgbaddebtcount, registereduserpct, familyplanparentsubcount, familyplanchildsubcount, marketsharefloorpercent, activeuserpct)
      SELECT
          p.bucketid,
          esb.countryid,
          yearmonth,
          calcruleid,
          calctypeid,
          sharepct,
          eligiblemonthlycntflg,
          bucketsubscribercount,
          bucketsubscribercountrn,
          calcsubcountmonth,
          contractsubrate,
          contractsubratecurrencyid,
          contractpersubrateid,
          local_sub_rate,
          local_currency_id,
          bucketplaycountentity,
          bucketplaycountall,
          propsharepct,
          bucketpayment,
          royaltypool,
          jobid,
          avgbaddebtcount,
          registereduserpct,
          familyplanparentsubcount,
          familyplanchildsubcount,
          marketsharefloorpercent,
          p.activeuserpct
        FROM
          project_id.r2d2_dba.PER_SUB_CALC_MONTH_WRK AS p
          INNER JOIN project_id.r2d2_dba.ENTITY_BUCKET_COUNTRY_MV AS esb ON esb.bucketid = p.bucketid
    ;
    COMMIT TRANSACTION;
  END;