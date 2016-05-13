
CALL pp_monitor.drop_table('pp_scratch.t1_mm16_bad_smb');
CREATE TABLE pp_scratch.t1_mm16_bad_smb AS (
SEL b.*, a.report_date
,CASE  WHEN a.bad_flag_detail = 'SMB/LM_Fraud' THEN 1
       WHEN a.bad_flag_detail = 'SMB/LM_Credit_T2' THEN 2
       WHEN a.bad_flag_detail = 'SMB/LM_Credit_T1' THEN 3
       ELSE 0 END AS is_bad
,CASE  WHEN a.bad_flag_detail IS NULL THEN 'Good'
       ELSE a.bad_flag_detail END AS bad_flag_detail
, g.pp_merch_gross_loss, pp_merch_net_loss
FROM pp_scratch.t1_mm16_base_smb AS b
LEFT JOIN pp_risk_roe_views.unified_seller_bad_tag AS a
ON b.rcvr_id = a.cust_id
AND b.pmt_start_date(FORMAT 'yyyy/mm')(CHAR(7)) = a.tag_date(FORMAT 'yyyy/mm')(CHAR(7))
AND a.tag_segment = '02 SME'
LEFT JOIN pp_oap_seller_bi_v.batch_loss_txn_base AS g
ON b.trans_id = g.payment_transid
) WITH DATA PRIMARY INDEX(trans_id);

-- time window analysis of bad_rate
CALL pp_monitor.drop_table('pp_oap_t1mm_t.t1mm_timewindow_pp');
CREATE TABLE pp_oap_t1mm_t.t1mm_timewindow_pp AS (
SELECT
EXTRACT(MONTH FROM pmt_start_date) AS onemonth
, seg
, SUM(wgt) AS total_unit
, SUM(dol_wgt) AS total_usd
, SUM(CASE WHEN is_bad=1 THEN wgt ELSE 0 END) AS bad_unit
, SUM(CASE WHEN is_bad=1 THEN dol_wgt ELSE 0 END) AS bad_usd
, (1.00000000*bad_unit / total_unit) AS unit_badrate
, (1.00000000*bad_usd / total_usd) AS dol_badrate
, SUM(wgt*pp_merch_gross_loss) AS gloss
, SUM(wgt*pp_merch_net_loss) AS nloss
, CAST(gloss AS DECIMAL(18,8)) / total_usd AS gloss_rate
, CAST(nloss AS DECIMAL(18,8)) / total_usd AS nloss_rate
FROM pp_oap_t1mm_t.t1_mm16_driver
GROUP BY 1, 2
) WITH DATA PRIMARY INDEX(seg);

--sampled driverset for union tagging
CALL pp_monitor.drop_table('pp_oap_t1mm_t.t1mm16_ebaypp_driver_smp');
CREATE TABLE pp_oap_t1mm_t.t1mm16_ebaypp_driver_smp AS(
SELECT a.*
, CASE WHEN t1_bad > 0 THEN 1
    WHEN seg = '02 SME' AND t1_bad = 0 THEN 78
    WHEN seg = '03 CS' AND t1_bad = 0 THEN 26
    WHEN seg = '04 YS' AND t1_bad = 0 THEN 3.5
    ELSE NULL END AS wgt
, a.usd_amt*wgt AS dol_wgt
FROM pp_scratch_risk.t1mm16_ebaypp_driver AS a
SAMPLE WHEN seg = '02 SME' AND t1_bad = 0 THEN 0.012820513
    WHEN seg = '03 CS' AND t1_bad = 0 THEN 0.038461538
    WHEN seg = '04 YS' AND t1_bad = 0 THEN 0.285714286
    ELSE 0.5,0.5 END
)WITH DATA PRIMARY INDEX(trans_id);