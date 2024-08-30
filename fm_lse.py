import argparse
import datetime
import logging
import json
from pyspark.sql.functions import col, when, trim, udf, struct, lit, isnull, isnan, coalesce, date_format, to_date, \
    to_timestamp, max, min, broadcast, substring, current_date,date_sub,trunc
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, StructType, Row, StructField, IntegerType, DecimalType, DateType, \
    TimestampType, LongType
from rftcari_pipeline.common import Task
from rftcari_pipeline.framework.kinesisUtil import KinesisUtil
from rftcari_pipeline.framework.writers.delta_writer import DeltaTableWriter, DeltaWriterConfig
import decimal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_fv(rate, nper, pmt, pv, pt):
    rate, nper, pmt, pv, pt=decimal.Decimal(rate), decimal.Decimal(nper), decimal.Decimal(pmt), decimal.Decimal(pv), decimal.Decimal(pt)
    var_c=decimal.Decimal(1.0)
    s = (var_c + rate) ** nper
    if rate==0:
        rate=1
    return ((pv * s) + pmt * (s - var_c) * (var_c + rate * pt) / rate)

def get_rate(p_periods, p_pmt, p_pv, p_fv, p_paymenttime, p_guess):
    p_fv,p_guess=decimal.Decimal(p_fv),decimal.Decimal(p_guess)
    v_next_rate = decimal.Decimal(p_guess)
    stepsilon = decimal.Decimal(0.00001)
    stmaxiterations = decimal.Decimal(100)
    stdelta = decimal.Decimal(0.00001)
    v_count = decimal.Decimal(0)
    v_rate = decimal.Decimal(v_next_rate)
    if p_guess == 0.0:
        v_next_rate = 0
        while (abs(v_next_rate - v_rate) < stepsilon) or (v_count > stmaxiterations):
            v_rate = v_next_rate
            if v_rate <= -12:
                v_rate = decimal.Decimal(-0.999 * 12)
            v_t = get_fv(v_rate, p_periods, p_pmt, p_pv, p_paymenttime) - p_fv
            v_dt = get_fv(v_rate + stdelta, p_periods, p_pmt, p_pv, p_paymenttime) - p_fv - v_t
            if v_dt == 0.0:
                v_count = stmaxiterations
            else:
                v_next_rate = v_rate - stdelta * v_t / v_dt
            v_count += 1
    return v_next_rate

get_rate_udf = udf(get_rate)


class FmLse(Task):
    def __init__(self, env=None):
        # self.env = env
        super().__init__()  # pragma: no cover
        self.kinesisPublish = KinesisUtil(params=None, env=env)

    def get_veh_model(self, df):
        return (df.withColumn('veh_model',
                              when(df.veh_code_alg.substr(1, 6).between('425115', '425116'), 'range rover evoque') \
                              .when(df.veh_code_alg.substr(1, 4) == 'sals', 'range rover sport').otherwise(
                                  trim(df.veh_model))) \
                .withColumn('veh_new_used_code', when(df.veh_new_used_code == 'd', 'u').otherwise(df.veh_new_used_code)) \
                .withColumn('q52_new_used_ind', when(df.q52_new_used_ind == 'd', 'u').otherwise(df.q52_new_used_ind)))

    def get_term(self, df):
        return df.withColumn('v_temp_term',
                             when(df.lse_mos_org <= 24, 24).when(df.lse_mos_org <= 36, 36).when(df.lse_mos_org <= 39,
                                                                                                39).when(
                                 df.lse_mos_org <= 42, 42) \
                             .when(df.lse_mos_org <= 48, 48).otherwise(60))

    def get_grade(self, df):
        return df.withColumn('v_grade', when(df.fico_score > 729, 'tier1').when(df.fico_score > 699, 'tier2').when(
            df.fico_score > 679, 'tier3').when(df.fico_score > 659, 'tier4').otherwise('tier5'))

    def get_veh_make(self, df):
        return df.withColumn('veh_make', when(df.make_id == 38, 'subaru').otherwise('othermake'))

    def get_subvened_ind(self, df):
        return df.withColumn("subvened_ind", when(df.lse_amt_subvention > 0, 1).otherwise(0))

    def get_pmt_depr(self, df):
        return df.withColumn("c_pmt_depr", (df.lse_amt_cap_cost - df.lse_amt_res_eot) / df.lse_mos_org)

    def get_buy_rate_pmt(self, df):
        return df.withColumn("c_buy_rate_pmt", ((df.lse_amt_cap_cost - df.lse_amt_res_eot) / df.lse_mos_org) + (
                (df.lse_amt_cap_cost + df.lse_amt_res_eot) / df.lse_buy_rate))


    def get_buy_rate(self, df):
        return df.withColumn("c_buy_rate",12 * get_rate_udf(df.lse_mos_org, df.c_buy_rate_pmt, -df.lse_amt_cap_cost,
                                                            df.lse_amt_res_eot, lit(1), lit(0)))

    def get_cust_rate_pmt(self, df):
        return df.withColumn("c_cust_rate_pmt", ((df.lse_amt_cap_cost - df.lse_amt_res_eot) / df.lse_mos_org) + (
                (df.lse_amt_cap_cost + df.lse_amt_res_eot) * df.lse_money_factor))

    def get_cust_rate(self, df):
        return df.withColumn("c_cust_rate", 12 * get_rate_udf(df.lse_mos_org, df.c_cust_rate_pmt, -df.lse_amt_cap_cost,
                                                              df.lse_amt_res_eot, lit(1), lit(0)))

    def get_progr_rate_pmt(self, df):
        return df.withColumn("c_progr_rate_pmt", when(df.subvened_ind == 1,
                                                      ((df.lse_amt_cap_cost - df.lse_amt_res_eot) / df.lse_mos_org) + ((
                                                                                                                               df.lse_amt_cap_cost + df.lse_amt_res_eot) * df.lse_dlr_markup_rate)).otherwise(
            0))

    def get_progr_rate(self, df):
        return df.withColumn("c_progr_rate",12 * get_rate_udf(df.lse_mos_org, df.c_progr_rate_pmt, -df.lse_amt_cap_cost,
                                                              df.lse_amt_res_eot, lit(1), lit(0)))

    def get_std_progr_rt_pmt(self, df):
        return df.withColumn("c_std_progr_rt_pmt", when(df.subvened_ind == 0, (
                (df.lse_amt_cap_cost - df.lse_amt_res_eot) / df.lse_mos_org) + ((
                                                                                        df.lse_amt_cap_cost + df.lse_amt_res_eot) * df.lse_dlr_markup_rate)).otherwise(
            0))

    def get_std_progr_rate(self, df):
        return df.withColumn("c_std_progr_rt",12 * get_rate_udf(df.lse_mos_org, df.c_std_progr_rt_pmt, -df.lse_amt_cap_cost,
                                                                df.lse_amt_res_eot, lit(1), lit(0))
                             )

    def get_lse_state(self, df):
        return df.withColumn("c_lse_state", when(df.lse_state == "01", "al").when(df.lse_state == "02", "ak").when(
            df.lse_state == "03", "az").when(df.lse_state == "04", "ar") \
                             .when(df.lse_state == "05", "ca").when(df.lse_state == "06", "co").when(
            df.lse_state == "07", "ct").when(df.lse_state == "08", "de").when(df.lse_state == "09", "dc") \
                             .when(df.lse_state == "10", "fl").when(df.lse_state == "11", "ga").when(
            df.lse_state == "12", "hi").when(df.lse_state == "13", "id").when(df.lse_state == "14", "il") \
                             .when(df.lse_state == "15", "in").when(df.lse_state == "16", "ia").when(
            df.lse_state == "17", "ks").when(df.lse_state == "18", "ky").when(df.lse_state == "19", "la") \
                             .when(df.lse_state == "20", "me").when(df.lse_state == "21", "md").when(
            df.lse_state == "22", "ma").when(df.lse_state == "23", "mi").when(df.lse_state == "24", "mn") \
                             .when(df.lse_state == "25", "ms").when(df.lse_state == "26", "mo").when(
            df.lse_state == "27", "mt").when(df.lse_state == "28", "ne").when(df.lse_state == "29", "nv") \
                             .when(df.lse_state == "30", "nh").when(df.lse_state == "31", "nj").when(
            df.lse_state == "32", "nm").when(df.lse_state == "33", "ny").when(df.lse_state == "34", "nc") \
                             .when(df.lse_state == "35", "nd").when(df.lse_state == "36", "oh").when(
            df.lse_state == "37", "ok").when(df.lse_state == "38", "or").when(df.lse_state == "39", "pa") \
                             .when(df.lse_state == "40", "ri").when(df.lse_state == "41", "sc").when(
            df.lse_state == "42", "sd").when(df.lse_state == "43", "tn").when(df.lse_state == "44", "tx") \
                             .when(df.lse_state == "45", "ut").when(df.lse_state == "46", "vt").when(
            df.lse_state == "47", "va").when(df.lse_state == "48", "wa").when(df.lse_state == "49", "wv") \
                             .when(df.lse_state == "50", "wi").when(df.lse_state == "51", "wy").when(
            df.lse_state == "70", "ca").when(df.lse_state == "72", "pr").when(df.lse_state == "78", "vi").otherwise(
            "xx"))

    def get_lse_age(self, df):
        df = df.withColumn('c_prodage', f.months_between(df.lse_booking_date, f.to_date('03/01/' + df.veh_yr)))
        return (df.withColumn('c_prodage',
                              when(df.c_prodage < -24, 'used5+').when(df.c_prodage <= 0, 'new').when(df.c_prodage <= 12,
                                                                                                     'used0').when(
                                  df.c_prodage <= 24, 'used1').when(df.c_prodage <= 36, 'used2').when(
                                  df.c_prodage <= 48,
                                  'used3').when(
                                  df.c_prodage <= 60, 'used4').when(df.c_prodage <= 72, 'used5+').otherwise('used5+')))

    def get_caf_ltv(self, df):
        df = df.withColumn('v_collat_val', when(df.p_msrp > 0, df.p_msrp).when(df.q99_inv_amt > 0, df.q99_inv_amt).when(
            df.q52_new_wholesale_amt > 0, df.q52_new_wholesale_amt).otherwise(lit(1)))
        return df.withColumn('c_booked_ltv', when(df.v_collat_val == 1, lit(999)).otherwise(
            (df.lse_amt_cap_cost / df.v_collat_val) * 100))

    def launch(self, txn_id, bus_dt, afsl_cals_dy_raw, ldb_dms_cals_d, afsl_acaps_dy_raw, acaps_auto_prv_lbl_d,
               dms_cals_m, cals_master_m, afsl_lkp_scrub_make_raw, afsl_lkp_make_hier_raw, p_lkup_scrub_model,
               lse_delinquent_rate_lkup, lse_collateral_rate_lkup, process_type):
        self.logger.info("*** launching proforma lease load task ***")  # pragma: no cover
        # afsl_cals_dy_raw=afsl_cals_dy_raw.withColumn('bus_dt_end',lit(bus_dt))
        # afsl_cals_dy_raw=afsl_cals_dy_raw.withColumn('bus_dt_end',date_format(to_date(col('bus_dt_end'),'yyyyMMdd'),'yyyy-MM-dd'))
        # afsl_cals_dy_raw=afsl_cals_dy_raw.withColumn('bus_dt_start',trunc('bus_dt_end','month'))
        # afsl_cals_dy_raw=afsl_cals_dy_raw.filter(afsl_cals_dy_raw.lse_booking_date.between(col('bus_dt_start'),col('bus_dt_end')))
        if process_type == 'daily':
            ldb_cals_master_d = afsl_cals_dy_raw.alias('t1').join(ldb_dms_cals_d.alias('t2'),
                                                                  ((col("t1.lse_nbr") == col("t2.lse_nbr")) & (col("t1.bus_mo").cast('int') == col("t2.bus_mo"))), 'left') \
                .selectExpr("t1.lse_nbr as acct_nbr", "t1.process_dt", "t1.acaps_dlr_nbr", "t1.chase_alg_residual",
                            "t1.chase_caf_residual", "t1.dealer_location_cd", "t1.fico_score",
                            "t1.fico_score_description",
                            "t1.lse_adj_msrp", "t1.lse_amt_cap_cost", "t1.lse_amt_markup", "t1.lse_amt_res_eot",
                            "t1.lse_amt_subvention", "t1.lse_booking_date", "t1.lse_buy_rate", "t1.lse_cust_nbr",
                            "t1.lse_dlr_markup_rate", "t1.lse_market_plan", "t1.lse_money_factor", "t1.lse_mos_org",
                            "t1.lse_pmt_base_rent", "t1.lse_pmt_depr", "t1.lse_rate_cust", "t1.lse_state",
                            "t1.lse_term_original", "t1.veh_cost", "t1.veh_modl_code_alg", "t1.veh_model",
                            "t1.veh_retail_price", "t1.veh_yr", "t1.veh_make_code_alg", "t1.lse_amt_idc2",
                            "t1.lse_cred_score", "t1.lse_level2", "t1.veh_new_used_code", "t1.veh_salvage_value",
                            "t1.sad_flat_fee_amt", "t1.veh_sale_type", "t1.veh_make", "t1.lse_cred_appl_key",
                            "t1.veh_ser_nbr", "t1.veh_depr_tot", "t1.lse_flag_term", "t1.lse_str_bal_tot",
                            "t1.dlr_location1", "t1.dlr_location2", "t1.lse_lrs_prg_rv_pct", "t2.caf_base_rv_amt",
                            "t2.caf_base_rv_pct", "t2.lse_rvrs_subv_amt", "t1.lse_date_lease", "t1.veh_code_alg",
                            "t1.lse_caf_mrm", "t1.contract_miles", "t1.veh_trim_code_alg", "t1.lse_dealer_name",
                            "t1.sad_acq_fee_rec_amt", "t1.lse_term_type", "t1.fico_score2", "t1.lse_amt_markup2",
                            "t1.plan_buy_rate", "t1.lse_code_acct_type", "t1.lse_new_fico_score",
                            "t1.lse_appl_nbr_related",
                            "t1.pool_enhance_pct_amt_2", "t1.lse_single_pay_flag", "t1.lse_rv_billable_amt",
                            "t1.lse_amt_dlr_part_upfront")
            fm_lse = ldb_cals_master_d.alias('t1').join(afsl_acaps_dy_raw.alias('t2'),
                                                        col("t1.lse_cred_appl_key") == col("t2.acp_appl_id"),
                                                        'left').join(acaps_auto_prv_lbl_d.alias('t3'),
                                                                     col("t1.lse_cred_appl_key") == col(
                                                                         "t3.application_id"), 'left').selectExpr(
                "acct_nbr", "acaps_dlr_nbr", "chase_alg_residual", "chase_caf_residual", "dealer_location_cd",
                "fico_score",
                "fico_score_description", "lse_adj_msrp", "lse_amt_cap_cost", "lse_amt_markup", "lse_amt_res_eot",
                "lse_amt_subvention", "lse_booking_date", "lse_buy_rate", "lse_cust_nbr", "lse_dlr_markup_rate",
                "lse_market_plan", "lse_money_factor", "lse_mos_org", "lse_pmt_base_rent", "lse_pmt_depr",
                "lse_rate_cust",
                "lse_state", "lse_term_original", "t1.veh_cost", "t1.veh_make_code_alg", "t1.veh_make",
                "t1.veh_modl_code_alg", "t1.veh_model", "veh_retail_price", "veh_yr", "sad_flat_fee_amt",
                "t1.veh_salvage_value", "lse_cred_score", "lse_amt_idc2", "0 as manuf_id", "veh_new_used_code",
                "veh_sale_type", "lse_cred_appl_key", "veh_ser_nbr", "lse_lrs_prg_rv_pct", "caf_base_rv_pct",
                "caf_base_rv_amt", "lse_rvrs_subv_amt as residual_value_subvent_amt", "lse_date_lease", "veh_code_alg",
                "lse_caf_mrm", "contract_miles", "veh_trim_code_alg",
                "lse_dealer_name", "t2.q52_new_invoice_amt", "t2.q52_new_wholesale_amt", "t2.q99_inv_amt",
                "coalesce(t3.pll_new_data_ind, 'n')  as loyalty_ind", "t2.acp_loan_to_income_ratio_pct",
                "t2.csc_2nd_pass_algorithm", "t2.q52_new_used_ind", "pool_enhance_pct_amt_2", "t1.lse_single_pay_flag",
                "t1.lse_rv_billable_amt", "t1.lse_amt_dlr_part_upfront", "t2.acp_appl_id")
        elif process_type == 'monthly':
            ldb_cals_master_m = afsl_cals_dy_raw.alias('t1').join(ldb_dms_cals_d.alias('t2'),
                                                                  ((col("t1.lse_nbr") == col("t2.lse_nbr")) & (col("t1.bus_mo").cast('int') == col("t2.bus_mo"))), 'left') \
                .selectExpr("t1.lse_nbr as acct_nbr", "t1.process_dt", "t1.acaps_dlr_nbr", "t1.chase_alg_residual",
                            "t1.chase_caf_residual", "t1.dealer_location_cd", "t1.fico_score",
                            "t1.fico_score_description",
                            "t1.lse_adj_msrp", "t1.lse_amt_cap_cost", "t1.lse_amt_markup", "t1.lse_amt_res_eot",
                            "t1.lse_amt_subvention", "t1.lse_booking_date", "t1.lse_buy_rate", "t1.lse_cust_nbr",
                            "t1.lse_dlr_markup_rate", "t1.lse_market_plan", "t1.lse_money_factor", "t1.lse_mos_org",
                            "t1.lse_pmt_base_rent", "t1.lse_pmt_depr", "t1.lse_rate_cust", "t1.lse_state",
                            "t1.lse_term_original", "t1.veh_cost", "t1.veh_modl_code_alg", "t1.veh_model",
                            "t1.veh_retail_price", "t1.veh_yr", "t1.veh_make_code_alg", "t1.lse_amt_idc2",
                            "t1.lse_cred_score", "t1.lse_level2", "t1.veh_new_used_code", "t1.veh_salvage_value",
                            "t1.sad_flat_fee_amt", "t1.veh_sale_type", "t1.veh_make", "t1.lse_cred_appl_key",
                            "t1.veh_ser_nbr", "t1.veh_depr_tot", "t1.lse_flag_term", "t1.lse_str_bal_tot",
                            "t1.dlr_location1", "t1.dlr_location2", "t1.lse_lrs_prg_rv_pct", "t2.caf_base_rv_amt",
                            "t2.caf_base_rv_pct", "t2.lse_rvrs_subv_amt", "t1.lse_date_lease", "t1.veh_code_alg",
                            "t1.lse_caf_mrm", "t1.contract_miles", "t1.veh_trim_code_alg", "t1.lse_dealer_name",
                            "t1.sad_acq_fee_rec_amt", "t1.lse_term_type", "t1.fico_score2", "t1.lse_amt_markup2",
                            "t1.plan_buy_rate", "t1.lse_code_acct_type", "t1.lse_new_fico_score",
                            "t1.lse_appl_nbr_related",
                            "t1.pool_enhance_pct_amt_2", "t1.lse_single_pay_flag", "t1.lse_rv_billable_amt",
                            "t1.lse_amt_dlr_part_upfront")
            fm_lse = ldb_cals_master_m.alias('t1').join(afsl_acaps_dy_raw.alias('t2'),
                                                        col("t1.lse_cred_appl_key") == col("t2.acp_appl_id"),
                                                        'left').join(acaps_auto_prv_lbl_d.alias('t3'),
                                                                     col("t1.lse_cred_appl_key") == col(
                                                                         "t3.application_id"), 'left').selectExpr(
                "acct_nbr", "acaps_dlr_nbr", "chase_alg_residual", "chase_caf_residual", "dealer_location_cd",
                "fico_score",
                "fico_score_description", "lse_adj_msrp", "lse_amt_cap_cost", "lse_amt_markup", "lse_amt_res_eot",
                "lse_amt_subvention", "lse_booking_date", "lse_buy_rate", "lse_cust_nbr", "lse_dlr_markup_rate",
                "lse_market_plan", "lse_money_factor", "lse_mos_org", "lse_pmt_base_rent", "lse_pmt_depr",
                "lse_rate_cust",
                "lse_state", "lse_term_original", "t1.veh_cost", "t1.veh_make_code_alg", "t1.veh_make",
                "t1.veh_modl_code_alg", "t1.veh_model", "veh_retail_price", "veh_yr", "sad_flat_fee_amt",
                "t1.veh_salvage_value", "lse_cred_score", "lse_amt_idc2", "0 as manuf_id", "veh_new_used_code",
                "veh_sale_type", "lse_cred_appl_key", "veh_ser_nbr", "lse_lrs_prg_rv_pct", "caf_base_rv_pct",
                "caf_base_rv_amt", "lse_rvrs_subv_amt as residual_value_subvent_amt", "lse_date_lease", "veh_code_alg",
                "lse_caf_mrm", "contract_miles", "veh_trim_code_alg",
                "lse_dealer_name", "t2.q52_new_invoice_amt", "t2.q52_new_wholesale_amt", "t2.q99_inv_amt",
                "coalesce(t3.pll_new_data_ind, 'n')  as loyalty_ind", "t2.acp_loan_to_income_ratio_pct",
                "t2.csc_2nd_pass_algorithm", "t2.q52_new_used_ind", "pool_enhance_pct_amt_2", "t1.lse_single_pay_flag",
                "t1.lse_rv_billable_amt", "t1.lse_amt_dlr_part_upfront", "t2.acp_appl_id")
        # fm_lse.select(max(fm_lse.lse_booking_date),min(fm_lse.lse_booking_date)).show()
        print(f"fm lse count {fm_lse.count()}")
        curr_date = datetime.datetime.strptime(bus_dt, '%Y%m%d')
        v_start_time = datetime.datetime.strptime(bus_dt, '%Y%m%d') + datetime.timedelta(days=-1)
        month_start_date = v_start_time.replace(day=1)
        fm_lse = fm_lse.transform(self.get_veh_model)
        print(f'fm lse count {fm_lse.count()}')
        v_prior_month_end_dt = month_start_date - datetime.timedelta(days=1)
        v_lkup_make = afsl_lkp_scrub_make_raw.alias("t1").join(afsl_lkp_make_hier_raw.alias("t2"),
                                                               col("t1.make_id") == col("t2.make_id"),
                                                               "left").selectExpr("t1.misspelled_make", "t2.make_id",
                                                                                  "t2.make", "t2.manufacturer",
                                                                                  "t2.regional_parent", "t2.make_abbr",
                                                                                  "t2.make_cpe", "t2.make_g",
                                                                                  "t2.make_r", "t2.cals_make")
        print(f'after afsl_lkp_scrub_make_raw count {fm_lse.count()}')
        fm_lse = fm_lse.withColumn('run_yr_mo', substring(lit(bus_dt), 1, 6).cast(IntegerType()))
        fm_lse = fm_lse.alias('t1').join(broadcast(v_lkup_make).alias('t2'), (
                (v_lkup_make.misspelled_make == fm_lse.veh_make_code_alg) | (
                v_lkup_make.misspelled_make == fm_lse.veh_make)), "left").selectExpr('t1.*','t2.make_id')
        fm_lse.dropDuplicates(['acct_nbr'])
        print(f'after v_lkup_make count {fm_lse.count()}')
        fm_lse = fm_lse.alias('t1').join(p_lkup_scrub_model.alias('t2'), (
                (p_lkup_scrub_model.make_id == fm_lse.make_id) & (
                p_lkup_scrub_model.misspelled_model == fm_lse.veh_model)), 'left').selectExpr('t1.*','t2.correct_model as c_model')
        print(f'after p_lkup_scrub_model count {fm_lse.count()}')
        fm_lse = fm_lse.transform(self.get_subvened_ind) \
            .transform(self.get_term) \
            .transform(self.get_grade) \
            .transform(self.get_veh_make)
        print(f'after subvened term grade make count {fm_lse.count()}')
        fm_lse = fm_lse.alias('t1').join(lse_delinquent_rate_lkup.alias('t2'), (
                (lse_delinquent_rate_lkup.veh_make == fm_lse.veh_make) & (
                lse_delinquent_rate_lkup.term == fm_lse.v_temp_term) & (
                        lse_delinquent_rate_lkup.grade == fm_lse.v_grade)), 'left').select('t1.*','t2.delinquent_rate')
        print(f'after delinquent rate count {fm_lse.count()}')
        fm_lse = fm_lse.alias('t1').join(lse_collateral_rate_lkup.alias('t2'),
                                         (col("t1.manuf_id") == col("t2.manuf_id")), 'left').selectExpr('t1.*','t2.collateral_rate')
        fm_lse.fillna(value=0, subset=['q52_new_invoice_amt'])
        print(f'after lse collateral rate count {fm_lse.count()}')
        fm_lse = fm_lse.withColumn("p_msrp", when(fm_lse.q52_new_invoice_amt > 0, fm_lse.q52_new_invoice_amt).otherwise(
            fm_lse.lse_adj_msrp))
        fm_lse = fm_lse.transform(self.get_caf_ltv).transform(self.get_lse_age)
        fm_lse = (fm_lse.transform(self.get_pmt_depr) \
                  .transform(self.get_buy_rate_pmt) \
                  .transform(self.get_buy_rate) \
                  .transform(self.get_cust_rate_pmt) \
                  .transform(self.get_cust_rate) \
                  .transform(self.get_progr_rate_pmt) \
                  .transform(self.get_progr_rate) \
                  .transform(self.get_lse_state) \
                  .transform(self.get_std_progr_rt_pmt) \
                  .transform(self.get_std_progr_rate))
        print(f'after many tansformation count {fm_lse.count()}')
        fm_lse = fm_lse.withColumn("edit_dt", lit(curr_date)).withColumn('sad_rebate_amt', lit(0))
        fm_lse = fm_lse.withColumn('chase_alg_residual', fm_lse.chase_alg_residual.cast('decimal(38,12)'))
        fm_lse = fm_lse.withColumn('__txn_id_long', lit(txn_id)).withColumn('__start_at', current_date()).withColumn(
            '__end_at', current_date())
        fm_lse=fm_lse.withColumn('bus_dt_end',lit(bus_dt))
        fm_lse=fm_lse.withColumn('bus_dt_end',date_format(to_date(col('bus_dt_end'),'yyyyMMdd'),'yyyy-MM-dd'))
        fm_lse=fm_lse.withColumn('bus_dt_start',trunc('bus_dt_end','month'))
        fm_lse=fm_lse.filter(fm_lse.lse_booking_date.between(col('bus_dt_start'),col('bus_dt_end')))

        fm_lse = fm_lse.select('run_yr_mo', 'acct_nbr', 'acaps_dlr_nbr', 'chase_alg_residual', 'chase_caf_residual',
                               'dealer_location_cd', 'fico_score', 'fico_score_description', 'lse_adj_msrp',
                               'lse_amt_cap_cost', 'lse_amt_markup', 'lse_amt_res_eot', 'lse_amt_subvention',
                               'lse_booking_date', 'lse_buy_rate', 'lse_cust_nbr', 'lse_dlr_markup_rate',
                               'lse_market_plan', 'lse_money_factor', 'lse_mos_org', 'lse_pmt_base_rent',
                               'lse_pmt_depr', 'lse_rate_cust', 'lse_state', 'lse_term_original', 'veh_cost', 'make_id',
                               'veh_model', 'veh_retail_price', 'veh_yr', 'subvened_ind', 'edit_dt', 'c_buy_rate',
                               'sad_flat_fee_amt', 'c_booked_ltv', 'lse_cred_score', 'lse_amt_idc2', 'manuf_id',
                               'c_model', 'veh_new_used_code', 'c_prodage', 'veh_sale_type', 'c_lse_state',
                               'c_buy_rate_pmt', 'lse_cred_appl_key', 'c_cust_rate', 'c_cust_rate_pmt', 'c_progr_rate',
                               'c_progr_rate_pmt', 'c_pmt_depr', 'lse_lrs_prg_rv_pct', 'delinquent_rate',
                               'collateral_rate', 'caf_base_rv_pct', 'caf_base_rv_amt', 'residual_value_subvent_amt',
                               'lse_date_lease', 'veh_code_alg', 'lse_caf_mrm', 'contract_miles', 'veh_trim_code_alg',
                               'lse_dealer_name', 'loyalty_ind', 'acp_loan_to_income_ratio_pct',
                               'csc_2nd_pass_algorithm', 'pool_enhance_pct_amt_2', 'lse_single_pay_flag',
                               'lse_rv_billable_amt', 'lse_amt_dlr_part_upfront', 'c_std_progr_rt',
                               'c_std_progr_rt_pmt', 'sad_rebate_amt', '__txn_id_long', '__start_at', '__end_at')
        events = {"fm_lse.count()": fm_lse.count()}
        logger.info(f"fm_lse load update events: {events}")
        response = self.kinesisPublish.events_publish(events)
        logger.info(f"fm lse load event completed with response - {response}")
        return fm_lse


def get_arguments():  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument("--job_params", default=None, required=False, help="Please provide valid job params")
    return parser.parse_known_args()


# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    args = get_arguments()
    json_string = (args[0].job_params).replace("'", '"')
    print("json parameter string = ", json_string)
    params = json.loads(json_string)
    txn_id = params['txn_id']
    bus_dt = params['bus_dt']
    bus_mo=bus_dt[:6]
    catalog_name = params['catalog_name']
    env = params['env']
    process_type = params['process_type']
    print(f"Arguments values catalog name: {catalog_name},env: {env},process_type: {process_type}")
    task = FmLse(env)
    afsl_cals_dy_raw = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_retail_schema.afsl_cals_dy_raw")
    afsl_cals_dy_raw=afsl_cals_dy_raw.filter(col('bus_mo')==bus_mo).withColumn('max_date', max('day_id').over(Window.partitionBy(col('bus_mo')))).filter(col('day_id')==col('max_date')).drop('max_date')
    ldb_dms_cals_d = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_retail_schema.ldb_dms_cals_d")
    ldb_dms_cals_d=ldb_dms_cals_d.filter(col('bus_mo')==bus_mo).withColumn('max_date', max('day_id').over(Window.partitionBy(col('bus_mo')))).filter(col('day_id')==col('max_date')).drop('max_date')
    afsl_acaps_dy_raw = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_retail_schema.afsl_acaps_dy_raw")
    afsl_acaps_dy_raw=afsl_acaps_dy_raw.filter(col('bus_mo')==bus_mo).withColumn('max_date', max('day_id').over(Window.partitionBy(col('bus_mo')))).filter(col('day_id')==col('max_date')).drop('max_date')
    acaps_auto_prv_lbl_d = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_retail_schema.acaps_auto_prv_lbl_d")
    acaps_auto_prv_lbl_d=acaps_auto_prv_lbl_d.filter(col('bus_mo')==bus_mo).withColumn('max_date', max('day_id').over(Window.partitionBy(col('bus_mo')))).filter(col('day_id')==col('max_date')).drop('max_date')
    dms_cals_m = task.spark.read.format('delta').table(f"{catalog_name}.fdi_trusted_auto_retail_schema.dms_cals_m")
    cals_master_m = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_retail_schema.cals_master_m")
    afsl_lkp_scrub_make_raw = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_retail_schema.afsl_lkp_scrub_make_raw")
    afsl_lkp_make_hier_raw = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_retail_schema.afsl_lkp_make_hier_raw")
    p_lkup_scrub_model = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.p_lkup_scrub_model")
    lse_delinquent_rate_lkup = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lse_delinquent_rate_lkup")
    lse_collateral_rate_lkup = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lse_collateral_rate_lkup")
    opt_df = task.launch(txn_id, bus_dt, afsl_cals_dy_raw, ldb_dms_cals_d, afsl_acaps_dy_raw, acaps_auto_prv_lbl_d,
                         dms_cals_m, cals_master_m, afsl_lkp_scrub_make_raw, afsl_lkp_make_hier_raw, p_lkup_scrub_model,
                         lse_delinquent_rate_lkup, lse_collateral_rate_lkup, process_type)
    delta_fm_lse = f"{catalog_name}.fdi_refined_enrich_auto_proforma_schema.fm_lse"
    delta_fm_lse_df = task.spark.read.format('delta').table(delta_fm_lse)
    opt_df = opt_df.select([col(c).cast(
        delta_fm_lse_df.select(c.lower()).schema[0].dataType) if c.lower() in opt_df.columns else c
                            for c in delta_fm_lse_df.columns])
    opt_df=opt_df.repartition(20)
    opt_df.write.mode("overwrite").option("partitionOverwriteMode", "dynamic").saveAsTable(delta_fm_lse)


# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == "__main__":
    entrypoint()
