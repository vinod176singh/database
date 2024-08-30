import argparse
import datetime
import logging
import json
from pyspark.sql.functions import col, when, trim, udf, struct, lit, isnull, isnan, coalesce, date_format, to_date, \
    to_timestamp, max, min, year, cast, current_date, substring, concat,length
from pyspark.sql import functions as f
from datetime import datetime
import decimal
from pyspark.sql.types import StringType, StructType, Row, StructField,IntegerType
from rftcari_pipeline.common import Task
from rftcari_dbx_pipeline.tasks.loanStgPrep import loadReltab,get_als_d,loadAcapsDaily,get_als_m
from .loanUtils import getAnr
from rftcari_pipeline.framework.kinesisUtil import KinesisUtil
from rftcari_pipeline.framework.writers.delta_writer import DeltaTableWriter, DeltaWriterConfig
from rftcari_dbx_pipeline.tasks.proforma_ln_ls import proforma_ln_ls_final_load

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_lse_cof_lp_anrr(p_term, p_abs, p_rv_amt, p_cap_cost, p_cof_curve, p_lp_curve, p_cf_curve,wacc):
    period, p_wacc, v_run_bal, v_run_decl_bal_d, run_res_val_wt = 1, wacc, 0, 0, 0
    # print(f"udf parameter values are {p_term},{p_abs},{p_rv_amt},{p_cap_cost},{p_cof_curve},{p_lp_curve},{p_cf_curve}")
    if p_term == 0:
        p_term = 1
    v_deprec_amt = p_cap_cost - p_rv_amt
    v_s1_time_zero = (v_deprec_amt / p_term)
    v_numer_avg_outst = 0
    v_run_res_val_wt, v_cof_run, v_run_accum_int, v_lp_run, v_cf_run, v_run_decl_bal = 0, 0, 1, 0, 0, 0
    cof_curve,lp_curve,cf_curve=[],[],[]
    if type(p_cof_curve)!=type(None):
        cof_curve=p_cof_curve.split('|')
    if type(p_lp_curve)!=type(None):
        lp_curve = p_lp_curve.split('|')
    if type(p_cf_curve)!=type(None):
        cf_curve=p_cf_curve.split('|')
    # print(len(cof_curve),len(lp_curve),len(cf_curve))
    itr = 0
    if (type(p_cof_curve)!=type(None)) & (type(p_lp_curve)!=type(None)) & (type(p_cf_curve)!=type(None)):
        while p_term > period:
            # print('Enter while ',period,v_s1_time_zero)
            v_abs_factor = p_abs * (period - 1)
            deprec = v_s1_time_zero - (v_s1_time_zero * (v_abs_factor))
            deprec_prepay = (v_deprec_amt - deprec - v_run_bal) * (p_abs / (1 - v_abs_factor))
            total_deprec = deprec + deprec_prepay
            v_run_bal = v_run_bal + total_deprec
            if period < p_term:
                v_res_val_wt = (p_rv_amt - run_res_val_wt) * (p_abs / (1 - p_abs * (period - 1)))
            else:
                v_res_val_wt = p_rv_amt - v_run_res_val_wt
            v_run_res_val_wt = v_run_res_val_wt + v_res_val_wt

            res_val_wt = v_res_val_wt
            run_res_val_wt = v_run_res_val_wt
            if period <= p_term:
                decl_bal = p_cap_cost - v_run_bal - v_run_res_val_wt
            else:
                decl_bal = 0
            v_numer_avg_outst = v_numer_avg_outst + (decl_bal * period)
            v_run_decl_bal = v_run_decl_bal + decl_bal
            decl_bal_d = decl_bal * (1 / ((1 + (p_wacc / 12)) ** period))
            v_run_decl_bal_d = v_run_decl_bal_d + decl_bal_d
            dep_plus_rv = total_deprec + res_val_wt
            accum_int = dep_plus_rv * period
            v_run_accum_int = v_run_accum_int + accum_int
            if len(cof_curve)>0:
                cof_period = decimal.Decimal((total_deprec + res_val_wt) * period) * (decimal.Decimal(cof_curve[itr]) / 12)
            v_cof_run = v_cof_run + cof_period
            if len(lp_curve)>0:
                lp_period = decimal.Decimal((total_deprec + res_val_wt) * period) * (decimal.Decimal(lp_curve[itr]) / 12)
                v_lp_run = v_lp_run + lp_period
            if len(cf_curve)>0:
                cf_period = decimal.Decimal((total_deprec + res_val_wt) * period) * (decimal.Decimal(cf_curve[itr]) / 12)
                v_cf_run = v_cf_run + cf_period
            period = period + 1
            itr = itr + 1
            if itr>83:
                itr=0
    cof = v_cof_run / decimal.Decimal(v_run_accum_int) * 12
    lp = v_lp_run / decimal.Decimal(v_run_accum_int) * 12
    o_cf = (v_cf_run / decimal.Decimal(v_run_accum_int) * 12)
    anr = (v_run_decl_bal / 12)
    anrdisc = (v_run_decl_bal_d / 12)
    o_avg_outst_perc_bkd = 0
    # print(f"udf calculated values are {cof},{lp},{o_cf},{anr},{anrdisc},{anrdisc},{o_avg_outst_perc_bkd}")
    return Row('cof', 'lp', 'o_cf', 'o_fe', 'anr', 'anrdisc', 'o_avg_outst_perc_bkd')(cof, lp, o_cf, '', anr,anrdisc, o_avg_outst_perc_bkd)


cof_schema = StructType([StructField("cof",
                                     StringType(), False),
                         StructField("lp",
                                     StringType(), False),
                         StructField("o_cf",
                                     StringType(), False),
                         StructField("o_fe",
                                     StringType(), False),
                         StructField("anr",
                                     StringType(), False),
                         StructField("anrdisc",
                                     StringType(), False),
                         StructField("o_avg_outst_perc_bkd",
                                     StringType(), False)])

get_lse_cof_lp_anrr_udf = udf(get_lse_cof_lp_anrr, cof_schema)


class FmLseProfit(Task):
    def __init__(self, env=None):
        self.env = env
        super().__init__()  # pragma: no cover
        self.kinesisPublish = KinesisUtil(params=None, env=env)

    # def error_message(self, df):
    #     df = df.withColumn('1 as v_err_txt', when(isnan(df.acct_nbr) | isnull(df.acct_nbr),
    #                                               'application id is null.')
    #                        .when(
    #         isnan(df.dealer_location_cd) | isnull(df.dealer_location_cd),
    #         'dealer location code is null.')
    #                        .when(isnan(df.fico_score) | isnull(df.fico_score),
    #                              "fico is null")
    #                        .when(
    #         isnan(df.lse_booking_date) | isnull(df.lse_booking_date), "p_bk_dt is null.")
    #                        .when(
    #         isnan(df.lse_amt_cap_cost) | isnull(df.lse_amt_cap_cost), "cap cost is null.")
    #                        .when(isnan(df.lse_amt_markup) | isnull(df.lse_amt_markup),
    #                              "p_rsve_amt is null")
    #                        .when(isnan(df.lse_amt_reserve) | isnull(df.lse_amt_reserve),
    #                              "p_rv_amt is null.")
    #                        .when(
    #         isnan(df.lse_amt_subvention) | isnull(df.lse_amt_subvention), 'p_subv_amt is null.')
    #                        .when(
    #         isnan(df.dealer_flats) | isnull(df.dealer_flats), "p_flat_amt is null.")
    #                        .when(isnan(df.lse_mos_org) | isnull(df.lse_mos_org),
    #                              "p_mos_org is null.")
    #                        .when(isnan(df.lse_pmt_depr) | isnull(df.lse_pmt_depr),
    #                              "p_pmt_depr is null")
    #                        .when(isnan(df.lse_buy_rate) | isnull(df.lse_buy_rate),
    #                              "p_rt_buy is null")
    #                        .when(isnan(df.lse_rate_cust) | isnull(df.lse_rate_cust),
    #                              "p_rt_cust is null")
    #                        .when(isnan(df.manuf_id) | isnull(df.manuf_id),
    #                              "p_manuf_id is null")
    #                        .when(isnan(df.make_id) | isnull(df.make_id),
    #                              "p_make_id is null")
    #                        .when(isnan(df.lse_amt_markup) | isnull(df.lse_amt_markup),
    #                              "p_acq_fee is null.")
    #                        .otherwise("null"))

    def get_corp_tax_rate(self, df):
        return df.withColumn('tax_rate', when(col("lse_booking_date") >= '2015-01-01', 0.4035) \
                             .when(col("lse_booking_date") >= '2011-01-01', 0.4035) \
                             .when(col("lse_booking_date") >= '2010-01-01', 0.4275) \
                             .when(col("lse_booking_date") >= '2009-12-01', 0.4075) \
                             .when(col("lse_booking_date") >= '2006-01-01', 0.395) \
                             .otherwise(0.39))

    def get_lse_age(self, df):
        df = df.withColumn('prodage', f.months_between(df.lse_booking_date, f.to_date('03/01/' + df.veh_yr)))
        return (df.withColumn('prodage',
                              when(df.prodage < -24, 'USED5+').when(df.prodage <= 0, 'NEW').when(df.prodage <= 12,
                                                                                                 'USED0').when(
                                  df.prodage <= 24, 'USED1').when(df.prodage <= 36, 'USED2').when(df.prodage <= 48,
                                                                                                  'USED3').when(
                                  df.prodage <= 60, 'USED4').when(df.prodage <= 72, 'USED5+').otherwise('USED5+')))

    def get_lse_dlc_region(self, df):
        return df.withColumn('c_region',
                             when(df.dealer_location_cd == '191090', 'VM').when(df.dealer_location_cd == '192010',
                                                                                'RV').when(
                                 df.dealer_location_cd == '191080', 'ML').when(df.dealer_location_cd == '000000',
                                                                               'SW').when(
                                 df.dealer_location_cd == '101020', 'SE').when(df.dealer_location_cd == '191010',
                                                                               'SU').when(
                                 df.dealer_location_cd == '101515', 'WE').when(df.dealer_location_cd == '102035',
                                                                               'SW').when(
                                 df.dealer_location_cd == '191030', 'LR').when(df.dealer_location_cd == '102030',
                                                                               'MW').when(
                                 df.dealer_location_cd == '102040', 'WE').when(df.dealer_location_cd == '101010',
                                                                               'NE').when(
                                 df.dealer_location_cd == '191020', 'JA').when(df.dealer_location_cd == '191040',
                                                                               'MZ').when(
                                 df.dealer_location_cd == '191050', 'EN').when(df.dealer_location_cd == '191070',
                                                                               'AM').when(
                                 df.dealer_location_cd == '191060', 'MA'))

    def assign_abs_speed(self, df):
        return df.withColumn('abs_speed',
                             when(df.lse_mos_org <= 24, 0.030).when(df.lse_mos_org <= 48, 0.013).otherwise(0.015))

    def get_term(self, df):
        return df.withColumn('v_temp_term',
                             when(df.lse_mos_org <= 24, 24).when(df.lse_mos_org <= 36, 36).when(df.lse_mos_org <= 39,
                                                                                                39).when(
                                 df.lse_mos_org <= 42, 42) \
                             .when(df.lse_mos_org <= 48, 48).otherwise(60))

    def get_lkup_rym_lob_product(self, df, fm_manuf_lse_lkup, lkup_rym_lob_product, p_dim_key, p_status):
        # df=df.withColumn('run_yr_mo',to_date(col('run_yr_mo')))
        # df=df.withColumn('run_yr_mo',f.to_date(when(length(df.run_yr_mo)==6,concat(df.run_yr_mo,lit('01'))).otherwise(df.run_yr_mo),'yyyyMMdd'))
        fm_manuf_lse_lkup=fm_manuf_lse_lkup.withColumn('rym_beg',date_format('rym_beg','yyyyMM')).withColumn('rym_end',date_format('rym_end','yyyyMM')).withColumn('rym_beg',col('rym_beg').cast('int')).withColumn('rym_end',col('rym_end').cast('int'))
        df = df.alias('t1').join(fm_manuf_lse_lkup.alias('t2'), ((col('t1.manuf_id') == col('t2.manuf_id')) & (
            df.run_yr_mo.between(fm_manuf_lse_lkup.rym_beg, fm_manuf_lse_lkup.rym_end))), 'left').select('t1.*',
                                                                                                         't2.manuf_short')
        df.show()
        fm_manuf_lse_lkup.show()
        lkup_rym_lob_product.show()
        df = df.alias('t1').join(lkup_rym_lob_product.alias('t2'), (
                (concat(df.lob, df.manuf_short) == lkup_rym_lob_product.lob) & (
            df.run_yr_mo.between(lkup_rym_lob_product.rym_beg, lkup_rym_lob_product.rym_end)) & (
                        lkup_rym_lob_product.dim_key == p_dim_key) & (lkup_rym_lob_product.status == p_status)),
                                 'left').select('t1.*', 't2.measure_val')
        df = df.drop('manuf_short')
        return df

    # def get_loss_attributes(self,df1,lkup_loss_override,lookup_var,lookup,val):
    #     return df1.alias('t1').join(lkup_loss_override.alias('t2'), ((col('t2.ln_ls') == 'LSE') & (
    #         df1.lse_date_lease.between(f.to_date(lkup_loss_override.rym_beg),
    #                                               f.to_date(lkup_loss_override.rym_end))) & (
    #                                                                                               col('t2.lookup_var') == lookup_var) & (
    #                                                                                               col('t2.lookup') == lookup) & (
    #                                                                                           lit(val).between(
    #                                                                                               col('t2.min_val'),
    #                                                                                               ('t2.max_val')))),
    #                                                      'left').select('t1.*', 't2.override')

    def get_loss_attributes(self,df1,lkup_loss_override, p_lookup, p_lookup_var, p_lookup_val,optCol):
        df1.createOrReplaceTempView("inputDf_view")
        lkup_loss_override.createOrReplaceTempView("lkup_loss_override")
        outdf0 = self.spark.sql(f"""select mst.*, nvl(ovr.override,{p_lookup_val}) as {optCol}
                        from inputDf_view mst left outer join lkup_loss_override ovr
                            on mst.ln_lse = ovr.ln_ls and mst.booked_rym between ovr.rym_beg and ovr.rym_end
                                and '{p_lookup}' = ovr.lookup and '{p_lookup_var}' = ovr.lookup_var
                                and {p_lookup_val} between ovr.min_val and ovr.max_val""")
        return outdf0
    def launch(self, txn_id, bus_dt, fm_lse, lse_stage, lkup_rym_lob_product, lse_rym_based_drivers, fm_manuf_lse_lkup,
               lse_capital_abs, lse_return_rate_lkup, cof_xref, sofr_cof, lp_cross_funding_credit, cof,
               manuf_lob_mgmt_fee, lp_credit_adj, lse_expenses_3, lse_expense_rym, lse_losses,
               ldb_acaps_acct_relship_tab, lse_expenses_marginal, lkup_loss_dealer_adjustor,
               lkup_loss_loyalty_adjustor_f9, lkup_loss_portfolio_adj_f9, lkup_loss_dynamic_adj_2_f9,
               lkup_loss_macro_adjustor_f9, lkup_loss_cd1_adjustor, lkup_loss_cd2_adjustor, lkup_loss_override,lp_cof):
        self.logger.info("*** launching cari mb locks rnzn hfi lease load task ***")  # pragma: no cover
        # fm_manuf_lse_lkup=fm_manuf_lse_lkup.withColumn('rym_beg',f.to_date(when(length(fm_manuf_lse_lkup.rym_beg)==6,concat(fm_manuf_lse_lkup.rym_beg,lit('01'))).otherwise(fm_manuf_lse_lkup.rym_beg),'yyyyMMdd')).withColumn('rym_end',f.to_date(when(length(fm_manuf_lse_lkup.rym_end)==6,concat(fm_manuf_lse_lkup.rym_end,lit('01'))).otherwise(fm_manuf_lse_lkup.rym_end),'yyyyMMdd'))
        # lkup_rym_lob_product=lkup_rym_lob_product.withColumn('rym_beg',f.to_date(when(length(lkup_rym_lob_product.rym_beg)==6,concat(lkup_rym_lob_product.rym_beg,lit('01'))).otherwise(lkup_rym_lob_product.rym_beg),'yyyyMMdd')).withColumn('rym_end',f.to_date(when(length(lkup_rym_lob_product.rym_end)==6,concat(lkup_rym_lob_product.rym_end,lit('01'))).otherwise(lkup_rym_lob_product.rym_end),'yyyyMMdd'))
        lse_profit_opt = fm_lse.transform(self.get_corp_tax_rate)
        lse_profit_opt = lse_profit_opt.select([col(c.lower()) for c in lse_profit_opt.columns])
        lse_profit_opt = lse_profit_opt.alias('t1').join(lse_stage.alias('t2'),(lse_profit_opt.acct_nbr==lse_stage.acct_nbr),'left').select('t1.*','t2.new_used_ind','t2.grade')
        lse_profit_opt = lse_profit_opt.withColumn('v_drive_dt', coalesce(lse_profit_opt.lse_date_lease,
                                                                          lse_profit_opt.lse_booking_date))
        lse_profit_opt = lse_profit_opt.withColumn('acp_appl_id', col('lse_cred_appl_key')).withColumn('sad_rebate_amt',
                                                                                                       lit(0))
        ldb_acaps_acct_relship_tab = ldb_acaps_acct_relship_tab.select([col(c.lower()) for c in ldb_acaps_acct_relship_tab.columns])
        lse_profit_opt = lse_profit_opt.alias('t1').join(lkup_rym_lob_product, (
                (lkup_rym_lob_product.lob == 'LSE') & (lkup_rym_lob_product.dim_key == 'SENDEBT') & (
            lse_profit_opt.lse_date_lease.between(f.to_date(lkup_rym_lob_product.rym_beg),
                                                  f.to_date(lkup_rym_lob_product.rym_end)))), 'left').selectExpr('t1.*',
                                                                                                                 'measure_val').withColumnRenamed(
            'measure_val', 'sr_dr_amt').withColumnRenamed('residual_value_subvent_amt',
                                                          'residual_value').withColumnRenamed('lse_amt_res_eot',
                                                                                              'lse_amt_reserve').withColumnRenamed(
            'c_buy_rate', 'c_buy_rt').withColumnRenamed('veh_new_used_code', 'new_used_code').withColumnRenamed(
            'lse_dlr_markup_rate', 'c_dealer_markup').withColumnRenamed('sad_flat_fee_amt', 'dealer_flats')
        print(f"lkup_rym_lob_product {lse_profit_opt.count()}")
        lse_profit_opt = lse_profit_opt.alias("t1").join(lkup_rym_lob_product.alias('t2'), (
                (lkup_rym_lob_product.lob == 'LSE') & (lkup_rym_lob_product.dim_key == 'SENDEBT') & (
            lse_profit_opt.lse_date_lease.between(f.to_date(lkup_rym_lob_product.rym_beg),
                                                  f.to_date(lkup_rym_lob_product.rym_end)))), 'left').selectExpr('t1.*',
                                                                                                                 'measure_val').withColumnRenamed(
            'measure_val', 'sbrd_dr_amt').withColumn('prepayment_speed_ind', lit(1))
        print(f"lkup_rym_lob_product {lse_profit_opt.count()}")
        lse_profit_opt = lse_profit_opt.alias('t1').join(lse_rym_based_drivers.alias('t2'), (
                (lse_rym_based_drivers.metric_label == 'L1M') & (
                to_timestamp(lse_rym_based_drivers.run_yr_mo) == to_timestamp(lse_profit_opt.lse_booking_date))),
                                                         'left').select('t1.*', 'metric_value').withColumnRenamed(
            'metric_value', 'libor_1month')
        print(f"lse_rym_based_drivers {lse_profit_opt.count()}")
        lse_profit_opt = lse_profit_opt.alias('t1').join(lse_rym_based_drivers.alias('t2'), (
                (lse_rym_based_drivers.metric_label == 'L24') & (
                to_timestamp(lse_rym_based_drivers.run_yr_mo) == to_timestamp(lse_profit_opt.lse_booking_date))),
                                                         'left').select('t1.*', 'metric_value').withColumnRenamed(
            'metric_value', 'p_libor24')
        print(f"lse_rym_based_drivers {lse_profit_opt.count()}")
        lse_profit_opt = lse_profit_opt.transform(self.get_lse_age)
        lse_profit_opt = lse_profit_opt.withColumn('c_cost_of_equity', lit(0.1))
        lse_profit_opt = lse_profit_opt.transform(self.get_lse_dlc_region).transform(self.assign_abs_speed).transform(
            self.get_term)
        lse_profit_opt=lse_profit_opt.drop('manuf_id')
        fm_manuf_lse_lkup=fm_manuf_lse_lkup.withColumn('dealer_location_cd',col('dealer_location_cd').cast('bigint'))
        fm_manuf_lse_lkup=fm_manuf_lse_lkup.withColumn('rym_beg',f.to_date(concat(fm_manuf_lse_lkup.rym_beg,lit('01')),'yyyyMMdd')).withColumn('rym_end',f.to_date(concat(fm_manuf_lse_lkup.rym_end,lit('01')),'yyyyMMdd'))
        fm_manuf_lse_lkup.select('rym_beg','rym_end','dealer_location_cd').distinct().show()
        lse_profit_opt.select('lse_booking_date','dealer_location_cd').distinct().show()
        lse_profit_opt = lse_profit_opt.alias('t1').join(fm_manuf_lse_lkup.alias('t2'),
                                                         ((lse_profit_opt.lse_booking_date.between(f.to_date(fm_manuf_lse_lkup.rym_beg),f.to_date(fm_manuf_lse_lkup.rym_end))) & (lse_profit_opt.dealer_location_cd==fm_manuf_lse_lkup.dealer_location_cd)),
                                                         'left').select('t1.*', 't2.manuf_id','t2.manuf_short').withColumnRenamed('manuf_short', 'lob')
        # lse_profit_opt = lse_profit_opt.alias('t1').join(fm_manuf_lse_lkup.alias('t2'),
        #                                                  (lse_profit_opt.manuf_id == fm_manuf_lse_lkup.manuf_id),
        #                                                  'left').select('t1.*', 't2.manuf_short').withColumnRenamed('manuf_short', 'lob')
        print(f"fm_manuf_lse_lkup {lse_profit_opt.count()}")
        lse_profit_opt.select('dealer_location_cd','manuf_id','lob').distinct().show()
        lse_profit_opt = lse_profit_opt.alias('t1').join(lse_capital_abs.alias('t2'), (
                (lse_profit_opt.lse_booking_date.between(to_timestamp(lse_capital_abs.rym_beg),
                                                         to_timestamp(lse_capital_abs.rym_end))) & (
                        lse_profit_opt.veh_model == lse_capital_abs.veh_model) & (
                        lse_profit_opt.make_id == lse_capital_abs.make_id)), 'left').selectExpr('t1.*',
                                                                                                't2.veh_model as c_temp_model',
                                                                                                't2.capital',
                                                                                                't2.capital_mmf',
                                                                                                't2.capital_std')
        lse_profit_opt = lse_profit_opt.withColumn('c_earning_rate', lit(0.0435)).withColumn('collection_fee',
                                                                                             lit(25 * 0.6334))

        lse_profit_opt = lse_profit_opt.alias('t1').join(lse_return_rate_lkup, (
                (lse_profit_opt.manuf_id == lse_return_rate_lkup.manuf_id) & (
                lse_profit_opt.veh_model == lse_return_rate_lkup.veh_model) & (
                        lse_profit_opt.v_temp_term == lse_return_rate_lkup.term) & (
                        lse_profit_opt.grade == lse_return_rate_lkup.grade) & (
                    lse_profit_opt.v_drive_dt.between(to_timestamp(lse_return_rate_lkup.rym_beg),
                                                      to_timestamp(lse_return_rate_lkup.rym_end)))),
                                                         'left').select('t1.*', 'return_rate')
        print(f"lse_return_rate_lkup {lse_profit_opt.count()}")
        lse_profit_opt = lse_profit_opt.withColumn('c_return_rate_ttc', lit(0))
        lse_profit_opt = lse_profit_opt.alias('t1').join(lkup_rym_lob_product.alias('t2'), (
                (lkup_rym_lob_product.lob == 'LSE') & (lkup_rym_lob_product.dim_key == 'ECC') & (
            lse_profit_opt.lse_date_lease.between(f.to_date(lkup_rym_lob_product.rym_beg),
                                                  f.to_date(lkup_rym_lob_product.rym_end)))), 'left').select('t1.*',
                                                                                                             't2.measure_val').withColumnRenamed(
            'measure_val', 'ecc')
        print(f"lkup_rym_lob_product {lse_profit_opt.count()}")
        lse_profit_opt = lse_profit_opt.withColumn('v_subv_ind',
                                                   when(lse_profit_opt.subvened_ind == 1, 1).otherwise(0))
        cof_xref=cof_xref.withColumn('rym_beg',f.to_date(when(length(cof_xref.rym_beg)==6,concat(cof_xref.rym_beg,lit('01'))).otherwise(cof_xref.rym_beg),'yyyyMMdd')).withColumn('rym_end',f.to_date(when(length(cof_xref.rym_end)==6,concat(cof_xref.rym_end,lit('01'))).otherwise(cof_xref.rym_end),'yyyyMMdd'))
        lse_profit_opt = lse_profit_opt.alias('t1').join(cof_xref.alias('t2'), (
                (lse_profit_opt.lse_booking_date.between(to_date(cof_xref.rym_beg), to_date(cof_xref.rym_end))) & (
                lse_profit_opt.v_subv_ind == cof_xref.subvened_ind) & (
                        lse_profit_opt.manuf_id == cof_xref.manuf_id) & (cof_xref.prnp_ind == 'L')),
                                                         'left').selectExpr('t1.*', 't2.lob as v_curve_type')
        cof_xref.select('rym_beg','rym_end','subvened_ind','manuf_id','prnp_ind').distinct().show()
        lse_profit_opt.select('lse_booking_date','v_subv_ind','manuf_id','v_curve_type').distinct().show()
        print(f"cof_xref {lse_profit_opt.count()}")
        lse_profit_opt = lse_profit_opt.alias('t1').join(sofr_cof.alias('t2'), (
            (lse_profit_opt.v_curve_type == sofr_cof.lob)),
                                                         'left').select('t1.*', 't2.sofr_cof_curve')
        print(f"sofr_xref {sofr_cof.count()}")
        lse_profit_opt = lse_profit_opt.alias('t1').join(cof.alias('t2'), (
                (lse_profit_opt.lse_date_lease == cof.cof_dt) & (lse_profit_opt.v_curve_type == cof.lob) & (
                cof.actual_priced_ind == 'P')), 'left').selectExpr('t1.*', 't2.cof_curve').withColumnRenamed('cof_curve',
                                                                                                             'v_cof_curve')
        lse_profit_opt.select('v_cof_curve').distinct().show()
        lse_profit_opt = lse_profit_opt.alias('t1').join(lp_cof.alias('t2'),
                                                         (lse_profit_opt.lse_date_lease.between(to_date(lp_cof.lp_cof_beg_date),to_date(lp_cof.lp_cof_end_date))), 'left').selectExpr('t1.*','t2.lp_curve').withColumnRenamed('lp_curve',
                                                                                                                                                                                                                              'v_lp_curve')
        lse_profit_opt = lse_profit_opt.alias('t1').join(cof.alias('t2'), (
                (lse_profit_opt.lse_booking_date == cof.cof_dt) & (lse_profit_opt.v_curve_type == cof.lob) & (
                cof.actual_priced_ind == 'P')), 'left').selectExpr('t1.*', 't2.cof_curve').withColumnRenamed('cof_curve',
                                                                                                             'vb_cof_curve')
        lse_profit_opt = lse_profit_opt.alias('t1').join(lp_cof.alias('t2'),
                                                         (lse_profit_opt.lse_booking_date.between(to_date(lp_cof.lp_cof_beg_date),to_date(lp_cof.lp_cof_end_date))), 'left').selectExpr('t1.*','t2.lp_curve').withColumnRenamed('lp_curve',
                                                                                                                                                                                                                                'vb_lp_curve')
        lse_profit_opt = lse_profit_opt.alias('t1').join(lp_cross_funding_credit.alias('t2'), (
                lse_profit_opt.v_curve_type == lp_cross_funding_credit.lob), 'left').select('t1.*',
                                                                                            't2.lp_cf_curve').withColumnRenamed(
            'lp_cf_curve', 'v_cf_curve')
        lse_profit_opt.select('v_cof_curve','v_lp_curve','vb_cof_curve','vb_lp_curve','v_cf_curve').distinct().show()
        lse_profit_opt = lse_profit_opt.withColumn('sofr_cof_wt', lit(0))
        lse_profit_opt=lse_profit_opt.withColumn('wacc',lit(0.03))
        cof_df = lse_profit_opt.withColumn("result",
                                           get_lse_cof_lp_anrr_udf(lse_profit_opt.v_temp_term, lse_profit_opt.abs_speed,
                                                                   lse_profit_opt.chase_alg_residual,
                                                                   lse_profit_opt.lse_amt_cap_cost,
                                                                   lse_profit_opt.v_cof_curve,
                                                                   lse_profit_opt.v_lp_curve,
                                                                   lse_profit_opt.v_cf_curve,
                                                                   lse_profit_opt.wacc))
        cof_df = cof_df.select('acct_nbr', 'result.*')
        cof_df = cof_df.dropDuplicates(['acct_nbr'])
        cof_df.show()
        # lse_profit_opt = lse_profit_opt.alias('t1').join(cof_df.alias('t2'),
        #                                                  (col('t1.acct_nbr') == col('t2.acct_nbr')),
        #                                                  'left').select('t1.*', 't2.cof', 't2.lp', 't2.o_cf', 't2.o_fe',
        #                                                                 't2.anr', 't2.anrdisc',
        #                                                                 't2.o_avg_outst_perc_bkd')
        lse_profit_opt = lse_profit_opt.alias('t1').join(cof_df.alias('t2'),
                                                         (col('t1.acct_nbr') == col('t2.acct_nbr')),
                                                         'left').select('t1.*', 't2.cof', 't2.lp', 't2.o_cf').withColumnRenamed('cof','v_cof').withColumnRenamed('lp','v_lp').withColumnRenamed('o_cf','v_cf')

        cof_df = lse_profit_opt.withColumn("result",
                                           get_lse_cof_lp_anrr_udf(lse_profit_opt.v_temp_term, lse_profit_opt.abs_speed,
                                                                   lse_profit_opt.chase_alg_residual,
                                                                   lse_profit_opt.lse_amt_cap_cost,
                                                                   lse_profit_opt.vb_cof_curve,
                                                                   lse_profit_opt.vb_lp_curve,
                                                                   lse_profit_opt.v_cf_curve,
                                                                   lse_profit_opt.wacc))
        cof_df = cof_df.select('acct_nbr', 'result.*')
        cof_df = cof_df.dropDuplicates(['acct_nbr'])
        lse_profit_opt = lse_profit_opt.alias('t1').join(cof_df.alias('t2'),
                                                         (col('t1.acct_nbr') == col('t2.acct_nbr')),
                                                         'left').select('t1.*', 't2.cof', 't2.lp', 't2.o_cf').withColumnRenamed('cof','vb_cof').withColumnRenamed('lp','vb_lp').withColumnRenamed('o_cf','vb_cf')
        lse_profit_opt = lse_profit_opt.withColumn('wacc', (
                lse_profit_opt.v_cof * (1.0 - lse_profit_opt.tax_rate) * (1.0 - lse_profit_opt.lse_amt_markup)) + (
                                                           lse_profit_opt.lse_amt_markup * lse_profit_opt.c_cost_of_equity))
        cof_df = lse_profit_opt.withColumn("result",
                                           get_lse_cof_lp_anrr_udf(lse_profit_opt.v_temp_term, lse_profit_opt.abs_speed,
                                                                   lse_profit_opt.chase_alg_residual,
                                                                   lse_profit_opt.lse_amt_cap_cost,
                                                                   lse_profit_opt.vb_cof_curve,
                                                                   lse_profit_opt.vb_lp_curve,
                                                                   lse_profit_opt.v_cf_curve,
                                                                   lse_profit_opt.wacc))
        cof_df = cof_df.select('acct_nbr', 'result.*')
        cof_df = cof_df.dropDuplicates(['acct_nbr'])
        lse_profit_opt = lse_profit_opt.alias('t1').join(cof_df.alias('t2'),
                                                         (col('t1.acct_nbr') == col('t2.acct_nbr')),'left').select('t1.*','t2.o_fe','t2.anr', 't2.anrdisc','t2.o_avg_outst_perc_bkd')
        print(f"cof_df {lse_profit_opt.count()}")





        lse_profit_opt = lse_profit_opt.alias('t1').join(manuf_lob_mgmt_fee.alias('t2'), (
                (lse_profit_opt.lse_booking_date.between(to_date(manuf_lob_mgmt_fee.rym_beg),
                                                         to_date(manuf_lob_mgmt_fee.rym_end))) & (
                        manuf_lob_mgmt_fee.portfolio == 'LEASE') & (
                        manuf_lob_mgmt_fee.manuf_id == lse_profit_opt.manuf_id) & (
                        manuf_lob_mgmt_fee.subvened_ind == lse_profit_opt.subvened_ind) & (
                        manuf_lob_mgmt_fee.new_used == lse_profit_opt.prodage)), 'left').selectExpr('t1.*',
                                                                                                    't2.origination_fee as o_fee_exp',
                                                                                                    't2.subvention_fund',
                                                                                                    't2.revenuew_share as manuf_rev_share_exp',
                                                                                                    't2.revenuew_share_base',
                                                                                                    't2.revenuew_share_bonus as o_fee_revshr_bonus_exp',
                                                                                                    't2.licensing_fee')
        print(f"manuf_lob_mgmt_fee {lse_profit_opt.count()}")
        lse_profit_opt = lse_profit_opt.withColumn('subvention_revenue',
                                                   (lse_profit_opt.lse_amt_subvention * (
                                                           lse_profit_opt.anr / lse_profit_opt.anrdisc)))
        lse_profit_opt = lse_profit_opt.withColumn('subvention_buydown', when(lse_profit_opt.lse_amt_subvention > 0, (
                (lse_profit_opt.lse_buy_rate - lse_profit_opt.c_earning_rate) * lse_profit_opt.anr)).otherwise(
            lit(0)))
        lse_profit_opt = lse_profit_opt.withColumn('acquisition_fee',
                                                   (lse_profit_opt.lse_amt_markup * (
                                                           lse_profit_opt.anr / lse_profit_opt.anrdisc)))
        lse_profit_opt = lse_profit_opt.alias('t1').join(lp_credit_adj.alias('t2'), (
                (lse_profit_opt.lse_booking_date.between(to_date(lp_credit_adj.rym_beg),
                                                         to_date(lp_credit_adj.rym_end))) & (
                        lp_credit_adj.lob == lse_profit_opt.lob) & (lp_credit_adj.dim_key == (
                lse_profit_opt.v_curve_type * lse_profit_opt.v_lp_curve * lse_profit_opt.anr * lse_profit_opt.lse_amt_cap_cost))),
                                                         'left').select('t1.*', 't2.credit_adj').withColumnRenamed(
            'credit_adj', 'v_cap_resou_crdt_wt')
        print(f"lp_credit_adj {lse_profit_opt.count()}")
        lse_profit_opt = lse_profit_opt.alias('t1').join(lp_credit_adj.alias('t2'), (
                (lse_profit_opt.lse_booking_date.between(to_date(lp_credit_adj.rym_beg),
                                                         to_date(lp_credit_adj.rym_end))) & (
                        lp_credit_adj.lob == lse_profit_opt.lob) & (lp_credit_adj.dim_key == (
                lse_profit_opt.v_curve_type * lse_profit_opt.v_lp_curve * lse_profit_opt.anr * lse_profit_opt.lse_amt_cap_cost))),
                                                         'left').select('t1.*',
                                                                        't2.credit_adj').withColumnRenamed('credit_adj',
                                                                                                           'v_cap_resou_crdt_mmf_wt')
        print(f"lp_credit_adj {lse_profit_opt.count()}")
        lse_profit_opt = lse_profit_opt.alias('t1').join(lp_credit_adj.alias('t2'), (
                (lse_profit_opt.lse_booking_date.between(to_date(lp_credit_adj.rym_beg),
                                                         to_date(lp_credit_adj.rym_end))) & (
                        lp_credit_adj.lob == lse_profit_opt.lob) & (lp_credit_adj.dim_key == (
                lse_profit_opt.v_curve_type * lse_profit_opt.v_lp_curve * lse_profit_opt.anr * lse_profit_opt.lse_amt_cap_cost))),
                                                         'left').select('t1.*',
                                                                        't2.credit_adj').withColumnRenamed('credit_adj',
                                                                                                           'v_cap_resou_crdt_std_wt')
        print(f"lp_credit_adj {lse_profit_opt.count()}")
        lse_profit_opt = lse_profit_opt.withColumn('v_cross_fund_crdt_wt',
                                                   lse_profit_opt.v_cf_curve * lse_profit_opt.anr)
        lse_profit_opt = lse_profit_opt.withColumn('v_liq_bal_wt', lse_profit_opt.v_cf_curve * lse_profit_opt.anr)
        lse_profit_opt = lse_profit_opt.withColumn('v_lp_cap_adj_wt', lse_profit_opt.v_cf_curve * lse_profit_opt.anr)
        lse_profit_opt = lse_profit_opt.withColumn('v_lp_anr_adj_wt', lse_profit_opt.v_cf_curve * lse_profit_opt.anr)
        lse_profit_opt = lse_profit_opt.withColumn('v_long_term_debt_wt',
                                                   lse_profit_opt.v_cf_curve * lse_profit_opt.anr)
        print(f"adjustor started {lse_profit_opt.count()}")
        lse_profit_opt.printSchema()
        lse_profit_opt=(lse_profit_opt.withColumn('dlr_grp',lit('ALL')).withColumn('dealer_type',lit('FR'))
                        .withColumn("booked_rym",date_format(col("lse_booking_date"),'yyyyMM')
                                    .cast(IntegerType())).withColumn('ln_lse',lit(2)).withColumn('dlr_v_manuf_id',lit(-1)).withColumn('channel_id',lit(-1)))

        lse_profit_opt=lse_profit_opt.transform(self.get_loss_attributes,lkup_loss_override,'INDFR','MANUF_ID','dlr_v_manuf_id','dlr_manuf_id')
        lse_profit_opt=lse_profit_opt.transform(self.get_loss_attributes,lkup_loss_override,'INDFR','MAKE_ID','make_id','dlr_make_id')
        lse_profit_opt=lse_profit_opt.transform(self.get_loss_attributes,lkup_loss_override,'INDFR','CHANNEL_ID','channel_id','dlr_channel_id')

        lse_profit_opt = lse_profit_opt.alias('t1').join(lkup_loss_dealer_adjustor.alias('t2'), (
                (col('t2.ln_lse')==col('t1.ln_lse')) & (
            lse_profit_opt.booked_rym.between(lkup_loss_dealer_adjustor.rym_beg, lkup_loss_dealer_adjustor.rym_end)) & (
                        lse_profit_opt.dlr_make_id == lkup_loss_dealer_adjustor.make_id) & (
                        lse_profit_opt.dlr_manuf_id == lkup_loss_dealer_adjustor.manuf_id) & (
                        lse_profit_opt.dlr_channel_id == lkup_loss_dealer_adjustor.channel_id) & (
                        lse_profit_opt.dlr_grp == lkup_loss_dealer_adjustor.dlr_grp) & (
                        lse_profit_opt.dealer_type == lkup_loss_dealer_adjustor.dealer_type)), 'left').select(
            't1.*', 't2.adjustor').withColumnRenamed('adjustor','o_adj_if')
        print(f"lkup_loss_dealer_adjustor {lse_profit_opt.count()}")

        # lse_profit_opt=(lse_profit_opt.withColumn('dlr_grp',lit('ALL')).withColumn('dealer_type',lit('FR'))
        #                 .withColumn("booked_rym",date_format(col("lse_booking_date"),'yyyyMM')
        #                             .cast(IntegerType())).withColumn('ln_lse',lit(2)).withColumn('dlr_v_manuf_id',lit(-1)).withColumn('channel_id',lit(-1)))
        print(lse_profit_opt.columns)
        lse_profit_opt = (lse_profit_opt.alias('t1').join(lkup_rym_lob_product.alias('t2'),
                                             (concat(lit('LSE'), lse_profit_opt["make_id"]) == lkup_rym_lob_product["lob"]) &
                                             (col("booked_rym").between(lkup_rym_lob_product["rym_beg"], lkup_rym_lob_product["rym_end"])) &
                                             (lkup_rym_lob_product["dim_key"] == "NLL" + "LS" + "EXCP"),'left').selectExpr('t1.*',"t2.measure_val as loy_v_make_id"))

        lse_profit_opt=lse_profit_opt.alias('t1').join(fm_manuf_lse_lkup.alias('t2'),(lse_profit_opt['make_id']==fm_manuf_lse_lkup['make_id'])).selectExpr('t1.*','t2.manuf_id as loy_v_manuf_id')
        lse_profit_opt = (
            lse_profit_opt
            .withColumn("booked_rym", date_format(col("lse_booking_date"), 'yyyyMM').cast(IntegerType()))
            .withColumn("adj_rym", date_format(col("lse_date_lease"), 'yyyyMMdd').cast(IntegerType()))
            .withColumn('ln_lse', lit(2))
            .withColumn('loy_v_loayl_id',when(lse_profit_opt['loyalty_ind'] == 'Y', lit(1)).otherwise(lit(0)))
            .withColumn('loy_v_subv', when(lse_profit_opt['subvened_ind'] == 'Y', lit(1)).otherwise(lit(0)))
            .withColumn('loy_v_new_used_id', when(lse_profit_opt['prodage'].substr(1, 1) == 'N', 'NEW').otherwise('USED'))
            .withColumn('loy_v_age_id', lse_profit_opt['prodage'])
        )

        lse_profit_opt.select('acct_nbr','booked_rym','adj_rym','ln_lse','loy_v_loayl_id','loy_v_subv','loy_v_new_used_id','loy_v_age_id').show()
        lse_profit_opt=lse_profit_opt.transform(self.get_loss_attributes,lkup_loss_override,'LOY','MANUF_ID','loy_v_manuf_id','loy_manuf_id')
        lse_profit_opt=lse_profit_opt.transform(self.get_loss_attributes,lkup_loss_override,'LOY','MAKE_ID','loy_v_make_id','loy_make_id')
        lse_profit_opt=lse_profit_opt.transform(self.get_loss_attributes,lkup_loss_override,'LOY','LOYAL_ID','loy_v_loayl_id','loyal_id')
        lse_profit_opt=lse_profit_opt.transform(self.get_loss_attributes,lkup_loss_override,'LOY','SUBV','loy_v_subv','loy_subv')
        lse_profit_opt=lse_profit_opt.transform(self.get_loss_attributes,lkup_loss_override,'LOY','MAKE_NEWUSD','loy_v_new_used_id','loy_new_used_id')
        lse_profit_opt=lse_profit_opt.transform(self.get_loss_attributes,lkup_loss_override,'LOY','AGE_ID','loy_v_age_id','loy_age_id')

        # V_AGE := CASE WHEN V_AGE_ID = -1 THEN 'ALL' ELSE P_AGE END;
        lse_profit_opt = lse_profit_opt.withColumn('fico_score', when(col('fico_score').isNull(), lit(0)).otherwise(col('fico_score')))
        lse_profit_opt = lse_profit_opt.withColumn('lse_cred_score', when(col('lse_cred_score').isNull(), lit(0)).otherwise(col('lse_cred_score')))
        lse_profit_opt = lse_profit_opt.withColumn('cdo_ind', lit('Y'))
        lse_profit_opt.select('acct_nbr','ln_lse','loy_make_id','booked_rym','loy_manuf_id','loyal_id','loy_subv','loy_new_used_id','loy_age_id','fico_score','lse_cred_score','cdo_ind').show()

        lkup_loss_loyalty_adjustor_f9.filter((col('ln_lse')==2)).show()
        lse_profit_opt = lse_profit_opt.alias('t1').join(lkup_loss_loyalty_adjustor_f9.alias('t2'), (
                (col('t1.ln_lse') == col('t2.ln_lse')) & (
            lse_profit_opt.adj_rym.between(lkup_loss_loyalty_adjustor_f9.rym_beg, lkup_loss_loyalty_adjustor_f9.rym_end)) & (
                        lse_profit_opt.loy_make_id == lkup_loss_loyalty_adjustor_f9.make_id) & (
                        lse_profit_opt.loy_manuf_id == lkup_loss_loyalty_adjustor_f9.manuf_id) & (
                        lse_profit_opt.loyal_id == lkup_loss_loyalty_adjustor_f9.loyal) & (
                        lse_profit_opt.loy_subv == lkup_loss_loyalty_adjustor_f9.subv) & (
                        lse_profit_opt.loy_new_used_id == lkup_loss_loyalty_adjustor_f9.new_used) & (
                        lse_profit_opt.loy_age_id == lkup_loss_loyalty_adjustor_f9.age) & (
                    lse_profit_opt.fico_score.between(lkup_loss_loyalty_adjustor_f9.min_fico,
                                                      lkup_loss_loyalty_adjustor_f9.min_fico)) & (
                    lse_profit_opt.lse_cred_score.between(lkup_loss_loyalty_adjustor_f9.min_gen,
                                                          lkup_loss_loyalty_adjustor_f9.max_gen)) & (
                        lse_profit_opt.cdo_ind == lkup_loss_loyalty_adjustor_f9.cdo_ind)), 'left').select(
            't1.*', 't2.adjustor').withColumnRenamed('adjustor','o_adj_ly')
        lse_profit_opt.select('acct_nbr','o_adj_ly').show()
        print(f"lkup_loss_loyalty_adjustor_f9 {lse_profit_opt.count()}")

        lse_profit_opt = lse_profit_opt.alias('t1').join(lkup_rym_lob_product.alias('t2'),
                                             (concat(lit('LSE'), lse_profit_opt["make_id"]) == lkup_rym_lob_product["lob"]) &
                                             (col("booked_rym").between(lkup_rym_lob_product["rym_beg"], lkup_rym_lob_product["rym_end"])) &
                                             (lkup_rym_lob_product["dim_key"] == "NLL" + "LS" + "EXCP"),'left') \
            .selectExpr('t1.*',"t2.measure_val as port_v_make_id")
        lse_profit_opt=lse_profit_opt.withColumn('p_lti',lit(0))
        lse_profit_opt=lse_profit_opt.alias('t1').join(fm_manuf_lse_lkup.alias('t2'),(lse_profit_opt['make_id']==fm_manuf_lse_lkup['make_id'])).selectExpr('t1.*','t2.manuf_id as port_v_manuf_id')
        lse_profit_opt = (
            lse_profit_opt
            .withColumn("booked_rym", date_format(col("lse_booking_date"), 'yyyyMM').cast(IntegerType()))
            .withColumn("adj_rym", date_format(col("lse_date_lease"), 'yyyyMMdd').cast(IntegerType()))
            .withColumn('ln_lse', lit(2))
            .withColumn('port_v_subv', when(lse_profit_opt['subvened_ind'] == 'Y', lit(1)).otherwise(lit(0)))
            .withColumn('loy_v_new_used_id', when(lse_profit_opt['prodage'].substr(1, 1) == 'N', 'NEW').otherwise('USED'))
            .withColumn('port_v_lti', lse_profit_opt['p_lti'])
        )
        lse_profit_opt=lse_profit_opt.transform(self.get_loss_attributes,lkup_loss_override,'PORT','MANUF_ID','port_v_manuf_id','port_manuf_id')
        lse_profit_opt=lse_profit_opt.transform(self.get_loss_attributes,lkup_loss_override,'PORT','LTI','port_v_lti','port_lti')
        lse_profit_opt=lse_profit_opt.transform(self.get_loss_attributes,lkup_loss_override,'PORT','MAKE_ID','port_v_make_id','port_make_id')
        lse_profit_opt=lse_profit_opt.transform(self.get_loss_attributes,lkup_loss_override,'PORT','SUBV','port_v_subv','port_subv')

        lse_profit_opt=lse_profit_opt.withColumn('cpo_ind',lit('X'))
        lse_profit_opt=lse_profit_opt.withColumn('port_lti',when(lse_profit_opt['port_lti'].isNull(),lse_profit_opt['p_lti']).otherwise(lse_profit_opt['port_lti']))
        lse_profit_opt=lse_profit_opt.withColumn('port_lti',when(lse_profit_opt['port_lti'].isNull(),0).otherwise(lse_profit_opt['port_lti']))
        lse_profit_opt=lse_profit_opt.withColumn('p_term',
                                                 when(lse_profit_opt.lse_mos_org <= 24, 24).when(lse_profit_opt.lse_mos_org <= 36, 36).when(lse_profit_opt.lse_mos_org <= 39,
                                                                                                                                            39).when(
                                                     lse_profit_opt.lse_mos_org <= 42, 42) \
                                                 .when(lse_profit_opt.lse_mos_org <= 48, 48).otherwise(60))
        lse_profit_opt.select('acct_nbr','ln_lse','port_make_id','port_lti','booked_rym','adj_rym','port_manuf_id','port_subv','c_booked_ltv','fico_score','lse_cred_score','cdo_ind').show()
        lkup_loss_portfolio_adj_f9.show()
        lse_profit_opt = lse_profit_opt.alias('t1').join(lkup_loss_portfolio_adj_f9.alias('t2'), (
                (col('t1.ln_lse') == col('t2.ln_lse')) & (
            lse_profit_opt.adj_rym.between(lkup_loss_portfolio_adj_f9.rym_beg,
                                           lkup_loss_portfolio_adj_f9.rym_end)) & (
                        lse_profit_opt.port_make_id == lkup_loss_portfolio_adj_f9.make_id) & (
                        lse_profit_opt.port_manuf_id == lkup_loss_portfolio_adj_f9.manuf_id) & (
                        lse_profit_opt.port_subv == lkup_loss_portfolio_adj_f9.subv) & (
                        lse_profit_opt.cpo_ind == lkup_loss_portfolio_adj_f9.cpo_ind) & (
                    lse_profit_opt.port_lti.between(lkup_loss_portfolio_adj_f9.min_lti,
                                                    lkup_loss_portfolio_adj_f9.max_lti)) & (
                    lse_profit_opt.p_term.between(lkup_loss_portfolio_adj_f9.min_term,
                                                  lkup_loss_portfolio_adj_f9.max_term)) & (
                    lse_profit_opt.c_booked_ltv.between(lkup_loss_portfolio_adj_f9.min_ltv,
                                                        lkup_loss_portfolio_adj_f9.max_ltv)) & (
                    lse_profit_opt.fico_score.between(lkup_loss_portfolio_adj_f9.min_fico,
                                                      lkup_loss_portfolio_adj_f9.max_fico)) & (
                    lse_profit_opt.lse_cred_score.between(lkup_loss_portfolio_adj_f9.min_gen,
                                                          lkup_loss_portfolio_adj_f9.max_gen)) & (
                        lse_profit_opt.cdo_ind == lkup_loss_portfolio_adj_f9.cdo_ind)), 'left').select('t1.*',
                                                                                                       't2.adjustor')
        lse_profit_opt = lse_profit_opt.withColumn('o_adj_port', col('adjustor')).drop('adjustor')
        lse_profit_opt.select('acct_nbr','o_adj_port').show()
        print(f"lkup_loss_portfolio_adj_f9 {lse_profit_opt.count()}")

        # lse_profit_opt=lse_profit_opt.transform(self.get_loss_attributes,lkup_loss_override,'PRG_ID','X',lse_profit_opt.make_id).withColumnRenamed(
        #     'override', 'ccp_prg_id').withColumn('ccp_prg_elg_ind',lit(2))
        # lse_profit_opt = lse_profit_opt.alias('t1').join(lkup_loss_dynamic_adj_2_f9.alias('t2'), (
        #             (col('t1.acct_nbr') == col('t2.ln_ls')) & (
        #         lse_profit_opt.lse_date_lease.between(f.to_date(lkup_loss_dynamic_adj_2_f9.rym_beg),
        #                                               f.to_date(lkup_loss_dynamic_adj_2_f9.rym_end))) & (
        #                         lse_profit_opt.dealer_location_cd == lkup_loss_dynamic_adj_2_f9.dlr_location_cd) & (
        #                         lse_profit_opt.make_id == lkup_loss_dynamic_adj_2_f9.make_id) & (
        #                         lse_profit_opt.ccp_prg_id == lkup_loss_dynamic_adj_2_f9.ccp_prg_id) & (
        #                         lse_profit_opt.ccp_prg_elg_ind == lkup_loss_dynamic_adj_2_f9.ccp_prg_elg_ind) & (
        #                         lse_profit_opt.cdo_ind == lkup_loss_dynamic_adj_2_f9.cdo_ind)), 'left').select('t1.*',
        #                                                                                                          't2.adjustor')
        # lse_profit_opt = lse_profit_opt.withColumn('o_adj_dyn', col('adjustor')).drop('adjustor')
        # print(f"lkup_loss_dynamic_adj_2_f9 {lse_profit_opt.count()}")

        # lse_profit_opt = lse_profit_opt.alias('t1').join(lkup_loss_macro_adjustor_f9.alias('t2'), (
        #             (col('t1.acct_nbr') == col('t2.ln_lse')) & (
        #         lse_profit_opt.lse_date_lease.between(f.to_date(lkup_loss_macro_adjustor_f9.rym_beg),
        #                                               f.to_date(lkup_loss_macro_adjustor_f9.rym_end))) & (
        #                         lse_profit_opt.make_id == lkup_loss_macro_adjustor_f9.make_id) & (
        #                         lse_profit_opt.manuf_id == lkup_loss_macro_adjustor_f9.manuf_id) & (
        #                         lse_profit_opt.subv == lkup_loss_macro_adjustor_f9.subv) & (
        #                         lse_profit_opt.lse_state == lkup_loss_macro_adjustor_f9.state) & (
        #                 lse_profit_opt.c_booked_ltv.between(lkup_loss_macro_adjustor_f9.min_ltv,
        #                                              lkup_loss_macro_adjustor_f9.max_ltv)) & (
        #                 lse_profit_opt.fico_score.between(lkup_loss_macro_adjustor_f9.min_fico,
        #                                             lkup_loss_macro_adjustor_f9.max_fico)) & (
        #                 lse_profit_opt.lse_cred_score.between(lkup_loss_macro_adjustor_f9.min_gen,
        #                                              lkup_loss_macro_adjustor_f9.max_gen)) & (
        #                         lse_profit_opt.cdo_ind == lkup_loss_macro_adjustor_f9.cdo_ind)), 'left').select('t1.*',
        #                                                                                                         't2.adjustor')
        # lse_profit_opt = lse_profit_opt.withColumn('o_adj_mcr', col('adjustor')).drop('adjustor')
        # print(f"lkup_loss_macro_adjustor_f9 {lse_profit_opt.count()}")
        # print(lse_profit_opt.columns)
        # print(ldb_acaps_acct_relship_tab)
        # lse_profit_opt=lse_profit_opt.alias('t1').join(ldb_acaps_acct_relship_tab.alias('t2'),(lse_profit_opt.acp_appl_id==ldb_acaps_acct_relship_tab.acp_appl_id),'left').select('t1.*','t2.cdi_ach_cr_am','t2.cdi_acct_07auto','t2.cdi_acct_06mort','t2.cdi_acct_totalaccts2','t2.cdi_mo3adb_03sav','t2.cdi_tot_pst_due_bal_am_all2')
        # lse_profit_opt = lse_profit_opt.alias('t1').join(lkup_loss_cd1_adjustor.alias('t2'), (
        #             (col('t1.acct_nbr') == col('t2.ln_lse')) & (
        #         lse_profit_opt.lse_date_lease.between(f.to_date(lkup_loss_cd1_adjustor.rym_beg),
        #                                               f.to_date(lkup_loss_cd1_adjustor.rym_end))) & (
        #                         lse_profit_opt.make_id == lkup_loss_cd1_adjustor.make_id) & (
        #                         lse_profit_opt.manuf_id == lkup_loss_cd1_adjustor.manuf_id) & (
        #                         lse_profit_opt.subv == lkup_loss_cd1_adjustor.subv) & (
        #                         lse_profit_opt.dealer_location_cd == lkup_loss_cd1_adjustor.dealer_location_cd) & (
        #                         lse_profit_opt.new_used == lkup_loss_cd1_adjustor.new_used) & (
        #                 lse_profit_opt.v_temp_term.between(lkup_loss_cd1_adjustor.min_term,
        #                                               lkup_loss_cd1_adjustor.max_term)) & (
        #                 lse_profit_opt.c_booked_ltv.between(lkup_loss_cd1_adjustor.min_ltv,
        #                                              lkup_loss_cd1_adjustor.max_ltv)) & (
        #                 lse_profit_opt.fico_score.between(lkup_loss_cd1_adjustor.min_fico,
        #                                             lkup_loss_cd1_adjustor.max_fico)) & (
        #                 lse_profit_opt.lse_cred_score.between(lkup_loss_cd1_adjustor.min_gen,
        #                                              lkup_loss_cd1_adjustor.max_gen)) & (
        #                 lse_profit_opt.cdi_acct_totalaccts2.between(lkup_loss_cd1_adjustor.min_cdi_acct_totalaccts2,
        #                                                               lkup_loss_cd1_adjustor.max_cdi_acct_totalaccts2)) & (
        #                 lse_profit_opt.cdi_acct_06mort.between(lkup_loss_cd1_adjustor.min_cdi_acct_06mort,
        #                                                          lkup_loss_cd1_adjustor.max_cdi_acct_06mort)) & (
        #                 lse_profit_opt.cdi_acct_07auto.between(lkup_loss_cd1_adjustor.min_cdi_acct_07auto,
        #                                                          lkup_loss_cd1_adjustor.max_cdi_acct_07auto)) & (
        #                 lse_profit_opt.cdi_mo3adb_03sav.between(lkup_loss_cd1_adjustor.min_mo3adb_chk_mm_sav,
        #                                                            lkup_loss_cd1_adjustor.max_mo3adb_chk_mm_sav)) & (
        #                 lse_profit_opt.cdi_ach_cr_am.between(lkup_loss_cd1_adjustor.min_cdi_ach_cr_am,
        #                                                        lkup_loss_cd1_adjustor.max_cdi_ach_cr_am)) & (
        #                         lse_profit_opt.cdo_ind == lkup_loss_cd1_adjustor.cdo_ind)), 'left').select('t1.*',
        #                                                                                                    't2.adjustor')
        # print(f"lkup_loss_cd1_adjustor {lse_profit_opt.count()}")
        # lse_profit_opt = lse_profit_opt.withColumn('o_adj_cdi1', col('adjustor')).drop('adjustor')
        # lse_profit_opt = lse_profit_opt.alias('t1').join(lkup_loss_cd2_adjustor.alias('t2'), (
        #             (col('t1.acct_nbr') == col('t2.ln_lse')) & (
        #         lse_profit_opt.lse_date_lease.between(f.to_date(lkup_loss_cd2_adjustor.rym_beg),
        #                                               f.to_date(lkup_loss_cd2_adjustor.rym_end))) & (
        #                         lse_profit_opt.make_id == lkup_loss_cd2_adjustor.make_id) & (
        #                         lse_profit_opt.manuf_id == lkup_loss_cd2_adjustor.manuf_id) & (
        #                         lse_profit_opt.subv == lkup_loss_cd2_adjustor.subv) & (
        #                         lse_profit_opt.dealer_location_cd == lkup_loss_cd2_adjustor.dealer_location_cd) & (
        #                         lse_profit_opt.new_used == lkup_loss_cd2_adjustor.new_used) & (
        #                 lse_profit_opt.v_temp_term.between(lkup_loss_cd2_adjustor.min_term,
        #                                               lkup_loss_cd2_adjustor.max_term)) & (
        #                 lse_profit_opt.c_booked_ltv.between(lkup_loss_cd2_adjustor.min_ltv,
        #                                              lkup_loss_cd2_adjustor.max_ltv)) & (
        #                 lse_profit_opt.fico_score.between(lkup_loss_cd2_adjustor.min_fico,
        #                                             lkup_loss_cd2_adjustor.max_fico)) & (
        #                 lse_profit_opt.lse_cred_score.between(lkup_loss_cd2_adjustor.min_gen,
        #                                              lkup_loss_cd2_adjustor.max_gen)) & (
        #                 lse_profit_opt.cdi_acct_totalaccts2.between(lkup_loss_cd2_adjustor.min_cdi_acct_totalaccts2,
        #                                                               lkup_loss_cd2_adjustor.max_cdi_acct_totalaccts2)) & (
        #                     lse_profit_opt.cdi_acct_06mort.between(lkup_loss_cd2_adjustor.min_acct_chck_sav_cd_mm,
        #                                                          lkup_loss_cd2_adjustor.max_acct_chck_sav_cd_mm)) & (
        #                 lse_profit_opt.cdi_acct_07auto.between(lkup_loss_cd2_adjustor.min_cdi_avg_od_am,
        #                                                          lkup_loss_cd2_adjustor.max_cdi_avg_od_am)) & (
        #                 lse_profit_opt.cdi_mo3adb_03sav.between(lkup_loss_cd2_adjustor.min_acct_chck_sav_cd_mm,
        #                                                            lkup_loss_cd2_adjustor.max_acct_chck_sav_cd_mm)) & (
        #                 lse_profit_opt.cdi_tot_pst_due_bal_am_all2.between(
        #                     lkup_loss_cd2_adjustor.min_cdi_tot_pstdue_bal_am_all2,
        #                     lkup_loss_cd2_adjustor.max_cdi_tot_pstdue_bal_am_all2))), 'left').select('t1.*',
        #                                                                                              't2.adjustor')
        # lse_profit_opt = lse_profit_opt.withColumn('o_adj_cdi2', col('adjustor')).drop('adjustor','cdi_ach_cr_am','cdi_acct_07auto','cdi_acct_06mort','cdi_acct_totalaccts2','cdi_mo3adb_03sav','cdi_tot_pst_due_bal_am_all2')
        # print(f"lkup_loss_cd2_adjustor {lse_profit_opt.count()}")

        lse_profit_opt=lse_profit_opt.withColumn('o_adj_dyn',lit(1)).withColumn('o_adj_mcr',lit(1)).withColumn('o_adj_cdi1',lit(1)).withColumn('o_adj_cdi2',lit(1))
        lse_profit_opt = lse_profit_opt.alias('t1').join(lse_expenses_3.alias('t2'), (
                (lse_profit_opt.manuf_id == lse_expenses_3.manuf_id) & (
            lse_profit_opt.lse_date_lease.between(f.to_date(lse_expenses_3.rym_beg),
                                                  f.to_date(lse_expenses_3.rym_end)))), 'left').select('t1.*',
                                                                                                       't2.auto_credit_expense',
                                                                                                       't2.corp_overhead',
                                                                                                       't2.doc_expense',
                                                                                                       't2.imaging_expense',
                                                                                                       't2.sales_expense',
                                                                                                       't2.mgmt_expense',
                                                                                                       't2.property_tax').withColumnRenamed(
            'auto_credit_expense', 'auto_credit_exp').withColumnRenamed('corp_overhead',
                                                                        'corporate_oh_exp').withColumnRenamed(
            'doc_expense', 'docs_prod_mgmt_exp').withColumnRenamed('imaging_expense', 'imaging_exp').withColumnRenamed(
            'sales_expense', 'sales_exp').withColumnRenamed('management_exp', 'mgmt_expense')
        print(f"lse_expense_3 {lse_profit_opt.count()}")
        lse_profit_opt = lse_profit_opt.alias('t1').join(lse_expense_rym.alias('t2'), (
                (lse_profit_opt.manuf_id == lse_expense_rym.manuf_id) & (
            lse_profit_opt.lse_date_lease.between(f.to_date(lse_expense_rym.rym_beg),
                                                  f.to_date(lse_expense_rym.rym_end)))), 'left').select('t1.*',
                                                                                                          'servicing_expense',
                                                                                                          'collections_expense',
                                                                                                          'arbso',
                                                                                                          'statementwt',
                                                                                                          'eot',
                                                                                                          'repo_expense',
                                                                                                          'servicing_collection_amt',
                                                                                                          'servicing_collection_bps',
                                                                                                          'vehicle_remarketing').withColumnRenamed(
            'servicing_expense', 'servicing_exp').withColumnRenamed('collections_expense',
                                                                    'collections_exp').withColumnRenamed('repo_expense',
                                                                                                         'repo_exp')
        print(f"lse_expense_rym {lse_profit_opt.count()}")
        lse_profit_opt = lse_profit_opt.alias('t1').join(lse_losses.alias('t2'), (
                (lse_profit_opt.make_id == lse_losses.make_id) & (lse_profit_opt.v_temp_term == lse_losses.term) & (
                lse_profit_opt.grade == lse_losses.grade)), 'left').select('t1.*', 't2.net_credit_loss').withColumnRenamed('net_credit_loss','losses')
        print(f"lse_losses {lse_profit_opt.count()}")
        lse_profit_opt = lse_profit_opt.alias('t1').join(ldb_acaps_acct_relship_tab.alias('t2'), (
                lse_profit_opt.acp_appl_id == ldb_acaps_acct_relship_tab.acp_appl_id), 'left').select('t1.*',
                                                                                                      'cdi_mob_01chk',
                                                                                                      'cdi_mo3adb_01chk',
                                                                                                      'cdi_mo3adb_02mm',
                                                                                                      'cdi_mo3adb_03sav',
                                                                                                      'cdi_acct_01chck',
                                                                                                      'cdi_acct_03sav',
                                                                                                      'cdi_acct_04cd',
                                                                                                      'cdi_acct_07auto',
                                                                                                      'cdi_mob_ccs',
                                                                                                      'cdi_tot_pst_due_bal_am_all2',
                                                                                                      'cdi_ach_cr_am',
                                                                                                      'cdi_mo3adb_ccs',
                                                                                                      'cdi_mo3adb_06mort',
                                                                                                      'cdi_mob_02mm',
                                                                                                      'cdi_mob_03sav',
                                                                                                      'cdi_mob_06mort',
                                                                                                      'cdi_nsf_fee_cn',
                                                                                                      'cdi_acct_totalaccts2',
                                                                                                      'cdi_acct_02mm',
                                                                                                      'cdi_acct_05ret',
                                                                                                      'cdi_acct_06mort',
                                                                                                      'cdi_tot_pst_due_bal_am_07auto',
                                                                                                      'cdi_tot_pst_due_bal_am_all',
                                                                                                      'cdi_avg_od_am')

        # lse_profit_opt=self.get_lkup_rym_lob_product(lse_profit_opt,fm_manuf_lse_lkup,lkup_rym_lob_product,'RMTEOTBASE','P')
        # lse_profit_opt=lse_profit_opt.withColumnRenamed('measure_val','c_veh_rmktg_lec_sales_marginal_ttc')
        lse_profit_opt = lse_profit_opt.alias('t1').join(lse_expenses_marginal.alias('t2'), (
                (lse_profit_opt.manuf_id == lse_expenses_marginal.manuf_id) & (
            lse_profit_opt.lse_date_lease.between(f.to_date(lse_expenses_marginal.rym_beg),
                                                  f.to_date(lse_expenses_marginal.rym_end)))), 'left').selectExpr(
            't1.*', 't1.sales_exp * t2.sales_exp_marginal as c_sales_exp_marginal',
            't1.manuf_rev_share_exp * t1.auto_credit_exp as c_manual_credit_exp_marginal',
            't1.auto_credit_exp * t2.auto_credit_exp_marginal as c_auto_credit_exp_marginal',
            '(t1.manuf_rev_share_exp+t1.auto_credit_exp) * t2.credit_exp_marginal as c_credit_exp_marginal',
            't1.imaging_exp * t2.imaging_exp_marginal as c_imaging_exp_marginal',
            't1.docs_prod_mgmt_exp * t2.docs_exp_marginal as c_docs_exp_marginal',
            't1.collections_exp * t2.collections_exp_marginal as c_collections_exp_marginal',
            't2.veh_rmktg_marginal * t2.veh_rmktg_lec_sales_marginal as c_veh_rmktg_lec_sales_marginal ',
            't1.c_return_rate_ttc * t2.veh_rmktg_lec_sales_marginal as c_veh_rmktg_lec_sales_marginal_ttc ',
            't1.sales_exp * t2.veh_rmktg_eot_sales_marginal as c_veh_rmktg_eot_sales_marginal ',
            't1.sales_exp * t2.veh_rmktg_eot_sales_marginal as c_veh_rmktg_eot_sales_marginal_ttc',
            't1.servicing_exp * t2.servicing_exp_marginal as c_servicing_exp_marginal ',
            't1.corporate_oh_exp * t2.corp_oh_exp_marginal as c_corp_oh_exp_marginal',
            't1.vehicle_remarketing * t2.veh_rmktg_marginal as c_veh_rmktg_marginal',
            't1.repo_exp * t2.repo_marginal as c_repo_marginal', 't1.eot * t2.eot_marginal as c_eot_marginal ',
            't1.servicing_collection_amt * t2.servcolloh_marginal as c_servcolloh_marginal',
            't1.servicing_collection_bps * t2.servcollohbps_marginal as c_servcollohbps_marginal ',
            't1.arbso * t2.autorecov_bankruptcy_marginal as c_arbso_marginal',
            't1.property_tax * t2.properytax_marginal as c_properytax_marginal ',
            't1.mgmt_expense * t2.mgmt_exp_marginal as c_mgmt_exp_marginal ',
            't2.statementwt_marginal * t2.statementwt_marginal as c_statementwt_marginal')
        print(f"lse_expenses_marginal {lse_profit_opt.count()}")
        lse_profit_opt = lse_profit_opt.withColumn('cpr_speed', lit(0.015)) \
            .withColumn('dtb', lit(0)) \
            .withColumn('dtb_lke', lit(0)) \
            .withColumn('gap_exp', lit(0.0002)) \
            .withColumn('rsrv_fndg_benefit', col('lse_amt_reserve').cast('double') * col('p_libor24').cast('double')) \
            .withColumnRenamed('o_fee_exp', 'manuf_fee') \
            .withColumn('model', col('c_model')) \
            .withColumn('depreciation_exp', coalesce(col('residual_value'), lit(0)) * (
                coalesce(col('lse_amt_reserve'), lit(0)) / coalesce(col('residual_value'), lit(0)))) \
            .withColumn('booked_rym', substring(lit(bus_dt), 1, 6)) \
            .withColumn('create_dt', current_date()) \
            .withColumnRenamed('c_progr_rate', 'c_progr_rt') \
            .withColumnRenamed('csc_2nd_pass_algorithm', 'p_csc_2nd_pass_algorithm') \
            .withColumn('loss_adj_flag', lit('Lease Current Prod Base0 + 4 Adjustments = E_LOSSES_ADJ4')) \
            .withColumnRenamed('o_cf', 'cf') \
            .withColumn('dlr_prm_wt', col('lse_amt_dlr_part_upfront') * (col('anr') / col('anrdisc'))) \
            .withColumn('recap_wt', lit(0)) \
            .withColumn('net_dlr_prm_wt', col('dlr_prm_wt') - col('recap_wt')) \
            .withColumn('__txn_id_long', lit(txn_id)) \
            .withColumn('__start_at', current_date()) \
            .withColumn('__end_at', current_date())
        lse_profit_opt = self.get_lkup_rym_lob_product(lse_profit_opt, fm_manuf_lse_lkup, lkup_rym_lob_product,
                                                       'RMTLECBASE', 'P')
        lse_profit_opt = lse_profit_opt.withColumnRenamed('measure_val', 'veh_rmktg_lec_salesops_exp_ttc')
        lse_profit_opt = self.get_lkup_rym_lob_product(lse_profit_opt, fm_manuf_lse_lkup, lkup_rym_lob_product,
                                                       'RMTEOTBASE', 'P')
        lse_profit_opt = lse_profit_opt.withColumnRenamed('measure_val', 'veh_rmktg_eot_sales_exp_ttc').withColumnRenamed('v_cof','cof').withColumnRenamed('v_lp','lp').withColumnRenamed('v_cf','cf')
        lse_profit_opt = lse_profit_opt.selectExpr('acct_nbr', 'dealer_location_cd', 'fico_score', 'lse_booking_date',
                                                   'lse_amt_cap_cost', 'residual_value', 'lse_amt_reserve', 'c_buy_rt',
                                                   'dealer_flats', 'lse_money_factor', 'lse_mos_org', 'lse_pmt_depr',
                                                   'lse_rate_cust', 'manuf_id', 'make_id', 'veh_yr',
                                                   'new_used_ind', 'grade', 'subvened_ind',
                                                   'prepayment_speed_ind', 'c_region', 'abs_speed',
                                                   'manuf_rev_share_exp', 'anr', 'anrdisc', 'auto_credit_exp',
                                                   'capital', 'cof', 'collection_fee', 'collections_exp',
                                                   'corporate_oh_exp', 'c_cost_of_equity', 'cpr_speed',
                                                   'c_dealer_markup', 'docs_prod_mgmt_exp', 'dtb', 'dtb_lke',
                                                   'c_earning_rate', 'gap_exp', 'imaging_exp', 'residual_value/anr as  inv_funding_exp',
                                                   'lp', 'losses', 'rsrv_fndg_benefit',
                                                   'auto_credit_exp as  manual_credit_exp', 'manuf_fee', '(1-return_rate) as  purchase_fee',
                                                   '(1-return_rate) as  c_return_rate', 'sales_exp', 'servicing_exp',
                                                   'subvention_fund as  manuf_subv_fund_exp', 'subvention_revenue',
                                                   'veh_rmktg_eot_sales_exp_ttc as  veh_rmktg_eot_sales_exp',
                                                   'veh_rmktg_eot_sales_exp_ttc as  veh_rmktg_lec_salesops_exp', 'wacc', 'model', 'prodage',
                                                   'ecc', 'acquisition_fee', 'depreciation_exp', 'tax_rate',
                                                   'booked_rym', 'create_dt', 'edit_dt', 'libor_1month',
                                                   'subvention_buydown', 'c_progr_rt', 'lse_amt_subvention',
                                                   'lse_cred_score', 'c_booked_ltv', '1 as  g5_adj1a',
                                                   'o_adj_if as  g5_adj1b', 'o_adj_ly as  g5_adj2', 'o_adj_dyn as  g5_adj3',
                                                   'o_adj_mcr as  g5_adj4', 'o_adj_mcr as  e_losses_adj1', 'o_adj_mcr as  e_losses_adj2',
                                                   'o_adj_mcr as  e_losses_adj3', 'o_adj_mcr as  e_losses_adj4', 'loyalty_ind',
                                                   'return_rate as  disposal_fee', 'return_rate as  mileage_damage_fee',
                                                   'losses as  c_manuf_rev_share', 'p_lti', 'p_csc_2nd_pass_algorithm',
                                                   'loss_adj_flag', 'capital * anr as  c_pref_div_wt', 'lse_state',
                                                   'o_adj_port as  g5_adj5', 'o_adj_mcr as  e_losses_adj5', 'cf',
                                                   'anr as  v_collat_crdt_wt', 'v_cross_fund_crdt_wt', 'v_liq_bal_wt',
                                                   'v_lp_cap_adj_wt', 'v_lp_anr_adj_wt', 'v_long_term_debt_wt',
                                                   'capital * anr as  v_pref_eqty_crdt_wt', 'anr as  v_oth_int_incadj_wt',
                                                   'v_cap_resou_crdt_wt', ' anr as  v_oth_cof_adj',
                                                   'mgmt_expense as  management_exp', 'capital_mmf', 'capital_std',
                                                   'return_rate as  rv_subv_recapture', 'lse_single_pay_flag', 'acp_appl_id',
                                                   'arbso', 'cdi_acct_01chck', 'cdi_acct_02mm', 'cdi_acct_03sav',
                                                   'cdi_acct_04cd', 'cdi_acct_05ret', 'cdi_acct_06mort',
                                                   'cdi_acct_07auto', 'cdi_acct_totalaccts2', 'cdi_ach_cr_am',
                                                   'cdi_avg_od_am', 'cdi_mo3adb_01chk', 'cdi_mo3adb_02mm',
                                                   'cdi_mo3adb_03sav', 'cdi_mo3adb_06mort', 'cdi_mo3adb_ccs',
                                                   'cdi_mob_01chk', 'cdi_mob_02mm', 'cdi_mob_03sav', 'cdi_mob_06mort',
                                                   'cdi_mob_ccs', 'cdi_nsf_fee_cn', 'cdi_tot_pst_due_bal_am_07auto',
                                                   'cdi_tot_pst_due_bal_am_all', 'cdi_tot_pst_due_bal_am_all2', 'eot',
                                                   'o_adj_mcr as  e_losses_adj6', 'o_adj_mcr as  e_losses_adj7', 'o_adj_CDI1 as  g5_adj6',
                                                   'o_adj_CDI2 as  g5_adj7', 'o_adj_mcr as  o_nll_0', 'docs_prod_mgmt_exp as  repo_expense',
                                                   'servicing_collection_amt', 'servicing_collection_bps',
                                                   'vehicle_remarketing', 'property_tax', 'revenuew_share_base',
                                                   'o_fee_revshr_bonus_exp as  revenuew_share_bonus', 'licensing_fee',
                                                   'c_sales_exp_marginal', 'c_manual_credit_exp_marginal',
                                                   'c_auto_credit_exp_marginal', 'c_credit_exp_marginal',
                                                   'c_imaging_exp_marginal', 'c_docs_exp_marginal',
                                                   'c_collections_exp_marginal', 'c_veh_rmktg_lec_sales_marginal',
                                                   'c_veh_rmktg_eot_sales_marginal', 'c_servicing_exp_marginal',
                                                   'c_corp_oh_exp_marginal', 'c_veh_rmktg_marginal', 'c_repo_marginal',
                                                   'c_eot_marginal', 'c_servcolloh_marginal',
                                                   'c_servcollohbps_marginal', 'c_arbso_marginal',
                                                   'c_properytax_marginal', 'c_mgmt_exp_marginal', 'run_yr_mo',
                                                   'statementwt as  c_statementwt', 'c_statementwt_marginal',
                                                   'lse_rv_billable_amt', 'losses * anr as  llrwt',
                                                   'residual_value/anr as  depreciation_exp_wt_ttc', 'residual_value/anr as  other_int_exp_ttc',
                                                   'sr_dr_amt', 'sbrd_dr_amt', 'v_cap_resou_crdt_std_wt',
                                                   'v_cap_resou_crdt_mmf_wt', 'cof as  bkd_cof_rt', 'cf as  bkd_cf_rt',
                                                   'lp as  bkd_lp_rt', 'sofr_cof_wt', 'residual_value as  csh_coll_wt',
                                                   'residual_value as  csh_coll_ftp_wt', 'c_progr_rt * anr as  dlr_mkp_wt', 'dlr_prm_wt',
                                                   'recap_wt', 'net_dlr_prm_wt', 'lse_amt_dlr_part_upfront',
                                                   'c_std_progr_rt', 'lse_date_lease', 'cdo_ind',
                                                   'residual_value as  rv_subv_recapture_ttc', 'c_return_rate_ttc as  disposal_fee_ttc',
                                                   'c_return_rate_ttc as  mileage_damage_fee_ttc', 'c_return_rate_ttc as  purchase_fee_ttc',
                                                   'veh_rmktg_lec_salesops_exp_ttc', 'veh_rmktg_eot_sales_exp_ttc',
                                                   'c_veh_rmktg_lec_sales_marginal_ttc',
                                                   'c_veh_rmktg_eot_sales_marginal_ttc', 'c_return_rate_ttc',
                                                   'o_adj_if', 'o_adj_ly', 'o_adj_port', 'o_adj_dyn', 'o_adj_mcr',
                                                   'o_adj_cdi1', 'o_adj_cdi2', '__txn_id_long', '__start_at',
                                                   '__end_at')
        # {'rv_subv_recapture_ttc', 'manuf_subv_fund_exp', 'g5_adj5', 'dlr_mkp_wt', 'c_return_rate', 'v_collat_crdt_wt', 'llrwt', 'bkd_lp_rt', 'e_losses_adj3', 'e_losses_adj1', 'disposal_fee', 'other_int_exp_ttc', 'e_losses_adj5', 'veh_rmktg_eot_sales_exp', 'mileage_damage_fee', 'purchase_fee', 'management_exp', 'rv_subv_recapture', 'v_oth_cof_adj', 'v_oth_int_incadj_wt', 'e_losses_adj2', 'g5_adj1a', 'bkd_cof_rt', 'losses', 'o_nll_0', 'disposal_fee_ttc', 'new_used_ind', 'v_pref_eqty_crdt_wt', 'repo_expense', 'g5_adj1b', 'c_pref_div_wt', 'bkd_cf_rt', 'e_losses_adj7', 'revenuew_share_bonus', 'inv_funding_exp', 'g5_adj7', 'g5_adj3', 'g5_adj6', 'mileage_damage_fee_ttc', 'c_statementwt', 'e_losses_adj6', 'veh_rmktg_lec_salesops_exp', 'c_manuf_rev_share', 'e_losses_adj4', 'depreciation_exp_wt_ttc', 'csh_coll_wt', 'g5_adj4', 'purchase_fee_ttc', 'csh_coll_ftp_wt', 'manual_credit_exp', 'g5_adj2'}
        events = {"lse_profit_opt.count()": lse_profit_opt.count()}
        logger.info(f"lse_profit_opt load update events: {events}")
        response = self.kinesisPublish.events_publish(events)
        logger.info(f"fm lse load event completed with response - {response}")
        return lse_profit_opt


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
    catalog_name = params['catalog_name']
    env = params['env']
    process_type = params['process_type']
    print(f"Arguments values catalog name: {catalog_name},env: {env},txn_id: {txn_id},bus_dt:{bus_dt}")
    task = FmLseProfit(env)
    fm_lse = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_refined_enrich_auto_proforma_schema.fm_lse")
    lse_stage = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_refined_enrich_auto_proforma_schema.lse_staging")
    lkup_rym_lob_product = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lkup_rym_lob_product")
    lse_rym_based_drivers = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lse_rym_based_drivers")
    fm_manuf_lse_lkup = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.fm_manuf_lse_lkup")
    lse_capital_abs = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lse_capital_abs_2011")
    lse_return_rate_lkup = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lse_return_rate_lkup")
    cof_xref = task.spark.read.format('delta').table(f"{catalog_name}.fdi_trusted_auto_proforma_schema.cof_xref")
    sofr_cof = task.spark.read.format('delta').table(f"{catalog_name}.fdi_trusted_auto_proforma_schema.sofr_cof")
    lp_cross_funding_credit = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lp_cross_funding_credit")
    cof = task.spark.read.format('delta').table(f"{catalog_name}.fdi_refined_enrich_auto_proforma_schema.cof")
    manuf_lob_mgmt_fee = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.manuf_lob_mgmt_fee")
    lp_credit_adj = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lkup_rym_lob_lp_credit_adj")
    lse_expenses_3 = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lse_expenses_3")
    lse_expense_rym = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lse_expenses_rym")
    lse_losses = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lse_losses")
    p_week_thru = datetime.strptime(bus_dt, '%Y%m%d').date()
    print(p_week_thru)
    p_run_yr_mo = int(p_week_thru.strftime('%Y%m'))
    p_bus_mo = int(p_week_thru.strftime('%Y%m'))
    p_day_id = int(p_week_thru.strftime('%d'))
    v_beg_date = datetime.strptime(('01' + str(p_run_yr_mo)), '%d%Y%m').date()

    ldb_acaps_d_df = loadAcapsDaily(task.spark, v_beg_date, p_week_thru, catalog_name, 'Execute', p_bus_mo, p_day_id)
    ldb_acaps_d_df=ldb_acaps_d_df.select([a.lower() for a in ldb_acaps_d_df.columns])
    ldb_acaps_d_df.persist()
    ldb_acaps_d_df.count()
    ldb_acaps_d_df.createOrReplaceTempView("ldb_acaps_d")

    ldb_acaps_acct_relship_tab = loadReltab(task.spark,catalog_name,'Execute')
    lse_expenses_marginal = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lse_expenses_marginal")
    lkup_loss_dealer_adjustor = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lkup_loss_dealer_adjustor")
    lkup_loss_loyalty_adjustor_f9 = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lkup_loss_loyalty_adjustor_f9")
    lkup_loss_portfolio_adj_f9 = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lkup_loss_portfolio_adj_f9")
    lkup_loss_dynamic_adj_2_f9 = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lkup_loss_dynamic_adj_2_f9")
    lkup_loss_macro_adjustor_f9 = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lkup_loss_macro_adjustor_f9")
    lkup_loss_cd1_adjustor = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lkup_loss_cdi1_adjustor")
    lkup_loss_cd2_adjustor = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lkup_loss_cdi2_adjustor")
    lkup_loss_override = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lkup_loss_override")
    lp_cof = task.spark.read.format('delta').table(
        f"{catalog_name}.fdi_trusted_auto_proforma_schema.lp_cof")
    opt_df = task.launch(txn_id, bus_dt, fm_lse, lse_stage, lkup_rym_lob_product, lse_rym_based_drivers,
                         fm_manuf_lse_lkup, lse_capital_abs, lse_return_rate_lkup, cof_xref, sofr_cof,
                         lp_cross_funding_credit, cof, manuf_lob_mgmt_fee, lp_credit_adj,
                         lse_expenses_3, lse_expense_rym, lse_losses, ldb_acaps_acct_relship_tab, lse_expenses_marginal,lkup_loss_dealer_adjustor,
                         lkup_loss_loyalty_adjustor_f9,lkup_loss_portfolio_adj_f9,lkup_loss_dynamic_adj_2_f9,lkup_loss_macro_adjustor_f9,lkup_loss_cd1_adjustor,
                         lkup_loss_cd2_adjustor,lkup_loss_override,lp_cof)
    lse_profit_opt = f"{catalog_name}.fdi_refined_enrich_auto_proforma_schema.lse_profit_opt"
    lse_profit_opt_df = task.spark.read.format('delta').table(lse_profit_opt)
    lse_profit_opt_df = lse_profit_opt_df.select([col(c.lower()) for c in lse_profit_opt_df.columns])
    opt_df = opt_df.select(
        [col(c).cast(lse_profit_opt_df.select(c.lower()).schema[0].dataType) if c in opt_df.columns else c
         for c in lse_profit_opt_df.columns])
    # print(set(lse_profit_opt_df.columns).difference(opt_df.columns))
    # print(lse_profit_opt_df.columns)
    # print([concat('0 as ', c) if c not in opt_df.columns else c for c in lse_profit_opt_df.columns])
    opt_df.write.mode('overwrite').saveAsTable(lse_profit_opt)

    v_cnt = task.spark.sql(f"""select count(*) val from {catalog_name}.fdi_refined_enrich_auto_proforma_schema.lse_profit_opt""").collect()[0]["val"]
    if v_cnt > 0:
        print('Calling Lease final table load ', datetime.now())
        proforma_ln_ls_final_load("LEASE", catalog_name, task.spark, p_run_yr_mo)
        print('Completed LEASE final table load ', datetime.now())
    else:
        print("Stage table has no records to push to final table")

    task.spark.sql(f"""merge into {catalog_name}.fdi_refined_enrich_auto_proforma_schema.flat_cancels t1
                         using (select lse_nbr, lse_term_type, current_timestamp() as edit_dt
                         from {catalog_name}.fdi_trusted_auto_retail_schema.afsl_cals_dy_raw 
                         where lse_booking_date between '{v_beg_date}' and '{p_week_thru}' and lse_term_type ='44') t2
                            on (t1.acct_nbr =t2.lse_nbr)
                          when matched then 
                                update
                                    set edit_dt = current_timestamp()
                          when not matched then
                                insert (acct_nbr, record_type, edit_dt,__txn_id_long,__start_at,__end_at)
                                values (t2.lse_nbr, t2.lse_term_type, t2.edit_dt, {txn_id}, current_date(), current_date())""")

    v_max_ls_add_to_sys_dt = task.spark.sql(f"""select max(lse_booking_date) dt from {catalog_name}.fdi_refined_enrich_auto_proforma_schema.lse_profit_archive
                                                where run_yr_mo = {p_run_yr_mo}""").collect()[0]["dt"]

    task.spark.sql(f"""merge into {catalog_name}.fdi_refined_enrich_auto_proforma_schema.profitability_data_status tgt
                         using (select {p_run_yr_mo} as run_yr_mo , 'LSE' as lob, '{v_max_ls_add_to_sys_dt}' as incl_data_through,
                               'N' as is_final_ind, current_date() as create_dt, current_date() as edit_dt, {txn_id} as __txn_id_long) src
                            on (tgt.run_yr_mo = src.run_yr_mo and tgt.lob = src.lob )
                          when matched then
                                update
                                    set tgt.incl_data_through = src.incl_data_through
                                    , tgt.edit_dt = src.edit_dt
                                    , tgt.__txn_id_long = src.__txn_id_long
                          when not matched then
                                insert (lob, run_yr_mo, incl_data_through, is_final_ind, edit_dt, create_dt,__txn_id_long,__START_AT)
                                values (src.lob, src.run_yr_mo, src.incl_data_through, src.is_final_ind, src.edit_dt, 
                                        src.create_dt, src.__txn_id_long, current_date())""")

# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == "__main__":
    entrypoint()
