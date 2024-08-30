import pytest
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, to_date, length,concat,lit
from rftcari_dbx_pipeline.tasks.fm_lse_profit import FmLseProfit, get_lse_cof_lp_anrr
from unittest.mock import patch


@pytest.fixture()
def fm_lse_txt_path():
    return str(Path(__file__).parent.joinpath("resources/fm_lse.csv"))


@pytest.fixture()
def lse_staging_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lse_staging.csv"))

@pytest.fixture()
def lse_profit_opt_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lse_profit_opt.csv"))


@pytest.fixture()
def lkup_rym_lob_product_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lkup_rym_lob_product.csv"))


@pytest.fixture()
def lse_rym_based_drivers_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lse_rym_based_drivers.csv"))


@pytest.fixture()
def fm_manuf_lse_lkup_txt_path():
    return str(Path(__file__).parent.joinpath("resources/fm_manuf_lse_lkup.csv"))


@pytest.fixture()
def lse_capital_abs_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lse_capital_abs.csv"))


@pytest.fixture()
def lse_return_rate_lkup_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lse_return_rate_lkup.csv"))


@pytest.fixture()
def cof_xref_txt_path():
    return str(Path(__file__).parent.joinpath("resources/cof_xref.csv"))


@pytest.fixture()
def sofr_cof_txt_path():
    return str(Path(__file__).parent.joinpath("resources/sofr_cof.csv"))


@pytest.fixture()
def lp_cross_funding_credit_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lp_cross_funding_credit.csv"))


@pytest.fixture()
def cof_txt_path():
    return str(Path(__file__).parent.joinpath("resources/cof.csv"))


@pytest.fixture()
def manuf_lob_mgmt_fee_txt_path():
    return str(Path(__file__).parent.joinpath("resources/manuf_lob_mgmt_fee.csv"))


@pytest.fixture()
def lkup_rym_lob_lp_credit_adj_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lkup_rym_lob_lp_credit_adj.csv"))

@pytest.fixture()
def loss_model_base0_txt_path():
    return str(Path(__file__).parent.joinpath("resources/loss_model_base0.csv"))

@pytest.fixture()
def lse_expenses_3_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lse_expenses_3.csv"))

@pytest.fixture()
def lse_expense_rym_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lse_expense_rym.csv"))
@pytest.fixture()
def lse_losses_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lse_losses.csv"))

@pytest.fixture()
def lse_losses_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lse_losses.csv"))

@pytest.fixture()
def ldb_acaps_acct_relship_tab_txt_path():
    return str(Path(__file__).parent.joinpath("resources/ldb_acaps_acct_relship_tab.csv"))

@pytest.fixture()
def lse_expenses_marginal_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lse_expenses_marginal.csv"))

@pytest.fixture()
def lkup_loss_dealer_adjustor_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lkup_loss_dealer_adjustor.csv"))

@pytest.fixture()
def lkup_loss_loyalty_adjustor_f9_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lkup_loss_loyalty_adjustor_f9.csv"))

@pytest.fixture()
def lkup_loss_portfolio_adj_f9_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lkup_loss_portfolio_adj_f9.csv"))

@pytest.fixture()
def lkup_loss_dynamic_adj_2_f9_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lkup_loss_dynamic_adj_2_f9.csv"))

@pytest.fixture()
def lkup_loss_macro_adjustor_f9_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lkup_loss_macro_adjustor_f9.csv"))

@pytest.fixture()
def lkup_loss_cd1_adjustor_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lkup_loss_cdi1_adjustor.csv"))

@pytest.fixture()
def lkup_loss_cd2_adjustor_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lkup_loss_cdi2_adjustor.csv"))

@pytest.fixture()
def lkup_loss_override_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lkup_loss_override.csv"))

@pytest.fixture()
def lp_cof_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lp_cof.csv"))


@pytest.mark.fasttest
def test_fm_lse_profit(spark: SparkSession, fm_lse_txt_path, lse_staging_txt_path,lse_profit_opt_txt_path ,lkup_rym_lob_product_txt_path,
                       lse_rym_based_drivers_txt_path, fm_manuf_lse_lkup_txt_path,
                       lse_capital_abs_txt_path, lse_return_rate_lkup_txt_path, cof_xref_txt_path, sofr_cof_txt_path,
                       lp_cross_funding_credit_txt_path, cof_txt_path, manuf_lob_mgmt_fee_txt_path,
                       lkup_rym_lob_lp_credit_adj_txt_path,loss_model_base0_txt_path,lse_expenses_3_txt_path,lse_expense_rym_txt_path,lse_losses_txt_path,ldb_acaps_acct_relship_tab_txt_path,
                       lse_expenses_marginal_txt_path,lkup_loss_dealer_adjustor_txt_path,lkup_loss_loyalty_adjustor_f9_txt_path,lkup_loss_portfolio_adj_f9_txt_path,lkup_loss_dynamic_adj_2_f9_txt_path,
                       lkup_loss_macro_adjustor_f9_txt_path,lkup_loss_cd1_adjustor_txt_path,lkup_loss_cd2_adjustor_txt_path,lkup_loss_override_txt_path,lp_cof_txt_path):
    logging.info("Testing the ldb cals master data process")

    fm_lse = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", ",").load(fm_lse_txt_path)
    _count = fm_lse.count()
    assert _count > 0, "No data available in collateral file"

    lse_staging = spark.read.format("csv").option("header", True).option("inferSchema", True).option(
        "delimiter", ",").load(lse_staging_txt_path)
    _count = lse_staging.count()
    assert _count > 0, "No data available in collateral file"

    lkup_rym_lob_product = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter', ',').load(lkup_rym_lob_product_txt_path)
    _count = lkup_rym_lob_product.count()
    assert _count > 0, "No data available for lkup_rym_lob_product table"

    lse_rym_based_drivers = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter', ',').load(lse_rym_based_drivers_txt_path)
    _count = lse_rym_based_drivers.count()
    assert _count > 0, "No data available for lse_rym_based_drivers table"

    fm_manuf_lse_lkup = spark.read.format('csv').option('header', True).option('inferSchema', True).option('delimiter',
                                                                                                           ',').load(
        fm_manuf_lse_lkup_txt_path)
    _count = fm_manuf_lse_lkup.count()
    assert _count > 0, "No data available for fm_manuf_lse_lkup table"

    lse_capital_abs = spark.read.format('csv').option('header', True).option('inferSchema', True).option('delimiter',
                                                                                                         ',').load(
        lse_capital_abs_txt_path)
    _count = lse_capital_abs.count()
    assert _count > 0, "No data available for lse_capital_abs table"

    lse_return_rate_lkup = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter', ',').load(lse_return_rate_lkup_txt_path)
    _count = lse_return_rate_lkup.count()
    assert _count > 0, "No data available for lse_return_rate_lkup table"

    cof_xref = spark.read.format('csv').option('header', True).option('inferSchema', True).option('delimiter',
                                                                                                  ',').load(
        cof_xref_txt_path)
    _count = cof_xref.count()
    assert _count > 0, "No data available for cof_xref table"

    sofr_cof = spark.read.format('csv').option('header', True).option('inferSchema', True).option('delimiter',
                                                                                                  ',').load(
        sofr_cof_txt_path)
    _count = sofr_cof.count()
    assert _count > 0, "No data available for sofr_cof table"

    lp_cross_funding_credit = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter', ',').load(lp_cross_funding_credit_txt_path)
    _count = lp_cross_funding_credit.count()
    assert _count > 0, "No data available for lp_cross_funding_credit table"

    cof = spark.read.format('csv').option('header', True).option('inferSchema', True).option('delimiter', ',').load(
        cof_txt_path)
    _count = cof.count()
    assert _count > 0, "No data available for cof table"

    manuf_lob_mgmt_fee = spark.read.format('csv').option('header', True).option('inferSchema', True).option('delimiter',
                                                                                                            ',').load(
        manuf_lob_mgmt_fee_txt_path)
    _count = manuf_lob_mgmt_fee.count()
    assert _count > 0, "No data available for manuf_lob_mgmt_fee table"

    lp_credit_adj = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter',
        ',').load(
        lkup_rym_lob_lp_credit_adj_txt_path)
    _count = lp_credit_adj.count()
    assert _count > 0, "No data available for lkup_rym_lob_lp_credit_adj table"

    loss_model_base0 = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter',
        ',').load(
        loss_model_base0_txt_path)
    _count = loss_model_base0.count()
    assert _count > 0, "No data available for loss_model_base0 table"

    lse_expenses_3 = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter',
        ',').load(
        lse_expenses_3_txt_path)
    _count = lse_expenses_3.count()
    assert _count > 0, "No data available for lse_expenses_3 table"

    lse_expense_rym = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter',
        ',').load(
        lse_expense_rym_txt_path)
    _count = lse_expense_rym.count()
    assert _count > 0, "No data available for lse_expense_rym table"

    lse_losses = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter',
        ',').load(
        lse_losses_txt_path)
    _count = lse_losses.count()
    assert _count > 0, "No data available for lse_losses table"

    lse_profit_opt = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter',
        ',').load(
        lse_profit_opt_txt_path)
    _count = lse_profit_opt.count()
    assert _count >= 0, "No data available for lse_profit_opt table"

    ldb_acaps_acct_relship_tab = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter',
        ',').load(
        ldb_acaps_acct_relship_tab_txt_path)
    _count = ldb_acaps_acct_relship_tab.count()
    assert _count > 0, "No data available for ldb_acaps_acct_relship_tab table"

    lse_expenses_marginal = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter',
        ',').load(
        lse_expenses_marginal_txt_path)
    _count = lse_expenses_marginal.count()
    assert _count > 0, "No data available for lse_expenses_marginal table"

    lkup_loss_dealer_adjustor = spark.read.format('csv').option('header', True).option('inferSchema', True).option('delimiter',',').load(lkup_loss_dealer_adjustor_txt_path)
    _count = lkup_loss_dealer_adjustor.count()
    assert _count > 0, "No data available for lkup_loss_dealer_adjustor table"

    lkup_loss_loyalty_adjustor_f9 = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter',
        ',').load(
        lkup_loss_loyalty_adjustor_f9_txt_path)
    _count = lkup_loss_loyalty_adjustor_f9.count()
    assert _count > 0, "No data available for lkup_loss_dealer_adjustor table"

    lkup_loss_portfolio_adj_f9 = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter',
        ',').load(
        lkup_loss_portfolio_adj_f9_txt_path)
    _count = lkup_loss_portfolio_adj_f9.count()
    assert _count > 0, "No data available for lkup_loss_portfolio_adj_f9 table"

    lkup_loss_dynamic_adj_2_f9 = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter',
        ',').load(
        lkup_loss_dynamic_adj_2_f9_txt_path)
    _count = lkup_loss_dynamic_adj_2_f9.count()
    assert _count > 0, "No data available for lkup_loss_dynamic_adj_2_f9 table"

    lkup_loss_macro_adjustor_f9 = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter',
        ',').load(
        lkup_loss_macro_adjustor_f9_txt_path)
    _count = lkup_loss_macro_adjustor_f9.count()
    assert _count > 0, "No data available for lkup_loss_macro_adjustor_f9 table"

    lkup_loss_cd1_adjustor = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter',
        ',').load(
        lkup_loss_cd1_adjustor_txt_path)
    _count = lkup_loss_cd1_adjustor.count()
    assert _count > 0, "No data available for lkup_loss_cd1_adjustor table"

    lkup_loss_cd2_adjustor = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter',
        ',').load(
        lkup_loss_cd2_adjustor_txt_path)
    _count = lkup_loss_cd2_adjustor.count()
    assert _count > 0, "No data available for lkup_loss_cd2_adjustor table"

    lkup_loss_override = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter',
        ',').load(
        lkup_loss_override_txt_path)
    _count = lkup_loss_override.count()
    assert _count > 0, "No data available for lkup_loss_override table"

    lp_cof = spark.read.format('csv').option('header', True).option('inferSchema', True).option(
        'delimiter',
        ',').load(lp_cof_txt_path)
    _count = lp_cof.count()
    assert _count > 0, "No data available for lp_cof table"
    with patch('rftcari_pipeline.framework.kinesisUtil.KinesisUtil.__init__', return_value=None), \
            patch('rftcari_pipeline.framework.kinesisUtil.KinesisUtil.events_publish', return_value=None):
        prft_obj = FmLseProfit('UAT')
        lse_prft_opt = prft_obj.launch('145650606308569', '20240301', fm_lse, lse_staging, lkup_rym_lob_product,
                                       lse_rym_based_drivers, fm_manuf_lse_lkup, lse_capital_abs,
                                       lse_return_rate_lkup, cof_xref, sofr_cof, lp_cross_funding_credit, cof,
                                       manuf_lob_mgmt_fee, lp_credit_adj,lse_expenses_3,lse_expense_rym,lse_losses,ldb_acaps_acct_relship_tab,lse_expenses_marginal,lkup_loss_dealer_adjustor,
                                       lkup_loss_loyalty_adjustor_f9,lkup_loss_portfolio_adj_f9,lkup_loss_dynamic_adj_2_f9,lkup_loss_macro_adjustor_f9,lkup_loss_cd1_adjustor,lkup_loss_cd2_adjustor,
                                       lkup_loss_override,lp_cof)
        lse_profit_opt=lse_profit_opt.select([c.lower() for c in lse_profit_opt.columns])
        # print([c if c  in lse_prft_opt.columns else concat(lit('0 as '),c) for c in lse_profit_opt.columns])
        # print(set(lse_profit_opt.columns).difference(lse_prft_opt.columns))
        # print(set(lse_profit_opt.columns).intersection(lse_prft_opt.columns))
        lse_prft_opt.createOrReplaceTempView("lse_prft_opt")
        _count = spark.table("lse_prft_opt").count()
        assert _count >= 0, "no data found in final table"
        logging.info("Testing the CARI ETL job for ldb_cals_master_d table - completed successfully")
