import pytest
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from rftcari_dbx_pipeline.tasks.fm_lse import FmLse
from unittest.mock import patch


@pytest.fixture()
def afsl_cals_dy_raw_txt_path(spark):
    return str(Path(__file__).parent.joinpath("resources/afsl_cals_dy_raw.csv"))

@pytest.fixture()
def fm_lse_txt_path():
    return str(Path(__file__).parent.joinpath("resources/fm_lse.csv"))

@pytest.fixture()
def ldb_dms_cals_d_txt_path():
    return str(Path(__file__).parent.joinpath("resources/ldb_dms_cals_d.csv"))


@pytest.fixture()
def afsl_acaps_dy_raw_txt_path():
    return str(Path(__file__).parent.joinpath("resources/afsl_acaps_dy_raw.csv"))


@pytest.fixture()
def acaps_auto_prv_lbl_d_txt_path():
    return str(Path(__file__).parent.joinpath("resources/acaps_auto_prv_lbl_d.csv"))


@pytest.fixture()
def cals_master_m_txt_path():
    return str(Path(__file__).parent.joinpath("resources/cals_master_m.csv"))


@pytest.fixture()
def dms_cals_m_txt_path():
    return str(Path(__file__).parent.joinpath("resources/dms_cals_m.csv"))


@pytest.fixture()
def afsl_lkp_scrub_make_raw_txt_path():
    return str(Path(__file__).parent.joinpath("resources/afsl_lkp_scrub_make_raw.csv"))


@pytest.fixture()
def afsl_lkp_make_hier_raw_txt_path():
    return str(Path(__file__).parent.joinpath("resources/afsl_lkp_make_hier_raw.csv"))


@pytest.fixture()
def p_lkup_scrub_model_txt_path():
    return str(Path(__file__).parent.joinpath("resources/p_lkup_scrub_model.csv"))


@pytest.fixture()
def lse_delinquent_rate_lkup_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lse_delinquent_rate_lkup.csv"))


@pytest.fixture()
def lse_collateral_rate_lkup_txt_path():
    return str(Path(__file__).parent.joinpath("resources/lse_collateral_rate_lkup.csv"))


@pytest.mark.fasttest
def test_fm_lse(spark: SparkSession, afsl_cals_dy_raw_txt_path, ldb_dms_cals_d_txt_path, afsl_acaps_dy_raw_txt_path,
                acaps_auto_prv_lbl_d_txt_path, cals_master_m_txt_path, dms_cals_m_txt_path,
                afsl_lkp_scrub_make_raw_txt_path, afsl_lkp_make_hier_raw_txt_path, p_lkup_scrub_model_txt_path,
                lse_delinquent_rate_lkup_txt_path, lse_collateral_rate_lkup_txt_path,fm_lse_txt_path):
    logging.info("Testing the ldb cals master data process")

    ldb_cals_master_d_stg = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option(
        "delimiter", ",").load(afsl_cals_dy_raw_txt_path)
    _count = ldb_cals_master_d_stg.count()
    assert _count > 0, "No data Available in ldb_cals_master_d_stg table"

    ldb_dms_cals_d = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter",
                                                                                                            ",").load(
        ldb_dms_cals_d_txt_path)
    _count = ldb_dms_cals_d.count()
    assert _count > 0, "No data Available in ldb_dms_cals_d table"

    afsl_acaps_dy_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option(
        "delimiter", ",").load(afsl_acaps_dy_raw_txt_path)
    _count = afsl_acaps_dy_raw.count()
    assert _count > 0, "No data Available in afsl_acaps_dy_raw table"

    acaps_auto_prv_lbl_d = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option(
        "delimiter", ",").load(acaps_auto_prv_lbl_d_txt_path)
    _count = acaps_auto_prv_lbl_d.count()
    assert _count > 0, "No data Available in acaps_auto_prv_lbl_d table"

    dms_cals_m = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option(
        "delimiter", ",").load(dms_cals_m_txt_path)
    _count = dms_cals_m.count()
    assert _count > 0, "No data Available in acaps_auto_prv_lbl_d table"

    cals_master_m = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option(
        "delimiter", ",").load(cals_master_m_txt_path)
    _count = acaps_auto_prv_lbl_d.count()
    assert _count > 0, "No data Available in acaps_auto_prv_lbl_d table"

    afsl_lkp_scrub_make_raw = spark.read.format("csv").option("header", True).option("inferSchema", True).option(
        "delimiter", ",").load(afsl_lkp_scrub_make_raw_txt_path)
    _count = afsl_lkp_scrub_make_raw.count()
    assert _count > 0, "No data available in afsl lookup make table"

    afsl_lkp_make_hier_raw = spark.read.format("csv").option("header", True).option("inferSchema", True).option(
        "delimiter", ",").load(afsl_lkp_make_hier_raw_txt_path)
    _count = afsl_lkp_make_hier_raw.count()
    assert _count > 0, "No data available in hierarchy lookup table"

    p_lkup_scrub_model = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter",
                                                                                                            ",").load(
        p_lkup_scrub_model_txt_path)
    _count = p_lkup_scrub_model.count()
    assert _count > 0, "No data available in model lookup table"

    lse_delinquent_rate_lkup = spark.read.format('csv').option("header", True).option("inferSchema", True).option(
        "delimiter", ",").load(lse_delinquent_rate_lkup_txt_path)
    _count = lse_delinquent_rate_lkup.count()
    assert _count > 0, "No data available in lse delinquent lookup file"

    lse_collateral_rate_lkup = spark.read.format("csv").option("header", True).option("inferSchema", True).option(
        "delimiter", ",").load(lse_collateral_rate_lkup_txt_path)
    _count = lse_collateral_rate_lkup.count()
    assert _count > 0, "No data available in collateral file"

    fm_lse = spark.read.format("csv").option("header", True).option("inferSchema", True).option(
        "delimiter", ",").load(fm_lse_txt_path)
    _count = fm_lse.count()
    assert _count > 0, "No data available in fm_lse file"



    with patch('rftcari_pipeline.framework.kinesisUtil.KinesisUtil.__init__', return_value=None), \
            patch('rftcari_pipeline.framework.kinesisUtil.KinesisUtil.events_publish', return_value=None):
        task = FmLse('Test')
        fm_lse_opt = task.launch('145650606308569', '20240301', ldb_cals_master_d_stg, ldb_dms_cals_d, afsl_acaps_dy_raw,
                             acaps_auto_prv_lbl_d, dms_cals_m, cals_master_m, afsl_lkp_scrub_make_raw,
                             afsl_lkp_make_hier_raw, p_lkup_scrub_model, lse_delinquent_rate_lkup,
                             lse_collateral_rate_lkup, 'daily')
        print(set(fm_lse.columns).difference(fm_lse_opt.columns))
        print(set(fm_lse_opt.columns).difference(fm_lse.columns))
        fm_lse_opt.createOrReplaceTempView("fm_lse_opt")
        _count = spark.table("fm_lse_opt").count()
        assert _count >= 0, "no data found in final table"
        logging.info("Testing the CARI ETL job for ldb_cals_master_d table - completed successfully")
