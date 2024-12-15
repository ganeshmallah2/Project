import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.crash_analysis import CrashAnalysis

@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder.master("local[1]").appName("CrashAnalysisTest").getOrCreate()

@pytest.fixture
def sample_dataframes(spark_session):
    df_Units = spark_session.createDataFrame([
        Row(CRASH_ID=1, VEH_BODY_STYL_ID="MOTORCYCLE", VEH_LIC_STATE_ID="TX", VEH_COLOR_ID="RED", FIN_RESP_TYPE_ID="PROOF OF LIABILITY INSURANCE", VEH_DMAG_SCL_1_ID="DAMAGED 5", VEH_DMAG_SCL_2_ID="NO DAMAGE", VEH_HNR_FL="Y", CONTRIB_FACTR_1_ID="ALCOHOL", CONTRIB_FACTR_2_ID="SPEED"),
        Row(CRASH_ID=2, VEH_BODY_STYL_ID="CAR", VEH_LIC_STATE_ID="CA", VEH_COLOR_ID="BLUE", FIN_RESP_TYPE_ID="PROOF OF LIABILITY INSURANCE", VEH_DMAG_SCL_1_ID="NO DAMAGE", VEH_DMAG_SCL_2_ID="DAMAGED 3", VEH_HNR_FL="N", CONTRIB_FACTR_1_ID="SPEED", CONTRIB_FACTR_2_ID="ALCOHOL"),
    ])

    df_Primary_Person = spark_session.createDataFrame([
        Row(CRASH_ID=1, PRSN_GNDR_ID="Male", PRSN_DEATH_TIME="2023-01-01T12:00:00", PRSN_INJRY_SEV_ID="KILLED", PRSN_AIRBAG_ID="NOT DEPLOYED", DRVR_LIC_TYPE_ID="DRIVER LICENSE", DRVR_ZIP="12345", PRSN_ETHNICITY_ID="HISPANIC"),
        Row(CRASH_ID=2, PRSN_GNDR_ID="Female", PRSN_DEATH_TIME=None, PRSN_INJRY_SEV_ID="NOT INJURED", PRSN_AIRBAG_ID="DEPLOYED", DRVR_LIC_TYPE_ID="COMMERCIAL DRIVER LIC.", DRVR_ZIP="67890", PRSN_ETHNICITY_ID="CAUCASIAN"),
    ])

    df_damages = spark_session.createDataFrame([
        Row(CRASH_ID=1, DAMAGED_PROPERTY="NONE"),
        Row(CRASH_ID=2, DAMAGED_PROPERTY="SOME DAMAGE"),
    ])

    df_charges = spark_session.createDataFrame([
        Row(CRASH_ID=1, CHARGE="SPEED"),
        Row(CRASH_ID=2, CHARGE="ALCOHOL"),
    ])

    return df_Units, df_Primary_Person, df_damages, df_charges

@pytest.fixture
def crash_analysis(sample_dataframes):
    df_Units, df_Primary_Person, df_damages, df_charges = sample_dataframes
    return CrashAnalysis(df_Units, df_Primary_Person, df_damages, df_charges)

def test_count_male_deaths_gt(crash_analysis):
    result = crash_analysis.count_male_deaths_gt()
    assert result == 1, f"Expected 1, but got {result}"

def test_count_two_wheelers(crash_analysis):
    result = crash_analysis.count_two_wheelers()
    assert result == 1, f"Expected 1, but got {result}"

def test_top_5_vehicle_makes(crash_analysis):
    result = crash_analysis.top_5_vehicle_makes()
    assert len(result) == 1, f"Expected 1, but got {len(result)}"

def test_valid_lic_hit_and_run(crash_analysis):
    result = crash_analysis.valid_lic_hit_and_run()
    assert "1" in result, f"Expected '1', but got {result}"

def test_max_acc_no_females(crash_analysis):
    result = crash_analysis.max_acc_no_females()
    assert "TX" in result, f"Expected 'TX', but got {result}"

def test_top_3_to_5_vehicle_makes(crash_analysis):
    result = crash_analysis.top_3_to_5_vehicle_makes()
    assert len(result) == 2, f"Expected 2, but got {len(result)}"

def test_top_ethnic_group_per_body_style(crash_analysis):
    result = crash_analysis.top_ethnic_group_per_body_style()
    assert len(result) == 1, f"Expected 1, but got {len(result)}"

def test_top_zip_codes_alcohol_crashes(crash_analysis):
    result = crash_analysis.top_zip_codes_alcohol_crashes()
    assert len(result) == 1, f"Expected 1, but got {len(result)}"

def test_count_crash_ids_no_damage(crash_analysis):
    result = crash_analysis.count_crash_ids_no_damage()
    assert "1" in result, f"Expected '1', but got {result}"

def test_top_5_vehicle_makes_driver(crash_analysis):
    result = crash_analysis.top_5_vehicle_makes_driver()
    assert len(result) == 1, f"Expected 1, but got {len(result)}"
