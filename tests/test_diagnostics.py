import pytest
from sparklens.diagnostics import (
    categorize_failure,
    calculate_distribution,
    analyze_stage_skew
)
from sparklens.version import (
    SparkVersionInfo,
    SparkMajorVersion,
    get_features_for_version
)


def test_categorize_spark4_ansi_divide_by_zero():
    v4_info = SparkVersionInfo(
        raw_version="4.0.0",
        major_version=SparkMajorVersion.SPARK_4_X,
        features=get_features_for_version(SparkMajorVersion.SPARK_4_X)
    )
    failure = "org.apache.spark.SparkArithmeticException: [CANNOT_DIVIDE_BY_ZERO] Division by zero. To return NULL instead, use 'try_divide'."
    category, error_class, remediation = categorize_failure(failure, v4_info)

    assert category == "ANSI_SQL_ERROR"
    assert error_class == "CANNOT_DIVIDE_BY_ZERO"
    assert any("try_divide" in r for r in remediation)


def test_categorize_spark4_ansi_cast_invalid_input():
    v4_info = SparkVersionInfo(
        raw_version="4.0.0",
        major_version=SparkMajorVersion.SPARK_4_X,
        features=get_features_for_version(SparkMajorVersion.SPARK_4_X)
    )
    failure = "org.apache.spark.SparkNumberFormatException: [CAST_INVALID_INPUT] The value 'abc' of the type \"STRING\" cannot be cast to \"BIGINT\"."
    category, error_class, remediation = categorize_failure(failure, v4_info)

    assert category == "ANSI_SQL_ERROR"
    assert error_class == "CAST_INVALID_INPUT"
    assert any("try_cast" in r for r in remediation)


def test_categorize_spark3_oom():
    v3_info = SparkVersionInfo(
        raw_version="3.5.1",
        major_version=SparkMajorVersion.SPARK_3_X,
        features=get_features_for_version(SparkMajorVersion.SPARK_3_X)
    )
    failure = "java.lang.OutOfMemoryError: Java heap space"
    category, error_class, remediation = categorize_failure(failure, v3_info)

    assert category == "OUT_OF_MEMORY"
    assert any("spark.executor.memory" in r for r in remediation)


def test_categorize_shuffle_fetch_failure():
    v4_info = SparkVersionInfo(
        raw_version="4.0.0",
        major_version=SparkMajorVersion.SPARK_4_X,
        features=get_features_for_version(SparkMajorVersion.SPARK_4_X)
    )
    failure = "org.apache.spark.shuffle.FetchFailedException: Failed to connect to /10.0.1.4:7337"
    category, error_class, remediation = categorize_failure(failure, v4_info)

    assert category == "SHUFFLE_FETCH_FAILURE"
    assert any("RocksDB" in r for r in remediation)


def test_calculate_distribution_skewed(mock_skewed_task_summary):
    dist = calculate_distribution(mock_skewed_task_summary["executorRunTime"])
    assert dist.skewed is True
    assert dist.min == 120.0
    assert dist.median == 500.0
    assert dist.max == 95000.0
    assert dist.ratio_max_median == 190.0


def test_calculate_distribution_uniform(mock_uniform_task_summary):
    dist = calculate_distribution(mock_uniform_task_summary["executorRunTime"])
    assert dist.skewed is False
    assert dist.ratio_max_median == 1.2


def test_analyze_stage_skew_spark3_no_aqe(mock_skewed_task_summary):
    v3_info = SparkVersionInfo(
        raw_version="3.5.1",
        major_version=SparkMajorVersion.SPARK_3_X,
        aqe_active=False,
        features=get_features_for_version(SparkMajorVersion.SPARK_3_X)
    )
    analysis = analyze_stage_skew(1, 0, mock_skewed_task_summary, v3_info)

    assert analysis.status == "SKEWED"
    assert analysis.spark_major_version == SparkMajorVersion.SPARK_3_X
    assert any("spark.sql.adaptive.enabled=true" in r for r in analysis.recommendations)
    assert any("spilled" in r for r in analysis.recommendations)


def test_analyze_stage_skew_spark4(mock_skewed_task_summary):
    v4_info = SparkVersionInfo(
        raw_version="4.0.0",
        major_version=SparkMajorVersion.SPARK_4_X,
        aqe_active=True,
        features=get_features_for_version(SparkMajorVersion.SPARK_4_X)
    )
    analysis = analyze_stage_skew(2, 0, mock_skewed_task_summary, v4_info)

    assert analysis.status == "SKEWED"
    assert analysis.spark_major_version == SparkMajorVersion.SPARK_4_X
    assert any("Spark 4.x AQE optimizations" in r for r in analysis.recommendations)
