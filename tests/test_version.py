import pytest
from sparklens.version import (
    parse_spark_version,
    SparkMajorVersion,
    get_features_for_version,
    extract_spark_version_from_env,
    extract_spark_version_from_version_endpoint
)

def test_parse_spark_version_3x():
    major, minor, patch, major_enum = parse_spark_version("3.5.1")
    assert major == 3
    assert minor == 5
    assert patch == 1
    assert major_enum == SparkMajorVersion.SPARK_3_X

    major, minor, patch, major_enum = parse_spark_version("v3.2.0-SNAPSHOT")
    assert major == 3
    assert minor == 2
    assert patch == 0
    assert major_enum == SparkMajorVersion.SPARK_3_X


def test_parse_spark_version_4x():
    major, minor, patch, major_enum = parse_spark_version("4.0.0")
    assert major == 4
    assert minor == 0
    assert patch == 0
    assert major_enum == SparkMajorVersion.SPARK_4_X

    major, minor, patch, major_enum = parse_spark_version("4.0.0-preview2")
    assert major == 4
    assert minor == 0
    assert patch == 0
    assert major_enum == SparkMajorVersion.SPARK_4_X


def test_parse_spark_version_unknown():
    major, minor, patch, major_enum = parse_spark_version("invalid")
    assert major_enum == SparkMajorVersion.UNKNOWN

    major, minor, patch, major_enum = parse_spark_version(None)
    assert major_enum == SparkMajorVersion.UNKNOWN


def test_features_matrix_spark3():
    features_3_5 = get_features_for_version(SparkMajorVersion.SPARK_3_X, minor=5)
    assert features_3_5.ansi_sql_default is False
    assert features_3_5.aqe_default is True
    assert features_3_5.default_shuffle_db == "LEVELDB"
    assert 8 in features_3_5.supported_java_versions
    assert "2.12" in features_3_5.supported_scala_versions
    assert features_3_5.spark_connect_supported is True
    assert features_3_5.variant_data_type_supported is False

    features_3_0 = get_features_for_version(SparkMajorVersion.SPARK_3_X, minor=0)
    assert features_3_0.aqe_default is False
    assert features_3_0.spark_connect_supported is False


def test_features_matrix_spark4():
    features_4_0 = get_features_for_version(SparkMajorVersion.SPARK_4_X, minor=0)
    assert features_4_0.ansi_sql_default is True
    assert features_4_0.aqe_default is True
    assert features_4_0.default_shuffle_db == "ROCKSDB"
    assert 8 not in features_4_0.supported_java_versions
    assert 17 in features_4_0.supported_java_versions
    assert "2.12" not in features_4_0.supported_scala_versions
    assert "2.13" in features_4_0.supported_scala_versions
    assert features_4_0.structured_logging is True
    assert features_4_0.spark_connect_supported is True
    assert features_4_0.variant_data_type_supported is True


def test_extract_spark_version_from_env_3x(mock_spark3_env):
    ver_info = extract_spark_version_from_env(mock_spark3_env)
    assert ver_info.raw_version == "3.5.1"
    assert ver_info.major == 3
    assert ver_info.major_version == SparkMajorVersion.SPARK_3_X
    assert "1.8" in ver_info.java_version
    assert "2.12" in ver_info.scala_version
    assert ver_info.aqe_active is False


def test_extract_spark_version_from_env_4x(mock_spark4_env):
    ver_info = extract_spark_version_from_env(mock_spark4_env)
    assert ver_info.raw_version == "4.0.0"
    assert ver_info.major == 4
    assert ver_info.major_version == SparkMajorVersion.SPARK_4_X
    assert "17" in ver_info.java_version
    assert "2.13" in ver_info.scala_version
    assert ver_info.ansi_mode_active is True
    assert ver_info.aqe_active is True


def test_extract_spark_version_from_version_endpoint():
    ver_info = extract_spark_version_from_version_endpoint({"spark": "4.0.0"})
    assert ver_info.major_version == SparkMajorVersion.SPARK_4_X
    assert ver_info.raw_version == "4.0.0"

    ver_info_fallback = extract_spark_version_from_version_endpoint({}, default_fallback="3.x")
    assert ver_info_fallback.major_version == SparkMajorVersion.SPARK_3_X
