import pytest
from sparklens.compatibility import audit_spark4_compatibility
from sparklens.version import extract_spark_version_from_env

def test_audit_spark3_app_compatibility(mock_spark3_env):
    ver_info = extract_spark_version_from_env(mock_spark3_env)
    report = audit_spark4_compatibility(mock_spark3_env, ver_info)

    assert report.target_version == "4.x"
    assert report.status == "HIGH_RISK"
    assert report.critical_count >= 3
    assert report.warning_count >= 1

    issue_descriptions = [i.description for i in report.issues]
    assert any("Java 8" in d for d in issue_descriptions)
    assert any("Scala 2.12" in d for d in issue_descriptions)
    assert any("LevelDB" in d for d in issue_descriptions)
    assert any("CMS Garbage Collector" in d for d in issue_descriptions)
    assert any("ANSI SQL mode" in d for d in issue_descriptions)


def test_audit_spark4_ready_app(mock_spark4_env):
    ver_info = extract_spark_version_from_env(mock_spark4_env)
    report = audit_spark4_compatibility(mock_spark4_env, ver_info)

    assert report.status == "READY"
    assert report.critical_count == 0
    assert report.warning_count == 0
    assert len(report.issues) == 0
