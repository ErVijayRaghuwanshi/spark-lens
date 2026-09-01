import re
from typing import Dict, Any, List, Optional
from sparklens.models import CompatibilityIssue, Spark4MigrationReport
from sparklens.version import SparkVersionInfo, SparkMajorVersion, _extract_prop


# Deprecated or removed properties in Spark 4.0
SPARK_4_REMOVED_OR_CHANGED_CONFIGS = [
    {
        "key": "spark.shuffle.service.db.backend",
        "bad_values": ["leveldb", "LEVELDB"],
        "severity": "CRITICAL",
        "category": "Shuffle",
        "description": "LevelDB backend for External Shuffle Service is deprecated/removed in Spark 4.x. RocksDB is now the default backend.",
        "remediation": "Change `spark.shuffle.service.db.backend` to `ROCKSDB` or remove the property to use the default."
    },
    {
        "key": "spark.sql.legacy.timeParserPolicy",
        "bad_values": ["legacy", "LEGACY"],
        "severity": "WARNING",
        "category": "SQL",
        "description": "Legacy datetime parser policy is removed or altered in Spark 4.x.",
        "remediation": "Update date/time formats to ISO-8601 patterns and switch `spark.sql.legacy.timeParserPolicy` to `CORRECTED` or `EXCEPTION`."
    },
    {
        "key": "spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation",
        "bad_values": ["true", "TRUE"],
        "severity": "WARNING",
        "category": "SQL",
        "description": "Creating managed Hive/Spark tables at non-empty locations was deprecated in Spark 3.x and is blocked in Spark 4.x.",
        "remediation": "Use unmanaged (EXTERNAL) tables or ensure target table location directory is empty prior to table creation."
    }
]

JVM_DEPRECATED_FLAGS = [
    {
        "flag": "-XX:+UseConcMarkSweepGC",
        "severity": "CRITICAL",
        "category": "Runtime",
        "description": "CMS Garbage Collector (-XX:+UseConcMarkSweepGC) is completely removed in Java 17 (the baseline for Spark 4.x).",
        "remediation": "Switch JVM GC to `-XX:+UseG1GC`, `-XX:+UseZGC`, or Shenandoah GC in `spark.driver.extraJavaOptions` and `spark.executor.extraJavaOptions`."
    }
]


def audit_spark4_compatibility(
    env_data: Dict[str, Any], 
    version_info: SparkVersionInfo
) -> Spark4MigrationReport:
    """Audit an application's environment and properties for Spark 4.x migration readiness."""
    spark_props = env_data.get("sparkProperties", [])
    system_props = env_data.get("systemProperties", [])
    
    issues: List[CompatibilityIssue] = []

    # 1. Check Java Version
    java_ver = version_info.java_version or ""
    if java_ver:
        # Check if Java 8 (1.8) or Java 11
        if java_ver.startswith("1.8") or java_ver.startswith("8."):
            issues.append(CompatibilityIssue(
                severity="CRITICAL",
                category="Runtime",
                property_name="java.version",
                current_value=java_ver,
                description="Java 8 is not supported in Spark 4.x. Spark 4 requires Java 17 or Java 21.",
                remediation="Upgrade cluster and driver JDK runtime to Java 17 (e.g. OpenJDK 17 LTS) or Java 21."
            ))
        elif java_ver.startswith("11."):
            issues.append(CompatibilityIssue(
                severity="CRITICAL",
                category="Runtime",
                property_name="java.version",
                current_value=java_ver,
                description="Java 11 support is dropped in Spark 4.x. Spark 4 requires Java 17 or Java 21.",
                remediation="Upgrade cluster and driver JDK runtime to Java 17 (LTS) or Java 21 (LTS)."
            ))

    # 2. Check Scala Version
    scala_ver = version_info.scala_version or ""
    if scala_ver:
        if scala_ver.startswith("2.12"):
            issues.append(CompatibilityIssue(
                severity="CRITICAL",
                category="Runtime",
                property_name="scala.version",
                current_value=scala_ver,
                description="Scala 2.12 support is dropped in Spark 4.x. Spark 4 is built against Scala 2.13.",
                remediation="Recompile all user JARs, dependencies, and UDFs with Scala 2.13."
            ))

    # 3. Check ANSI SQL Mode
    ansi_prop = _extract_prop(spark_props, "spark.sql.ansi.enabled")
    if ansi_prop is None or ansi_prop.lower() != "true":
        issues.append(CompatibilityIssue(
            severity="WARNING",
            category="SQL",
            property_name="spark.sql.ansi.enabled",
            current_value=str(ansi_prop),
            description="Spark 4.x enables ANSI SQL mode by default (spark.sql.ansi.enabled=true). Queries with division by zero or invalid type casts will throw exceptions instead of returning NULL.",
            remediation="Audit SQL queries. Use `try_divide()` and `try_cast()` for lenient parsing, or explicitly set `spark.sql.ansi.enabled=false` during initial migration."
        ))

    # 4. Check known deprecated / removed Spark configuration keys
    for check in SPARK_4_REMOVED_OR_CHANGED_CONFIGS:
        val = _extract_prop(spark_props, check["key"])
        if val is not None and val in check["bad_values"]:
            issues.append(CompatibilityIssue(
                severity=check["severity"],
                category=check["category"],
                property_name=check["key"],
                current_value=val,
                description=check["description"],
                remediation=check["remediation"]
            ))

    # 5. Check JVM GC and Extra Java Options
    driver_opts = _extract_prop(spark_props, "spark.driver.extraJavaOptions") or ""
    executor_opts = _extract_prop(spark_props, "spark.executor.extraJavaOptions") or ""
    combined_opts = f"{driver_opts} {executor_opts}"
    for jvm_check in JVM_DEPRECATED_FLAGS:
        if jvm_check["flag"] in combined_opts:
            issues.append(CompatibilityIssue(
                severity=jvm_check["severity"],
                category=jvm_check["category"],
                property_name="spark.[driver|executor].extraJavaOptions",
                current_value=jvm_check["flag"],
                description=jvm_check["description"],
                remediation=jvm_check["remediation"]
            ))

    # 6. Check legacy configurations (spark.sql.legacy.*)
    if isinstance(spark_props, list):
        for item in spark_props:
            k = None
            v = None
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                k, v = item[0], item[1]
            elif isinstance(item, dict):
                k, v = item.get("key"), item.get("value")
            
            if k and k.startswith("spark.sql.legacy.") and k not in [c["key"] for c in SPARK_4_REMOVED_OR_CHANGED_CONFIGS]:
                issues.append(CompatibilityIssue(
                    severity="WARNING",
                    category="Configuration",
                    property_name=k,
                    current_value=str(v),
                    description=f"Legacy configuration `{k}` may have been altered or deprecated in Spark 4.x.",
                    remediation=f"Verify `{k}` in Spark 4.0 migration guide and adopt standard non-legacy behavior."
                ))

    # Count issues
    crit_count = sum(1 for i in issues if i.severity == "CRITICAL")
    warn_count = sum(1 for i in issues if i.severity == "WARNING")

    if crit_count > 0:
        status = "HIGH_RISK"
    elif warn_count > 0:
        status = "ACTION_REQUIRED"
    else:
        status = "READY"

    checklist = [
        "1. Verify JDK is Java 17 LTS or Java 21 LTS.",
        "2. Rebuild user custom code and libraries against Scala 2.13.",
        "3. Test SQL expressions against ANSI SQL compliance (use try_cast/try_divide where appropriate).",
        "4. Switch shuffle service db backend from LEVELDB to ROCKSDB.",
        "5. Clean up removed legacy JVM flags (such as CMS GC options).",
        "6. Test with Adaptive Query Execution (AQE) enabled."
    ]

    return Spark4MigrationReport(
        app_id=env_data.get("appId") or "unknown",
        current_version=version_info.raw_version,
        target_version="4.x",
        status=status,
        critical_count=crit_count,
        warning_count=warn_count,
        issues=issues,
        migration_checklist=checklist
    )
