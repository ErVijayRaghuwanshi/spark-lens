import re
import logging
from typing import Dict, Any, List, Optional, Tuple
from sparklens.models import (
    MetricQuantileDistribution,
    DataSkewAnalysis,
    StageFailureDetail,
    ApplicationHealthSummary,
)
from sparklens.version import SparkVersionInfo, SparkMajorVersion

logger = logging.getLogger("sparklens.diagnostics")


# Regex to detect Spark 4.x structured error classes, e.g. [CANNOT_DIVIDE_BY_ZERO]
SPARK_ERROR_CLASS_REGEX = re.compile(r"\[([A-Z0-9_]+(?:\.[A-Z0-9_]+)?)\]")


def categorize_failure(
    failure_reason: Optional[str], 
    version_info: Optional[SparkVersionInfo] = None
) -> Tuple[str, Optional[str], List[str]]:
    """Categorize stage failure reason into structured categories with version-specific remediation."""
    if not failure_reason:
        return "GENERIC_FAILURE", None, ["No failure reason reported in event log."]

    reason_lower = failure_reason.lower()
    is_spark_4 = version_info is not None and version_info.major_version == SparkMajorVersion.SPARK_4_X

    # 1. Detect Spark Structured Error Classes (prominent in Spark 4.x and Spark 3.4+)
    match = SPARK_ERROR_CLASS_REGEX.search(failure_reason)
    error_class = match.group(1) if match else None

    if error_class:
        if any(ansi in error_class for ansi in ["CANNOT_DIVIDE_BY_ZERO", "DIVIDE_BY_ZERO"]):
            remediation = [
                "Division by zero encountered.",
                "In Spark 4.x (where ANSI SQL is enabled by default), use `try_divide(numerator, denominator)` to safely return NULL on zero divisor.",
                "Alternatively, add `NULLIF(denominator, 0)` or set `spark.sql.ansi.enabled=false` for legacy behavior."
            ]
            return "ANSI_SQL_ERROR", error_class, remediation

        if any(cast_err in error_class for ansi in ["CAST_INVALID_INPUT", "INVALID_PARAMETER_VALUE", "DATATYPE_MISMATCH"] for cast_err in [ansi]):
            remediation = [
                "Invalid type casting encountered under ANSI mode.",
                "In Spark 4.x, use `try_cast(column AS target_type)` to return NULL instead of failing the job.",
                "Check input schema and upstream data quality for unexpected nulls or malformed formatting."
            ]
            return "ANSI_SQL_ERROR", error_class, remediation

        if any(overflow in error_class for overflow in ["NUMERIC_VALUE_OUT_OF_RANGE", "ARITHMETIC_OVERFLOW"]):
            remediation = [
                "Arithmetic overflow occurred.",
                "In Spark 4.x ANSI mode, overflow raises runtime exceptions. Use `try_add()`, `try_multiply()`, or cast to wider datatypes (`BIGINT` or `DECIMAL`)."
            ]
            return "ANSI_SQL_ERROR", error_class, remediation

        if any(arr_err in error_class for arr_err in ["INVALID_ARRAY_INDEX", "INDEX_OUT_OF_BOUNDS"]):
            remediation = [
                "Array or map index out of bounds.",
                "In Spark 4.x ANSI mode, invalid array access throws errors. Use `element_at(array, index)` or `try_element_at()`."
            ]
            return "ANSI_SQL_ERROR", error_class, remediation

        if any(schema_err in error_class for schema_err in ["UNRESOLVED_COLUMN", "TABLE_OR_VIEW_NOT_FOUND", "COLUMN_ALREADY_EXISTS"]):
            remediation = [
                f"Catalog/Schema resolution error: {error_class}.",
                "Verify table names, column casing, and catalog metastore configuration."
            ]
            return "SCHEMA_MISMATCH", error_class, remediation

    # 2. Out of Memory Errors
    if any(oom in reason_lower for oom in ["outofmemoryerror", "java heap space", "exceeded memory limits", "oomkilled", "gc overhead limit exceeded"]):
        remediation = [
            "Out of Memory (OOM) error detected on Executor/Driver.",
            "Increase executor memory with `spark.executor.memory` (e.g. 4g -> 8g).",
            "Increase memory overhead using `spark.executor.memoryOverhead` or `spark.executor.memoryOverheadFactor` (Spark 3.3+ / 4.x).",
            "If Garbage Collection overhead is high, check partition count (`spark.sql.shuffle.partitions`) and investigate data skew."
        ]
        return "OUT_OF_MEMORY", error_class, remediation

    # 3. Shuffle Fetch Failures
    if any(fetch in reason_lower for fetch in ["fetchfailedexception", "metadatafetchfailedexception", "failed to connect to"]):
        remediation = [
            "Shuffle Fetch Failure: Target executor holding shuffle blocks was lost or unreachable.",
            "Increase `spark.core.connection.ack.wait.timeout` (e.g. to 120s) and `spark.shuffle.io.maxRetries` (e.g. to 10).",
            "For Spark 4.x: Verify that External Shuffle Service is using RocksDB (`spark.shuffle.service.db.backend=ROCKSDB`)." if is_spark_4 else "For Spark 3.x: Consider tuning `spark.shuffle.file.buffer` and executor memory overhead."
        ]
        return "SHUFFLE_FETCH_FAILURE", error_class, remediation

    # 4. Executor Lost / Node Eviction
    if any(loss in reason_lower for loss in ["executorlostexception", "heartbeat missing", "command exited with code", "container killed"]):
        remediation = [
            "Executor was terminated or lost heartbeat to the driver.",
            "Check cluster manager (Kubernetes / YARN) events for node spot evictions or container memory kill events.",
            "Increase `spark.network.timeout` (default 120s) if executors are experiencing prolonged GC pauses."
        ]
        return "EXECUTOR_LOSS", error_class, remediation

    # 5. Broadcast Join Timeout
    if "broadcast" in reason_lower and "timeout" in reason_lower:
        remediation = [
            "Broadcast Exchange timed out waiting for relation build.",
            "Increase `spark.sql.broadcastTimeout` (e.g. from 300s to 600s) or turn off automatic broadcast threshold if table is large."
        ]
        return "BROADCAST_TIMEOUT", error_class, remediation

    return "GENERIC_FAILURE", error_class, [
        "Inspect task exception stack trace and driver logs for detailed failure diagnostics."
    ]


def calculate_distribution(metric_data: Optional[Dict[str, Any]]) -> MetricQuantileDistribution:
    """Calculate distribution and skew from Spark taskSummary quantiles."""
    if not metric_data or not isinstance(metric_data, dict):
        return MetricQuantileDistribution(message="Metric not available")

    quantiles = metric_data.get("quantiles", [])
    if len(quantiles) < 6:
        return MetricQuantileDistribution(message="Insufficient quantile values returned by Spark API")

    min_val = float(quantiles[0])
    p25_val = float(quantiles[1])
    median_val = float(quantiles[2])
    p75_val = float(quantiles[3])
    p95_val = float(quantiles[4])
    max_val = float(quantiles[5])

    if median_val == 0:
        ratio = float(max_val) if max_val > 0 else 1.0
        skewed = max_val > 0
    else:
        ratio = max_val / median_val
        skewed = ratio >= 3.0

    return MetricQuantileDistribution(
        min=round(min_val, 2),
        p25=round(p25_val, 2),
        median=round(median_val, 2),
        p75=round(p75_val, 2),
        p95=round(p95_val, 2),
        max=round(max_val, 2),
        ratio_max_median=round(ratio, 2),
        skewed=skewed
    )


def analyze_stage_skew(
    stage_id: int, 
    stage_attempt_id: int, 
    task_summary: Dict[str, Any], 
    version_info: SparkVersionInfo
) -> DataSkewAnalysis:
    """Perform data and runtime skew analysis on task summaries."""
    runtime_analysis = calculate_distribution(task_summary.get("executorRunTime"))
    gc_analysis = calculate_distribution(task_summary.get("jvmGcTime"))
    spill_mem_analysis = calculate_distribution(task_summary.get("memoryBytesSpilled"))
    spill_disk_analysis = calculate_distribution(task_summary.get("diskBytesSpilled"))

    has_runtime_skew = runtime_analysis.skewed
    has_spill = (spill_mem_analysis.max > 0) or (spill_disk_analysis.max > 0)

    recommendations: List[str] = []

    if has_runtime_skew:
        recommendations.append("High task runtime skew detected: Max task duration is significantly higher than the median.")
        if version_info.major_version == SparkMajorVersion.SPARK_3_X:
            if not version_info.aqe_active:
                recommendations.append("AQE is disabled. Enable Adaptive Query Execution via `spark.sql.adaptive.enabled=true` and `spark.sql.adaptive.skewJoin.enabled=true`.")
            else:
                recommendations.append("AQE skew join is enabled, but skew persists. Consider key salting or adjusting `spark.sql.adaptive.skewJoin.skewedPartitionFactor`.")
        elif version_info.major_version == SparkMajorVersion.SPARK_4_X:
            recommendations.append("Spark 4.x AQE optimizations active. Fine-tune `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` (e.g. 128MB/256MB) or salt skewed join keys.")
        else:
            recommendations.append("Consider key salting or enabling AQE skew join optimizations (`spark.sql.adaptive.skewJoin.enabled=true`).")

    if has_spill:
        recommendations.append(
            f"Tasks spilled {spill_mem_analysis.max} bytes memory / {spill_disk_analysis.max} bytes disk. "
            "Increase `spark.executor.memory` or decrease `spark.executor.cores` to give each task more memory headroom."
        )

    if gc_analysis.skewed and gc_analysis.max > 5000:
        recommendations.append("Significant JVM Garbage Collection pause time detected on tail tasks. Audit memory-heavy object creation or broadcast variables.")

    status = "SKEWED" if (has_runtime_skew or (spill_mem_analysis.skewed and spill_mem_analysis.max > 0)) else "UNIFORM"

    return DataSkewAnalysis(
        stage_id=stage_id,
        attempt_id=stage_attempt_id,
        spark_major_version=version_info.major_version,
        status=status,
        runtime_skew=runtime_analysis,
        gc_time_distribution=gc_analysis,
        memory_spill_distribution=spill_mem_analysis,
        disk_spill_distribution=spill_disk_analysis,
        recommendations=recommendations
    )
