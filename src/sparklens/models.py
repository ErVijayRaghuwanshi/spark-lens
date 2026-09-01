from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from sparklens.version import SparkMajorVersion, SparkVersionInfo, SparkFeatures


class MetricQuantileDistribution(BaseModel):
    """Distribution metrics across tasks calculated from quantiles."""
    min: float = 0.0
    p25: Optional[float] = None
    median: float = 0.0
    p75: Optional[float] = None
    p95: float = 0.0
    max: float = 0.0
    ratio_max_median: float = 1.0
    skewed: bool = False
    message: Optional[str] = None


class DataSkewAnalysis(BaseModel):
    """Analysis result detecting task runtime and spill skew in a stage."""
    stage_id: int
    attempt_id: int
    spark_major_version: SparkMajorVersion = SparkMajorVersion.UNKNOWN
    status: str = Field(description="'SKEWED' or 'UNIFORM'")
    runtime_skew: MetricQuantileDistribution
    gc_time_distribution: MetricQuantileDistribution
    memory_spill_distribution: MetricQuantileDistribution
    disk_spill_distribution: MetricQuantileDistribution
    recommendations: List[str]


class StageFailureDetail(BaseModel):
    """Parsed and categorized details for a failed stage."""
    stage_id: int
    attempt_id: int
    name: str
    failure_reason: str
    error_category: str = Field(
        description="Categorized type: 'ANSI_SQL_ERROR', 'OUT_OF_MEMORY', 'SHUFFLE_FETCH_FAILURE', 'EXECUTOR_LOSS', 'SCHEMA_MISMATCH', or 'GENERIC_FAILURE'"
    )
    error_class: Optional[str] = Field(
        default=None, 
        description="Standard Spark 4.x Error Class (e.g. CANNOT_DIVIDE_BY_ZERO, CAST_INVALID_INPUT) if detected."
    )
    num_failed_tasks: int = 0
    num_complete_tasks: int = 0
    executor_run_time: int = 0
    remediation_steps: List[str] = []


class ApplicationHealthSummary(BaseModel):
    """High-level health report and failure summary for a Spark application."""
    app_name: Optional[str]
    app_id: str
    spark_version: SparkMajorVersion
    spark_version_raw: str
    duration_ms: int
    total_jobs: int
    failed_jobs_count: int
    total_stages: int
    failed_stages_count: int
    status: str = Field(description="'Healthy' or 'Degraded/Failed'")
    major_failures: List[Dict[str, Any]]
    recommendations: List[str] = []


class CompatibilityIssue(BaseModel):
    """Individual configuration or compatibility issue when migrating to Spark 4.x."""
    severity: str = Field(description="'CRITICAL', 'WARNING', or 'INFO'")
    category: str = Field(description="'SQL', 'Configuration', 'Runtime', 'Shuffle', 'Streaming'")
    property_name: Optional[str] = None
    current_value: Optional[str] = None
    description: str
    remediation: str


class Spark4MigrationReport(BaseModel):
    """Full migration and compatibility audit scorecard for an application."""
    app_id: str
    current_version: str
    target_version: str = "4.x"
    status: str = Field(description="'READY', 'ACTION_REQUIRED', or 'HIGH_RISK'")
    critical_count: int = 0
    warning_count: int = 0
    issues: List[CompatibilityIssue] = []
    migration_checklist: List[str] = []
