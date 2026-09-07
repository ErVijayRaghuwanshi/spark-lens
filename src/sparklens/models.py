from enum import Enum
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


# --- Livy-Next Models ---

class LivySessionState(str, Enum):
    """Lifecycle states for Livy-Next interactive sessions."""
    STARTING = "starting"
    IDLE = "idle"
    BUSY = "busy"
    DEAD = "dead"
    SHUTTING_DOWN = "shutting_down"


class LivyStatementState(str, Enum):
    """Execution states for Livy-Next statements."""
    WAITING = "waiting"
    RUNNING = "running"
    AVAILABLE = "available"
    ERROR = "error"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"


class LivyStatementOutput(BaseModel):
    """Output payload from a Livy statement execution."""
    status: Optional[str] = Field(default=None, description="'ok' or 'error'")
    execution_count: Optional[int] = None
    data: Optional[Dict[str, Any]] = Field(default=None, description="Output data e.g. application/json or text/plain")
    ename: Optional[str] = Field(default=None, description="Exception name if execution failed")
    evalue: Optional[str] = Field(default=None, description="Exception message/details")
    traceback: Optional[List[str]] = Field(default=None, description="Stack trace lines")


class LivyStatement(BaseModel):
    """Statement submitted to a Livy interactive session."""
    id: int
    code: Optional[str] = None
    state: LivyStatementState
    output: Optional[LivyStatementOutput] = None
    progress: Optional[float] = 0.0
    started: Optional[int] = None
    completed: Optional[int] = None


class LivySession(BaseModel):
    """Interactive Livy-Next session connected to Spark Connect."""
    id: int
    appId: Optional[str] = None
    sessionId: Optional[str] = None
    state: LivySessionState
    kind: Optional[str] = "spark"
    name: Optional[str] = None
    owner: Optional[str] = None
    proxyUser: Optional[str] = None
    log: Optional[List[str]] = []
    appInfo: Optional[Dict[str, Any]] = {}
    lastActivity: Optional[str] = None


class LivySessionsResponse(BaseModel):
    """Response containing list of active Livy-Next sessions."""
    model_config = {"populate_by_name": True}

    from_idx: Optional[int] = Field(default=0, alias="from")
    total: int
    sessions: List[LivySession] = []
    idleTimeout: Optional[int] = None
    deadTimeout: Optional[int] = None


class LivyStatementsResponse(BaseModel):
    """Response containing statements submitted to a Livy-Next session."""
    total_statements: int
    statements: List[LivyStatement] = []


class LivyExecutionSummary(BaseModel):
    """Convenient high-level summary of a statement execution result."""
    session_id: int
    statement_id: int
    code: str
    state: str
    status: Optional[str] = None
    duration_ms: Optional[int] = None
    data_preview: Optional[Any] = None
    schema_fields: Optional[List[Dict[str, Any]]] = None
    row_count: Optional[int] = None
    error_name: Optional[str] = None
    error_value: Optional[str] = None
    error_category: Optional[str] = None
    error_class: Optional[str] = None
    remediation_steps: List[str] = []

