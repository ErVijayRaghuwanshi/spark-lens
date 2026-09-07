import logging
import sys
from typing import List, Dict, Any, Optional
from fastmcp import FastMCP

from sparklens.config import settings
from sparklens.client import client, livy_client
from sparklens.version import (
    SparkMajorVersion,
    extract_spark_version_from_env,
)
from sparklens.diagnostics import (
    categorize_failure,
    analyze_stage_skew,
)
from sparklens.compatibility import audit_spark4_compatibility

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr
)
logger = logging.getLogger("sparklens")

# Recommended System Prompt for AI Agents using SparkLens
SYSTEM_PROMPT = """You are a senior Apache Spark performance tuning, diagnostic, and execution AI engineer.
You are equipped with the SparkLens toolkit, bridging post-hoc Spark History Server observability with interactive Apache Livy-Next (Spark Connect) execution across Apache Spark 3.x and Spark 4.x.

Your objective is to assist developers and data engineers in analyzing execution logs, diagnosing runtime failures, detecting data skew, evaluating Spark 4.0 migration readiness, and executing interactive queries safely.

Core Capabilities & Guidelines:

1. Application Discovery & Version Detection:
   - When asked about completed or running applications, start with `list_applications`.
   - Call `get_spark_version` (with or without `app_id`) to detect the major version (3.x vs 4.x), runtime JVM (Java 8/11/17/21), Scala version, and whether ANSI SQL mode or Adaptive Query Execution (AQE) is active.

2. Health Diagnostics & Failure Troubleshooting:
   - For high-level health and failure correlation of an application, run `analyze_application`.
   - Diagnose failed stages with `find_failed_stages` and `explain_stage_failure`. Pay close attention to Spark 4.x standardized error classes (e.g., `[CANNOT_DIVIDE_BY_ZERO]`, `[CAST_INVALID_INPUT]`, `[NUMERIC_VALUE_OUT_OF_RANGE]`) and provide ANSI-tolerant remediations (`try_divide`, `try_cast`).
   - For executor crashes, OOMs, or GC overhead, inspect `get_executors` and `get_environment`.

3. Data Skew & Performance Optimization:
   - Analyze task duration and memory/disk spill skew in heavy stages using `find_data_skew`.
   - Inspect SQL physical execution plans with `list_sql_queries` and `get_sql_query_details` to identify costly sort-merge joins, cartesian products, or missing partition pruning.
   - Provide concrete tuning advice: AQE settings (`spark.sql.adaptive.skewJoin.enabled`), partition sizing, key salting, or broadcast hints.

4. Spark 4.0 Migration Auditing:
   - When preparing an application for Spark 4.x upgrade, run `check_spark_compatibility` to audit configurations against breaking changes (Java 17 baseline, Scala 2.13, LevelDB shuffle removal, deprecated configs, and default ANSI mode).

5. Interactive Querying & Livy-Next (Spark Connect):
   - When working with interactive sessions, use `list_livy_sessions`, `get_livy_session`, or `create_livy_session`.
   - Execute queries or code safely using `run_livy_statement`, which waits for completion and formats tabular results and data previews.
   - If a statement fails, review the categorized error and remediation steps. Under Spark 4 ANSI mode, suggest rewrite solutions (e.g. null-tolerant functions).
   - Use `diagnose_livy_session` to bridge interactive session errors and logs with the underlying Spark History Server application diagnostics via `appId`.

6. Output Presentation:
   - Present metrics, schemas, and comparative analyses in clean, structured Markdown tables.
   - Separate root causes from actionable remediation steps using clear headings and bullet points.
   - Keep answers concise, factual, and scannable without dumping unnecessary raw logs into context.
"""

# Initialize FastMCP Server
mcp = FastMCP(
    "SparkLens",
    instructions=SYSTEM_PROMPT
)


# --- MCP Prompts ---

@mcp.prompt()
def diagnose_application(app_id: str) -> str:
    """Create a prompt to diagnose failures in a Spark application."""
    return f"""You are a senior Apache Spark performance tuning and troubleshooting expert.
Your goal is to diagnose the health of the Spark application with ID: `{app_id}`.

Please follow these steps:
1. Start by calling `get_spark_version` with `app_id="{app_id}"` to identify the Spark major version (Spark 3.x vs Spark 4.x), runtime JVM, and ANSI mode settings.
2. Call `analyze_application` with `app_id="{app_id}"` to get a high-level diagnostic summary.
3. If the application status is "Degraded/Failed" or there are failed jobs/stages:
   - Call `find_failed_stages` with `app_id="{app_id}"` to inspect stage failures and categorized error classes (e.g. Spark 4 ANSI SQL errors like `CANNOT_DIVIDE_BY_ZERO` or OOM/Executor loss).
   - If deeper task logs or stack traces are needed, call `explain_stage_failure` or `get_stage_details`.
4. Call `get_executors` with `app_id="{app_id}"` to check if there are dead/failed executors or high GC overhead.
5. Synthesize your findings into a clear, structured report:
   - Identify root causes and error classifications.
   - Provide concrete version-aware remediation steps (code changes, configuration tuning, memory adjustments).
"""


@mcp.prompt()
def optimize_application(app_id: str) -> str:
    """Create a prompt to analyze performance and optimize a Spark application."""
    return f"""You are a senior Apache Spark performance tuning and troubleshooting expert.
Your goal is to conduct a performance and optimization audit for the Spark application with ID: `{app_id}`.

Please follow these steps:
1. Check the Spark version by calling `get_spark_version` with `app_id="{app_id}"`.
2. Call `analyze_application` with `app_id="{app_id}"` to inspect overall run duration and metrics.
3. List the stages using `get_stages` with `app_id="{app_id}"` and identify the slowest stages.
4. For the slowest or resource-heavy stages:
   - Call `find_data_skew` with the stage ID to analyze runtime skew, memory spills, disk spills, and GC distribution.
5. Call `get_executors` with `app_id="{app_id}"` to review core/memory allocation and GC time.
6. Call `get_environment` to review configuration properties (`spark.sql.shuffle.partitions`, AQE settings, shuffle backend).
7. Provide an optimization report with:
   - Major bottlenecks (data skew, spills, excessive shuffling, under/over-partitioning).
   - Spark version-specific configuration recommendations (e.g. AQE skew join parameters, memory overhead factor).
   - Code-level tuning recommendations (key salting, join strategy adjustments).
"""


@mcp.prompt()
def audit_spark4_migration(app_id: str) -> str:
    """Create a prompt to evaluate a Spark 3.x application for Spark 4.x migration readiness."""
    return f"""You are an Apache Spark migration and upgrade specialist.
Your goal is to perform a Spark 4.0 migration readiness audit for the application with ID: `{app_id}`.

Please follow these steps:
1. Call `check_spark_compatibility` with `app_id="{app_id}"` to run automated checks against Spark 4.x breaking changes.
2. Review the application properties via `get_environment` with `app_id="{app_id}"`.
3. Check for:
   - ANSI SQL mode impacts (`spark.sql.ansi.enabled=true` default in Spark 4).
   - Java runtime compatibility (Java 17/21 required; Java 8/11 dropped).
   - Scala compatibility (Scala 2.13 required; Scala 2.12 dropped).
   - Deprecated configuration keys and LevelDB shuffle backend.
   - Removed JVM flags (such as CMS GC).
4. Summarize findings in a clear migration checklist with severity ratings and exact configuration changes needed.
"""


@mcp.prompt()
def explain_sql_query(app_id: str, sql_id: int) -> str:
    """Create a prompt to explain and optimize a specific Spark SQL query execution."""
    return f"""You are a senior Apache Spark SQL optimizer expert.
Your goal is to inspect and suggest optimization strategies for the Spark SQL execution ID: `{sql_id}` in application: `{app_id}`.

Please follow these steps:
1. Retrieve full details of the SQL execution by calling `get_sql_query_details` with `app_id="{app_id}"` and `sql_id={sql_id}`.
2. Analyze the Spark SQL physical plan:
   - Look for expensive operators (SortMergeJoin, CartesianProduct, BroadcastNestedLoopJoin, large Shuffles).
   - Check if Adaptive Query Execution (AQE) was applied.
   - Check partition pruning and filter pushdowns.
3. Explain the query's execution plan in plain, understandable terms.
4. Provide actionable recommendations (broadcast hints, partition pruning, AQE tuning).
"""


@mcp.prompt()
def troubleshoot_livy_session(session_id: int) -> str:
    """Create a prompt to troubleshoot and diagnose an interactive Livy-Next session."""
    return f"""You are an expert Apache Spark & Livy-Next troubleshooting engineer.
Your goal is to investigate and diagnose the interactive Livy session with ID: `{session_id}`.

Please follow these steps:
1. Call `get_livy_session` with `session_id={session_id}` to inspect the session state, runtime kind, and linked Spark `appId`.
2. Call `list_livy_statements` with `session_id={session_id}` to review submitted statements and identify any that failed.
3. Call `diagnose_livy_session` with `session_id={session_id}` to cross-reference session failures with Spark History Server metrics.
4. For any failed statements:
   - Identify the error category and structured error class (e.g., `[DIVIDE_BY_ZERO]`, `[PARSE_SYNTAX_ERROR]`, `[INVALID_HANDLE.SESSION_CLOSED]`).
   - Suggest code or configuration remediation.
5. Provide a summary of session health and actionable recommendations.
"""


@mcp.prompt()
def execute_and_verify_sql(session_id: int, sql_query: str) -> str:
    """Create a prompt to safely execute a SQL query in a Livy-Next session and handle ANSI/runtime errors."""
    return f"""You are a senior Apache Spark SQL developer.
Your goal is to execute the following SQL query in Livy session `{session_id}` and verify its output:

```sql
{sql_query}
```

Please follow these steps:
1. Call `run_livy_statement` with `session_id={session_id}` and `code='''{sql_query}'''`.
2. If execution succeeds (`status="ok"`):
   - Review the returned schema and preview rows.
   - Summarize the result and row count.
3. If execution fails with an ANSI SQL error or runtime exception:
   - Check the `errorCategory`, `errorClass`, and `remediationSteps`.
   - In Spark 4.0 ANSI mode, rewrite failing expressions using tolerant functions (e.g., replace `/` with `try_divide`, `CAST` with `try_cast`, or null-safe operations).
   - Re-execute the corrected query via `run_livy_statement`.
"""


# --- Version & Discovery Tools ---

@mcp.tool
async def get_spark_version(app_id: Optional[str] = None) -> Dict[str, Any]:
    """Retrieve detected Spark major version (3.x vs 4.x), runtime details, and supported features.
    
    Args:
        app_id: Optional Spark application ID to detect app-specific runtime version. If omitted, checks server version.
    """
    version_info = await client.resolve_version(app_id=app_id)
    return version_info.model_dump()


@mcp.tool
async def list_applications(
    status: Optional[str] = "all", 
    limit: Optional[int] = 20
) -> List[Dict[str, Any]]:
    """List applications available in Spark History Server.
    
    Args:
        status: Filter by application status ('completed', 'running', or 'all')
        limit: Max number of applications to return (default: 20)
    """
    return await client.get_applications(status=status, limit=limit)


@mcp.tool
async def get_application(app_id: str) -> Dict[str, Any]:
    """Get high-level details and attempts for a specific Spark application."""
    return await client.get_application(app_id)


# --- Deep-Dive Data Tools ---

@mcp.tool
async def get_jobs(app_id: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get all jobs for a specific Spark application.
    
    Args:
        app_id: The unique Spark application ID
        status: Filter by status ('running', 'succeeded', 'failed', 'unknown')
    """
    return await client.get_jobs(app_id, status=status)


@mcp.tool
async def get_stages(app_id: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get all stages for a Spark application.
    
    Args:
        app_id: The unique Spark application ID
        status: Filter by status ('active', 'complete', 'pending', 'failed')
    """
    return await client.get_stages(app_id, status=status)


@mcp.tool
async def get_stage_details(
    app_id: str, 
    stage_id: int, 
    stage_attempt_id: int = 0
) -> Dict[str, Any]:
    """Get detailed information about a specific stage, including task summaries and aggregator metrics."""
    return await client.get_stage_details(app_id, stage_id, stage_attempt_id)


@mcp.tool
async def get_executors(app_id: str) -> List[Dict[str, Any]]:
    """Get list of active and dead executors for a Spark application."""
    return await client.get_executors(app_id)


@mcp.tool
async def get_environment(app_id: str) -> Dict[str, Any]:
    """Get Spark environment properties, JVM settings, classpath, and system properties for the application."""
    return await client.get_environment(app_id)


# --- SQL Analysis Tools ---

@mcp.tool
async def list_sql_queries(
    app_id: str, 
    limit: Optional[int] = 20
) -> List[Dict[str, Any]]:
    """List SQL query executions for a specific Spark application.
    
    Args:
        app_id: The unique Spark application ID
        limit: Max number of SQL query entries to return
    """
    return await client.get_sql_queries(app_id, limit=limit)


@mcp.tool
async def get_sql_query_details(
    app_id: str, 
    sql_id: int
) -> Dict[str, Any]:
    """Get full details of a specific SQL query execution, including logical and physical execution plans.
    
    Args:
        app_id: The unique Spark application ID
        sql_id: The execution ID of the SQL query
    """
    return await client.get_sql_query_details(app_id, sql_id)


# --- Diagnostic & Troubleshooting Tools ---

@mcp.tool
async def find_failed_stages(app_id: str) -> List[Dict[str, Any]]:
    """Scan stages for a Spark application and identify those that failed, with categorized error classes and version-aware remediation."""
    stages = await client.get_stages(app_id)
    failed = [s for s in stages if s.get("status") == "FAILED"]
    
    version_info = await client.resolve_version(app_id=app_id)

    summary = []
    for stage in failed:
        reason = stage.get("failureReason", "")
        category, error_class, remediation = categorize_failure(reason, version_info)
        summary.append({
            "stageId": stage.get("stageId"),
            "attemptId": stage.get("attemptId", 0),
            "name": stage.get("name"),
            "failureReason": reason,
            "errorCategory": category,
            "errorClass": error_class,
            "numFailedTasks": stage.get("numFailedTasks", 0),
            "numCompleteTasks": stage.get("numCompleteTasks", 0),
            "executorRunTime": stage.get("executorRunTime", 0),
            "remediationSteps": remediation
        })
    return summary


@mcp.tool
async def explain_stage_failure(
    app_id: str, 
    stage_id: int, 
    stage_attempt_id: int = 0
) -> Dict[str, Any]:
    """Deep-dive investigation of a single failed stage, retrieving task failure reasons and actionable remediation.
    
    Args:
        app_id: The Spark application ID
        stage_id: The stage ID to inspect
        stage_attempt_id: The stage attempt ID (default 0)
    """
    stage_details = await client.get_stage_details(app_id, stage_id, stage_attempt_id)
    version_info = await client.resolve_version(app_id=app_id)

    failure_reason = stage_details.get("failureReason", "")
    category, error_class, remediation = categorize_failure(failure_reason, version_info)

    # Collect task-level failures if present
    tasks_map = stage_details.get("tasks", {})
    failed_tasks = []
    if isinstance(tasks_map, dict):
        for task_id, task_data in tasks_map.items():
            if task_data.get("errorMessage") or task_data.get("status") == "FAILED":
                failed_tasks.append({
                    "taskId": task_id,
                    "host": task_data.get("host"),
                    "errorMessage": task_data.get("errorMessage"),
                })

    return {
        "stageId": stage_id,
        "attemptId": stage_attempt_id,
        "name": stage_details.get("name"),
        "status": stage_details.get("status"),
        "failureReason": failure_reason,
        "errorCategory": category,
        "errorClass": error_class,
        "numFailedTasks": stage_details.get("numFailedTasks", len(failed_tasks)),
        "numCompleteTasks": stage_details.get("numCompleteTasks", 0),
        "failedTaskSamples": failed_tasks[:5],
        "remediationSteps": remediation,
        "sparkVersion": version_info.raw_version
    }


@mcp.tool
async def analyze_application(app_id: str) -> Dict[str, Any]:
    """Perform a high-level diagnostic analysis of the Spark application to summarize execution health and Spark version details."""
    try:
        app = await client.get_application(app_id)
        jobs = await client.get_jobs(app_id)
        stages = await client.get_stages(app_id)
        version_info = await client.resolve_version(app_id=app_id)
    except Exception as e:
        return {"error": f"Failed to retrieve data for application {app_id}: {str(e)}"}
        
    failed_jobs = [j for j in jobs if j.get("status") == "FAILED"]
    failed_stages = [s for s in stages if s.get("status") == "FAILED"]
    
    total_duration_ms = 0
    if "attempts" in app and len(app["attempts"]) > 0:
        attempt = app["attempts"][0]
        total_duration_ms = attempt.get("duration", 0)
        
    major_failures = []
    for job in failed_jobs:
        major_failures.append({
            "type": "Job Failure",
            "id": job.get("jobId"),
            "name": job.get("name"),
            "description": f"Failed with stages: {job.get('stageIds')}. Failure Reason: {job.get('failureReason', 'Unknown')}"
        })
        
    for stage in failed_stages[:5]:
        reason = stage.get("failureReason", "No failure reason provided.")
        category, err_class, _ = categorize_failure(reason, version_info)
        major_failures.append({
            "type": "Stage Failure",
            "id": stage.get("stageId"),
            "name": stage.get("name"),
            "errorCategory": category,
            "errorClass": err_class,
            "description": reason
        })

    recommendations = []
    if len(failed_jobs) > 0 or len(failed_stages) > 0:
        recommendations.append("Application encountered job/stage failures. Run `find_failed_stages` to review root causes.")
    if version_info.major_version == SparkMajorVersion.SPARK_3_X and not version_info.aqe_active:
        recommendations.append("Consider enabling Adaptive Query Execution (`spark.sql.adaptive.enabled=true`) for dynamic optimization.")
        
    return {
        "appName": app.get("name"),
        "appId": app_id,
        "sparkMajorVersion": version_info.major_version.value,
        "sparkVersion": version_info.raw_version,
        "javaVersion": version_info.java_version,
        "scalaVersion": version_info.scala_version,
        "durationMs": total_duration_ms,
        "totalJobs": len(jobs),
        "failedJobsCount": len(failed_jobs),
        "totalStages": len(stages),
        "failedStagesCount": len(failed_stages),
        "status": "Healthy" if len(failed_jobs) == 0 else "Degraded/Failed",
        "majorFailures": major_failures,
        "recommendations": recommendations
    }


@mcp.tool
async def find_data_skew(
    app_id: str, 
    stage_id: int, 
    stage_attempt_id: int = 0
) -> Dict[str, Any]:
    """Analyze task summary metrics of a stage to detect partition, runtime, and spill skew with version-specific recommendations.
    
    Args:
        app_id: The unique Spark application ID
        stage_id: The stage ID to inspect
        stage_attempt_id: The stage attempt ID (default 0)
    """
    try:
        summary = await client.get_stage_task_summary(app_id, stage_id, stage_attempt_id)
        version_info = await client.resolve_version(app_id=app_id)
    except Exception as e:
        return {"error": f"Failed to retrieve task summary for stage {stage_id}: {str(e)}"}
        
    analysis = analyze_stage_skew(stage_id, stage_attempt_id, summary, version_info)
    return analysis.model_dump()


# --- Spark 4.x Migration & Compatibility Tools ---

@mcp.tool
async def check_spark_compatibility(app_id: str) -> Dict[str, Any]:
    """Audit a Spark 3.x application environment against Spark 4.x breaking changes and deprecated configurations.
    
    Args:
        app_id: The unique Spark application ID
    """
    try:
        env_data = await client.get_environment(app_id)
        version_info = await client.resolve_version(app_id=app_id)
    except Exception as e:
        return {"error": f"Failed to retrieve environment for application {app_id}: {str(e)}"}

    report = audit_spark4_compatibility(env_data, version_info)
    return report.model_dump()


# --- Livy-Next Interactive Session & Execution Tools ---

@mcp.tool
async def list_livy_sessions(
    from_idx: Optional[int] = 0, 
    limit: Optional[int] = 20
) -> Dict[str, Any]:
    """List active interactive sessions managed by Apache Livy-Next (Spark Connect).
    
    Args:
        from_idx: Starting index for session pagination (default: 0)
        limit: Maximum number of sessions to return (default: 20)
    """
    try:
        return await livy_client.list_sessions(from_idx=from_idx, limit=limit)
    except Exception as e:
        return {"error": f"Failed to list Livy sessions: {str(e)}"}


@mcp.tool
async def get_livy_session(session_id: int) -> Dict[str, Any]:
    """Get details, state, and Spark Connect application ID for a specific Livy-Next session.
    
    Args:
        session_id: Unique integer ID of the Livy session
    """
    try:
        return await livy_client.get_session(session_id)
    except Exception as e:
        return {"error": f"Failed to get Livy session {session_id}: {str(e)}"}


@mcp.tool
async def create_livy_session(
    name: Optional[str] = None,
    kind: str = "spark",
    proxy_user: Optional[str] = None,
    conf: Optional[Dict[str, str]] = None,
    jars: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Create a new interactive session in Livy-Next connecting to Spark Connect.
    
    Args:
        name: Optional descriptive session name
        kind: Session kind ('spark', 'sql', 'pyspark', 'sparkr', default: 'spark')
        proxy_user: Optional proxy user to run the session as
        conf: Optional dictionary of Spark configuration properties
        jars: Optional list of JAR files to include
    """
    try:
        return await livy_client.create_session(
            name=name,
            kind=kind,
            proxy_user=proxy_user,
            conf=conf,
            jars=jars
        )
    except Exception as e:
        return {"error": f"Failed to create Livy session: {str(e)}"}


@mcp.tool
async def delete_livy_session(session_id: int) -> Dict[str, Any]:
    """Terminate and delete an interactive Livy-Next session.
    
    Args:
        session_id: The ID of the session to terminate
    """
    try:
        return await livy_client.delete_session(session_id)
    except Exception as e:
        return {"error": f"Failed to delete Livy session {session_id}: {str(e)}"}


@mcp.tool
async def list_livy_statements(session_id: int) -> Dict[str, Any]:
    """List all statements executed or queued in a Livy-Next interactive session.
    
    Args:
        session_id: Unique integer ID of the Livy session
    """
    try:
        return await livy_client.list_statements(session_id)
    except Exception as e:
        return {"error": f"Failed to list statements for session {session_id}: {str(e)}"}


@mcp.tool
async def get_livy_statement(session_id: int, statement_id: int) -> Dict[str, Any]:
    """Get the execution state, progress, and results of a specific statement in a Livy-Next session.
    
    Args:
        session_id: The Livy session ID
        statement_id: The statement ID within the session
    """
    try:
        return await livy_client.get_statement(session_id, statement_id)
    except Exception as e:
        return {"error": f"Failed to get statement {statement_id} for session {session_id}: {str(e)}"}


@mcp.tool
async def submit_livy_statement(session_id: int, code: str) -> Dict[str, Any]:
    """Submit code or SQL asynchronously to an interactive Livy-Next session without waiting for completion.
    
    Args:
        session_id: The Livy session ID to execute in
        code: The SQL query or Spark code to execute
    """
    try:
        return await livy_client.submit_statement(session_id, code)
    except Exception as e:
        return {"error": f"Failed to submit statement to session {session_id}: {str(e)}"}


@mcp.tool
async def cancel_livy_statement(session_id: int, statement_id: int) -> Dict[str, Any]:
    """Cancel an active or queued statement execution in a Livy-Next session.
    
    Args:
        session_id: The Livy session ID
        statement_id: The statement ID to cancel
    """
    try:
        return await livy_client.cancel_statement(session_id, statement_id)
    except Exception as e:
        return {"error": f"Failed to cancel statement {statement_id} in session {session_id}: {str(e)}"}


@mcp.tool
async def run_livy_statement(
    session_id: int,
    code: str,
    timeout_seconds: float = 60.0
) -> Dict[str, Any]:
    """Submit code or SQL to a Livy-Next session, wait for completion, and return structured output with error diagnosis.
    
    If an error occurs (such as a Spark 4.0 ANSI SQL error or syntax error), the error is parsed
    and targeted remediation steps are provided.
    
    Args:
        session_id: The Livy session ID to run within
        code: SQL query or Spark statement to execute
        timeout_seconds: Max seconds to wait for execution to complete (default: 60.0)
    """
    try:
        stmt = await livy_client.run_statement_and_wait(
            session_id=session_id,
            code=code,
            timeout_seconds=timeout_seconds
        )
    except TimeoutError as te:
        return {
            "sessionId": session_id,
            "code": code,
            "state": "timeout",
            "error": str(te)
        }
    except Exception as e:
        return {"error": f"Failed to execute statement in session {session_id}: {str(e)}"}

    stmt_id = stmt.get("id")
    state = stmt.get("state", "unknown")
    started = stmt.get("started")
    completed = stmt.get("completed")
    duration_ms = (completed - started) if (started and completed) else None
    output = stmt.get("output") or {}

    status = output.get("status")
    data = output.get("data")
    ename = output.get("ename")
    evalue = output.get("evalue")
    traceback = output.get("traceback", [])

    summary: Dict[str, Any] = {
        "sessionId": session_id,
        "statementId": stmt_id,
        "code": code,
        "state": state,
        "status": status,
        "durationMs": duration_ms,
    }

    if status == "ok" and data:
        if "application/json" in data:
            json_data = data["application/json"]
            schema = json_data.get("schema", {})
            rows = json_data.get("data", [])
            summary["schema"] = schema.get("fields", [])
            summary["rowCount"] = len(rows)
            summary["previewRows"] = rows[:20]
        elif "text/plain" in data:
            summary["textOutput"] = data["text/plain"]
        else:
            summary["data"] = data
    elif status == "error" or state == "error":
        error_msg = evalue or "\n".join(traceback) or ename or "Unknown execution error"
        summary["errorName"] = ename
        summary["errorValue"] = evalue
        summary["traceback"] = traceback[:5]

        category, error_class, remediation = categorize_failure(error_msg, None)
        summary["errorCategory"] = category
        summary["errorClass"] = error_class
        summary["remediationSteps"] = remediation

    return summary


@mcp.tool
async def diagnose_livy_session(session_id: int) -> Dict[str, Any]:
    """Diagnose an active Livy-Next session by correlating session state with Spark History Server diagnostics.
    
    Retrieves the Spark application ID (`appId`) associated with the Livy session,
    analyzes recent statement executions and failures, and queries the History Server
    to assess overall application execution health.
    
    Args:
        session_id: The unique integer ID of the Livy-Next session
    """
    try:
        session_data = await livy_client.get_session(session_id)
    except Exception as e:
        return {"error": f"Failed to retrieve Livy session {session_id}: {str(e)}"}

    app_id = session_data.get("appId")
    session_state = session_data.get("state")
    session_kind = session_data.get("kind")
    log = session_data.get("log", [])
    app_info = session_data.get("appInfo", {})

    result: Dict[str, Any] = {
        "sessionId": session_id,
        "state": session_state,
        "kind": session_kind,
        "appId": app_id,
        "appInfo": app_info,
        "sessionLogTail": log[-10:] if log else [],
    }

    failed_statements = []
    try:
        stmts_resp = await livy_client.list_statements(session_id)
        statements = stmts_resp.get("statements", [])
        for stmt in statements:
            if stmt.get("state") == "error":
                out = stmt.get("output") or {}
                err_text = out.get("evalue") or out.get("ename") or "Statement error"
                cat, err_cls, rem = categorize_failure(err_text, None)
                failed_statements.append({
                    "statementId": stmt.get("id"),
                    "code": stmt.get("code"),
                    "errorCategory": cat,
                    "errorClass": err_cls,
                    "errorMessage": err_text,
                    "remediationSteps": rem,
                })
    except Exception as e:
        result["statementsError"] = f"Could not list statements: {str(e)}"

    result["failedStatementsCount"] = len(failed_statements)
    result["failedStatements"] = failed_statements[:5]

    if app_id:
        try:
            history_health = await analyze_application(app_id)
            result["sparkHistoryDiagnostics"] = history_health
        except Exception as e:
            result["sparkHistoryDiagnostics"] = {
                "warning": f"Could not correlate with Spark History Server for appId {app_id}: {str(e)}"
            }
    else:
        result["sparkHistoryDiagnostics"] = {
            "info": "No appId currently linked to this Livy session."
        }

    return result


def main():
    """Main entry point to run the FastMCP server."""
    transport = settings.spark_mcp_transport
    
    if transport == "sse":
        host = settings.spark_mcp_host
        port = settings.spark_mcp_port
        logger.info(f"Starting FastMCP server with SSE transport on {host}:{port}")
        mcp.run(transport="sse", host=host, port=port)
    else:
        logger.info("Starting FastMCP server with STDIO transport")
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
