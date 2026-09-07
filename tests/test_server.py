import pytest
import respx
from sparklens.config import settings
from sparklens.server import (
    mcp,
    get_spark_version,
    analyze_application,
    check_spark_compatibility,
    find_data_skew,
    find_failed_stages,
    diagnose_application,
    audit_spark4_migration,
    optimize_application,
    explain_sql_query,
    list_livy_sessions,
    get_livy_session,
    create_livy_session,
    delete_livy_session,
    list_livy_statements,
    get_livy_statement,
    submit_livy_statement,
    cancel_livy_statement,
    run_livy_statement,
    diagnose_livy_session,
    troubleshoot_livy_session,
    execute_and_verify_sql,
)

@pytest.mark.asyncio
async def test_mcp_tool_registration():
    tools = await mcp.list_tools()
    tool_names = [tool.name for tool in tools]
    expected_tools = [
        "get_spark_version",
        "list_applications",
        "get_application",
        "get_jobs",
        "get_stages",
        "get_stage_details",
        "get_executors",
        "get_environment",
        "list_sql_queries",
        "get_sql_query_details",
        "find_failed_stages",
        "explain_stage_failure",
        "analyze_application",
        "find_data_skew",
        "check_spark_compatibility",
        "list_livy_sessions",
        "get_livy_session",
        "create_livy_session",
        "delete_livy_session",
        "list_livy_statements",
        "get_livy_statement",
        "submit_livy_statement",
        "cancel_livy_statement",
        "run_livy_statement",
        "diagnose_livy_session",
    ]
    for expected in expected_tools:
        assert expected in tool_names, f"Tool {expected} not registered in FastMCP"


@pytest.mark.asyncio
async def test_mcp_prompt_registration():
    prompts = await mcp.list_prompts()
    prompt_names = [prompt.name for prompt in prompts]
    assert "diagnose_application" in prompt_names
    assert "optimize_application" in prompt_names
    assert "audit_spark4_migration" in prompt_names
    assert "explain_sql_query" in prompt_names
    assert "troubleshoot_livy_session" in prompt_names
    assert "execute_and_verify_sql" in prompt_names

    res = audit_spark4_migration("app-123")
    assert "app-123" in res
    assert "check_spark_compatibility" in res

    livy_prompt = troubleshoot_livy_session(3)
    assert "3" in livy_prompt
    assert "diagnose_livy_session" in livy_prompt

    sql_prompt = execute_and_verify_sql(3, "SELECT 1/0")
    assert "SELECT 1/0" in sql_prompt
    assert "run_livy_statement" in sql_prompt


@pytest.mark.asyncio
@respx.mock
async def test_get_spark_version_tool(mock_spark4_env):
    respx.get(f"{settings.spark_history_url}/api/v1/applications/app-4x/environment").respond(
        status_code=200,
        json=mock_spark4_env
    )

    ver = await get_spark_version(app_id="app-4x")
    assert ver["major_version"] == "4.x"
    assert ver["raw_version"] == "4.0.0"
    assert ver["features"]["ansi_sql_default"] is True


@pytest.mark.asyncio
@respx.mock
async def test_analyze_application_tool(mock_spark4_env):
    respx.get(f"{settings.spark_history_url}/api/v1/applications/app-4x").respond(
        status_code=200,
        json={"name": "SparkETL-4x", "attempts": [{"duration": 120000}]}
    )
    respx.get(f"{settings.spark_history_url}/api/v1/applications/app-4x/jobs").respond(
        status_code=200,
        json=[{"jobId": 1, "status": "SUCCEEDED"}]
    )
    respx.get(f"{settings.spark_history_url}/api/v1/applications/app-4x/stages").respond(
        status_code=200,
        json=[{"stageId": 1, "status": "COMPLETE"}]
    )
    respx.get(f"{settings.spark_history_url}/api/v1/applications/app-4x/environment").respond(
        status_code=200,
        json=mock_spark4_env
    )

    result = await analyze_application("app-4x")
    assert result["status"] == "Healthy"
    assert result["sparkMajorVersion"] == "4.x"
    assert result["failedJobsCount"] == 0


@pytest.mark.asyncio
@respx.mock
async def test_find_failed_stages_with_spark4_ansi_error(mock_spark4_env):
    respx.get(f"{settings.spark_history_url}/api/v1/applications/app-4x/stages").respond(
        status_code=200,
        json=[
            {
                "stageId": 2,
                "attemptId": 0,
                "name": "calc_ratios",
                "status": "FAILED",
                "failureReason": "org.apache.spark.SparkArithmeticException: [CANNOT_DIVIDE_BY_ZERO] Division by zero.",
                "numFailedTasks": 1,
                "numCompleteTasks": 40,
                "executorRunTime": 5000
            }
        ]
    )
    respx.get(f"{settings.spark_history_url}/api/v1/applications/app-4x/environment").respond(
        status_code=200,
        json=mock_spark4_env
    )

    failed = await find_failed_stages("app-4x")
    assert len(failed) == 1
    assert failed[0]["errorCategory"] == "ANSI_SQL_ERROR"
    assert failed[0]["errorClass"] == "CANNOT_DIVIDE_BY_ZERO"
    assert any("try_divide" in r for r in failed[0]["remediationSteps"])


@pytest.mark.asyncio
@respx.mock
async def test_check_spark_compatibility_tool(mock_spark3_env):
    respx.get(f"{settings.spark_history_url}/api/v1/applications/app-3x/environment").respond(
        status_code=200,
        json=mock_spark3_env
    )

    report = await check_spark_compatibility("app-3x")
    assert report["target_version"] == "4.x"
    assert report["status"] == "HIGH_RISK"
    assert report["critical_count"] >= 3


@pytest.mark.asyncio
@respx.mock
async def test_list_and_get_livy_session_tools():
    respx.get(f"{settings.spark_livy_url}/sessions").respond(
        status_code=200,
        json={"total": 1, "sessions": [{"id": 1, "state": "idle", "kind": "spark"}]}
    )
    respx.get(f"{settings.spark_livy_url}/sessions/1").respond(
        status_code=200,
        json={"id": 1, "state": "idle", "kind": "spark", "appId": "app-livy-1"}
    )

    sessions = await list_livy_sessions()
    assert sessions["total"] == 1
    assert sessions["sessions"][0]["id"] == 1

    session = await get_livy_session(1)
    assert session["id"] == 1
    assert session["appId"] == "app-livy-1"


@pytest.mark.asyncio
@respx.mock
async def test_run_livy_statement_tool_success():
    respx.post(f"{settings.spark_livy_url}/sessions/1/statements").respond(
        status_code=201,
        json={"id": 5, "code": "SELECT 1", "state": "waiting"}
    )
    respx.get(f"{settings.spark_livy_url}/sessions/1/statements/5").respond(
        status_code=200,
        json={
            "id": 5,
            "code": "SELECT 1",
            "state": "available",
            "output": {
                "status": "ok",
                "data": {
                    "application/json": {
                        "schema": {"fields": [{"name": "1", "type": "integer"}]},
                        "data": [[1]]
                    }
                }
            },
            "started": 1000,
            "completed": 1200
        }
    )

    result = await run_livy_statement(1, "SELECT 1")
    assert result["sessionId"] == 1
    assert result["statementId"] == 5
    assert result["status"] == "ok"
    assert result["durationMs"] == 200
    assert result["rowCount"] == 1
    assert result["previewRows"] == [[1]]


@pytest.mark.asyncio
@respx.mock
async def test_run_livy_statement_tool_ansi_error():
    respx.post(f"{settings.spark_livy_url}/sessions/1/statements").respond(
        status_code=201,
        json={"id": 6, "code": "SELECT 1/0", "state": "waiting"}
    )
    respx.get(f"{settings.spark_livy_url}/sessions/1/statements/6").respond(
        status_code=200,
        json={
            "id": 6,
            "code": "SELECT 1/0",
            "state": "error",
            "output": {
                "status": "error",
                "ename": "SparkExecutionError",
                "evalue": "execution error: [DIVIDE_BY_ZERO] Division by zero.",
                "traceback": ["execution error: [DIVIDE_BY_ZERO] Division by zero."]
            },
            "started": 1000,
            "completed": 1050
        }
    )

    result = await run_livy_statement(1, "SELECT 1/0")
    assert result["sessionId"] == 1
    assert result["statementId"] == 6
    assert result["status"] == "error"
    assert result["errorCategory"] == "ANSI_SQL_ERROR"
    assert result["errorClass"] == "DIVIDE_BY_ZERO"
    assert any("try_divide" in step for step in result["remediationSteps"])


@pytest.mark.asyncio
@respx.mock
async def test_diagnose_livy_session_tool(mock_spark4_env):
    respx.get(f"{settings.spark_livy_url}/sessions/3").respond(
        status_code=200,
        json={
            "id": 3,
            "state": "idle",
            "kind": "spark",
            "appId": "app-corr-1",
            "log": ["Session created", "Error encountered in statement"],
            "appInfo": {"sparkUiUrl": "http://localhost:18088/history/app-corr-1"}
        }
    )
    respx.get(f"{settings.spark_livy_url}/sessions/3/statements").respond(
        status_code=200,
        json={
            "total_statements": 1,
            "statements": [
                {
                    "id": 0,
                    "code": "SELECT 1/0",
                    "state": "error",
                    "output": {"evalue": "execution error: [DIVIDE_BY_ZERO] Division by zero."}
                }
            ]
        }
    )

    # History server endpoints for app-corr-1
    respx.get(f"{settings.spark_history_url}/api/v1/applications/app-corr-1").respond(
        status_code=200,
        json={"name": "LivyConnectApp", "attempts": [{"duration": 50000}]}
    )
    respx.get(f"{settings.spark_history_url}/api/v1/applications/app-corr-1/jobs").respond(
        status_code=200,
        json=[]
    )
    respx.get(f"{settings.spark_history_url}/api/v1/applications/app-corr-1/stages").respond(
        status_code=200,
        json=[]
    )
    respx.get(f"{settings.spark_history_url}/api/v1/applications/app-corr-1/environment").respond(
        status_code=200,
        json=mock_spark4_env
    )

    diag = await diagnose_livy_session(3)
    assert diag["sessionId"] == 3
    assert diag["appId"] == "app-corr-1"
    assert diag["failedStatementsCount"] == 1
    assert diag["failedStatements"][0]["errorCategory"] == "ANSI_SQL_ERROR"
    assert diag["failedStatements"][0]["errorClass"] == "DIVIDE_BY_ZERO"
    assert "sparkHistoryDiagnostics" in diag
    assert diag["sparkHistoryDiagnostics"]["appName"] == "LivyConnectApp"

