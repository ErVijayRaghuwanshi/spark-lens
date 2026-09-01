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
    explain_sql_query
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
        "check_spark_compatibility"
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

    res = audit_spark4_migration("app-123")
    assert "app-123" in res
    assert "check_spark_compatibility" in res


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
