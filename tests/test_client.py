import pytest
import respx
import httpx
from sparklens.config import SparkLensSettings
from sparklens.client import SparkHistoryClient
from sparklens.version import SparkMajorVersion

@pytest.mark.asyncio
@respx.mock
async def test_client_get_applications():
    config = SparkLensSettings(spark_history_url="http://mock-history:18080")
    client = SparkHistoryClient(config=config)

    respx.get("http://mock-history:18080/api/v1/applications").respond(
        status_code=200,
        json=[
            {"id": "app-1", "name": "App 1"},
            {"id": "app-2", "name": "App 2"}
        ]
    )

    apps = await client.get_applications()
    assert len(apps) == 2
    assert apps[0]["id"] == "app-1"


@pytest.mark.asyncio
@respx.mock
async def test_client_basic_auth():
    config = SparkLensSettings(
        spark_history_url="http://mock-history:18080",
        spark_auth_type="basic",
        spark_username="admin",
        spark_password="secretpassword"
    )
    client = SparkHistoryClient(config=config)

    route = respx.get("http://mock-history:18080/api/v1/applications").respond(
        status_code=200,
        json=[]
    )

    await client.get_applications()
    assert route.called
    auth_header = route.calls.last.request.headers.get("Authorization")
    assert auth_header is not None
    assert auth_header.startswith("Basic ")


@pytest.mark.asyncio
@respx.mock
async def test_client_oauth_token():
    config = SparkLensSettings(
        spark_history_url="http://mock-history:18080",
        spark_auth_type="oauth",
        spark_access_token="bearer-token-12345"
    )
    client = SparkHistoryClient(config=config)

    route = respx.get("http://mock-history:18080/api/v1/applications").respond(
        status_code=200,
        json=[]
    )

    await client.get_applications()
    assert route.called
    assert route.calls.last.request.headers.get("Authorization") == "Bearer bearer-token-12345"


@pytest.mark.asyncio
@respx.mock
async def test_client_resolve_version_from_app(mock_spark4_env):
    config = SparkLensSettings(spark_history_url="http://mock-history:18080")
    client = SparkHistoryClient(config=config)

    respx.get("http://mock-history:18080/api/v1/applications/app-test/environment").respond(
        status_code=200,
        json=mock_spark4_env
    )

    ver_info = await client.resolve_version(app_id="app-test")
    assert ver_info.major_version == SparkMajorVersion.SPARK_4_X
    assert ver_info.raw_version == "4.0.0"
    assert ver_info.features.structured_logging is True


@pytest.mark.asyncio
@respx.mock
async def test_client_error_handling():
    config = SparkLensSettings(spark_history_url="http://mock-history:18080")
    client = SparkHistoryClient(config=config)

    respx.get("http://mock-history:18080/api/v1/applications/non-existent").respond(
        status_code=404,
        text="Application not found"
    )

    with pytest.raises(RuntimeError) as exc_info:
        await client.get_application("non-existent")
    assert "Spark History API error (404)" in str(exc_info.value)
