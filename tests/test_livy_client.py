import pytest
import respx
import httpx
from sparklens.config import SparkLensSettings
from sparklens.client import SparkLivyNextClient

MOCK_LIVY_URL = "http://mock-livy:8998"

@pytest.fixture
def livy_client():
    config = SparkLensSettings(spark_livy_url=MOCK_LIVY_URL)
    return SparkLivyNextClient(config=config)


@pytest.mark.asyncio
@respx.mock
async def test_livy_list_sessions(livy_client):
    respx.get(f"{MOCK_LIVY_URL}/sessions").respond(
        status_code=200,
        json={
            "from": 0,
            "total": 1,
            "sessions": [
                {
                    "id": 1,
                    "state": "idle",
                    "kind": "spark",
                    "appId": "app-test-123"
                }
            ]
        }
    )

    resp = await livy_client.list_sessions(from_idx=0, limit=10)
    assert resp["total"] == 1
    assert resp["sessions"][0]["id"] == 1
    assert resp["sessions"][0]["appId"] == "app-test-123"


@pytest.mark.asyncio
@respx.mock
async def test_livy_get_session(livy_client):
    respx.get(f"{MOCK_LIVY_URL}/sessions/1").respond(
        status_code=200,
        json={
            "id": 1,
            "state": "idle",
            "kind": "spark",
            "appId": "app-test-123",
            "log": ["Session created"]
        }
    )

    resp = await livy_client.get_session(1)
    assert resp["id"] == 1
    assert resp["state"] == "idle"
    assert "Session created" in resp["log"]


@pytest.mark.asyncio
@respx.mock
async def test_livy_create_session(livy_client):
    route = respx.post(f"{MOCK_LIVY_URL}/sessions").respond(
        status_code=201,
        json={
            "id": 2,
            "name": "sql-session",
            "state": "starting",
            "kind": "sql"
        }
    )

    resp = await livy_client.create_session(name="sql-session", kind="sql", conf={"spark.sql.shuffle.partitions": "10"})
    assert route.called
    assert resp["id"] == 2
    assert resp["state"] == "starting"


@pytest.mark.asyncio
@respx.mock
async def test_livy_delete_session(livy_client):
    route = respx.delete(f"{MOCK_LIVY_URL}/sessions/2").respond(
        status_code=200,
        json={"msg": "deleted"}
    )

    resp = await livy_client.delete_session(2)
    assert route.called
    assert resp["msg"] == "deleted"


@pytest.mark.asyncio
@respx.mock
async def test_livy_list_statements(livy_client):
    respx.get(f"{MOCK_LIVY_URL}/sessions/1/statements").respond(
        status_code=200,
        json={
            "total_statements": 1,
            "statements": [
                {
                    "id": 0,
                    "code": "SELECT 1",
                    "state": "available"
                }
            ]
        }
    )

    resp = await livy_client.list_statements(1)
    assert resp["total_statements"] == 1
    assert resp["statements"][0]["code"] == "SELECT 1"


@pytest.mark.asyncio
@respx.mock
async def test_livy_get_statement(livy_client):
    respx.get(f"{MOCK_LIVY_URL}/sessions/1/statements/0").respond(
        status_code=200,
        json={
            "id": 0,
            "code": "SELECT 1",
            "state": "available",
            "output": {"status": "ok"}
        }
    )

    resp = await livy_client.get_statement(1, 0)
    assert resp["id"] == 0
    assert resp["state"] == "available"
    assert resp["output"]["status"] == "ok"


@pytest.mark.asyncio
@respx.mock
async def test_livy_submit_statement(livy_client):
    route = respx.post(f"{MOCK_LIVY_URL}/sessions/1/statements").respond(
        status_code=201,
        json={
            "id": 1,
            "code": "SHOW TABLES",
            "state": "waiting"
        }
    )

    resp = await livy_client.submit_statement(1, "SHOW TABLES")
    assert route.called
    assert resp["id"] == 1
    assert resp["state"] == "waiting"


@pytest.mark.asyncio
@respx.mock
async def test_livy_cancel_statement(livy_client):
    route = respx.post(f"{MOCK_LIVY_URL}/sessions/1/statements/1/cancel").respond(
        status_code=200,
        json={"msg": "cancelled"}
    )

    resp = await livy_client.cancel_statement(1, 1)
    assert route.called
    assert resp["msg"] == "cancelled"


@pytest.mark.asyncio
@respx.mock
async def test_livy_run_statement_and_wait_success(livy_client):
    respx.post(f"{MOCK_LIVY_URL}/sessions/1/statements").respond(
        status_code=201,
        json={"id": 10, "code": "SELECT 100", "state": "waiting"}
    )
    respx.get(f"{MOCK_LIVY_URL}/sessions/1/statements/10").respond(
        status_code=200,
        json={
            "id": 10,
            "code": "SELECT 100",
            "state": "available",
            "output": {
                "status": "ok",
                "data": {"application/json": {"data": [[100]]}}
            }
        }
    )

    resp = await livy_client.run_statement_and_wait(1, "SELECT 100", timeout_seconds=5.0, poll_interval=0.01)
    assert resp["id"] == 10
    assert resp["state"] == "available"
    assert resp["output"]["status"] == "ok"


@pytest.mark.asyncio
@respx.mock
async def test_livy_run_statement_and_wait_timeout(livy_client):
    respx.post(f"{MOCK_LIVY_URL}/sessions/1/statements").respond(
        status_code=201,
        json={"id": 11, "code": "SELECT sleep(10)", "state": "waiting"}
    )
    respx.get(f"{MOCK_LIVY_URL}/sessions/1/statements/11").respond(
        status_code=200,
        json={
            "id": 11,
            "code": "SELECT sleep(10)",
            "state": "running"
        }
    )

    with pytest.raises(TimeoutError) as exc_info:
        await livy_client.run_statement_and_wait(1, "SELECT sleep(10)", timeout_seconds=0.05, poll_interval=0.02)
    assert "timed out" in str(exc_info.value)


@pytest.mark.asyncio
@respx.mock
async def test_livy_client_error_handling(livy_client):
    respx.get(f"{MOCK_LIVY_URL}/sessions/999").respond(
        status_code=404,
        text="Session not found"
    )

    with pytest.raises(RuntimeError) as exc_info:
        await livy_client.get_session(999)
    assert "Livy API error (404)" in str(exc_info.value)


@pytest.mark.asyncio
@respx.mock
async def test_livy_create_session_with_connect_identity(livy_client):
    uuid_str = "6002ebfc-3aaf-4d3b-8f98-07b9ae46a51f"
    route = respx.post(f"{MOCK_LIVY_URL}/sessions").respond(
        status_code=201,
        json={
            "id": 10,
            "sessionId": uuid_str,
            "userId": "alice",
            "userAgent": "argus-worker",
            "state": "idle",
            "kind": "spark",
            "appInfo": {
                "sparkAppId": "app-connect-1",
                "sparkUiUrl": "http://localhost:4141",
                "sparkConnectUiUrl": f"http://localhost:4141/connect/session/?id={uuid_str}"
            }
        }
    )

    resp = await livy_client.create_session(
        name="etl-connect",
        kind="spark",
        user_id="alice",
        session_id=uuid_str,
        user_agent="argus-worker",
        token="my-token",
        conf={"spark.sql.shuffle.partitions": "200"}
    )
    assert route.called
    sent_json = route.calls.last.request.content.decode("utf-8")
    assert '"userId":"alice"' in sent_json or '"userId": "alice"' in sent_json
    assert f'"{uuid_str}"' in sent_json
    assert '"argus-worker"' in sent_json
    assert '"my-token"' in sent_json
    assert resp["sessionId"] == uuid_str
    assert resp["appInfo"]["sparkConnectUiUrl"] == f"http://localhost:4141/connect/session/?id={uuid_str}"


@pytest.mark.asyncio
@respx.mock
async def test_livy_get_and_delete_by_uuid(livy_client):
    uuid_str = "6002ebfc-3aaf-4d3b-8f98-07b9ae46a51f"
    respx.get(f"{MOCK_LIVY_URL}/sessions/{uuid_str}").respond(
        status_code=200,
        json={"id": 0, "sessionId": uuid_str, "state": "idle"}
    )
    respx.delete(f"{MOCK_LIVY_URL}/sessions/{uuid_str}").respond(
        status_code=200,
        json={"msg": "deleted"}
    )

    sess = await livy_client.get_session(uuid_str)
    assert sess["sessionId"] == uuid_str

    del_resp = await livy_client.delete_session(uuid_str)
    assert del_resp["msg"] == "deleted"


@pytest.mark.asyncio
@respx.mock
async def test_livy_submit_statement_with_tags(livy_client):
    route = respx.post(f"{MOCK_LIVY_URL}/sessions/1/statements").respond(
        status_code=201,
        json={
            "id": 7,
            "code": "SELECT 42",
            "state": "waiting",
            "tags": ["project:test", "team:data"]
        }
    )

    resp = await livy_client.submit_statement(1, "SELECT 42", tags=["project:test", "team:data"])
    assert route.called
    sent_json = route.calls.last.request.content.decode("utf-8")
    assert '"tags"' in sent_json
    assert '"project:test"' in sent_json
    assert resp["tags"] == ["project:test", "team:data"]


@pytest.mark.asyncio
@respx.mock
async def test_livy_list_statements_pagination(livy_client):
    route = respx.get(f"{MOCK_LIVY_URL}/sessions/1/statements").respond(
        status_code=200,
        json={
            "total_statements": 25,
            "statements": [{"id": 5, "code": "SELECT 5", "state": "available"}]
        }
    )

    resp = await livy_client.list_statements(1, from_idx=5, size=10)
    assert route.called
    assert route.calls.last.request.url.query == b"from=5&size=10"
    assert resp["total_statements"] == 25


@pytest.mark.asyncio
@respx.mock
async def test_livy_get_statement_row_pagination(livy_client):
    route = respx.get(f"{MOCK_LIVY_URL}/sessions/1/statements/0").respond(
        status_code=200,
        json={
            "id": 0,
            "code": "SELECT * FROM large_table",
            "state": "available",
            "output": {
                "status": "ok",
                "data": {
                    "application/json": {
                        "schema": {"fields": [{"name": "id", "type": "integer"}]},
                        "data": [[21], [22]],
                        "total": 100,
                        "from": 20,
                        "size": 2
                    }
                }
            }
        }
    )

    resp = await livy_client.get_statement(1, 0, from_row=20, size=2)
    assert route.called
    assert route.calls.last.request.url.query == b"from=20&size=2"
    json_out = resp["output"]["data"]["application/json"]
    assert json_out["total"] == 100
    assert json_out["from"] == 20
    assert json_out["size"] == 2
    assert len(json_out["data"]) == 2


@pytest.mark.asyncio
@respx.mock
async def test_livy_run_statement_and_wait_with_tags_and_pagination(livy_client):
    respx.post(f"{MOCK_LIVY_URL}/sessions/1/statements").respond(
        status_code=201,
        json={"id": 12, "code": "SELECT * FROM range(100)", "state": "waiting", "tags": ["tag1"]}
    )
    respx.get(f"{MOCK_LIVY_URL}/sessions/1/statements/12").respond(
        status_code=200,
        json={
            "id": 12,
            "code": "SELECT * FROM range(100)",
            "state": "available",
            "tags": ["tag1"],
            "output": {
                "status": "ok",
                "data": {
                    "application/json": {
                        "schema": {"fields": [{"name": "id", "type": "long"}]},
                        "data": [[0], [1], [2]],
                        "total": 100,
                        "from": 0,
                        "size": 3
                    }
                }
            }
        }
    )

    resp = await livy_client.run_statement_and_wait(
        1,
        "SELECT * FROM range(100)",
        tags=["tag1"],
        timeout_seconds=5.0,
        poll_interval=0.01,
        from_row=0,
        size=3
    )
    assert resp["id"] == 12
    assert resp["tags"] == ["tag1"]
    assert resp["output"]["data"]["application/json"]["total"] == 100
