import asyncio
import logging
import sys
from typing import List, Dict, Any, Optional
import httpx

from sparklens.config import SparkLensSettings, settings
from sparklens.version import (
    SparkVersionInfo,
    extract_spark_version_from_env,
    extract_spark_version_from_version_endpoint,
    parse_spark_version,
    get_features_for_version,
)

logger = logging.getLogger("sparklens.client")

class SparkLivyNextClient:
    """Async client to interact with the Apache Livy-Next REST API."""

    def __init__(self, config: Optional[SparkLensSettings] = None):
        self.config = config or settings

    async def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Sends a GET request to the Livy REST API."""
        url = f"{self.config.spark_livy_url}{path}"
        headers = {"Accept": "application/json"}

        logger.info(f"Querying Livy API: {url} with params {params}")

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(url, params=params, headers=headers)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP {e.response.status_code} error from {url}: {e.response.text}")
                raise RuntimeError(f"Livy API error ({e.response.status_code}): {e.response.text}") from e
            except httpx.RequestError as e:
                logger.error(f"Network/Connection error querying {url}: {e}")
                raise RuntimeError(f"Failed to connect to Livy Server at {self.config.spark_livy_url}: {e}") from e

    async def post(self, path: str, json_body: Optional[Dict[str, Any]] = None) -> Any:
        """Sends a POST request to the Livy REST API."""
        url = f"{self.config.spark_livy_url}{path}"
        headers = {"Accept": "application/json", "Content-Type": "application/json"}

        logger.info(f"Posting to Livy API: {url} with payload {json_body}")

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.post(url, json=json_body or {}, headers=headers)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP {e.response.status_code} error from {url}: {e.response.text}")
                raise RuntimeError(f"Livy API error ({e.response.status_code}): {e.response.text}") from e
            except httpx.RequestError as e:
                logger.error(f"Network/Connection error posting to {url}: {e}")
                raise RuntimeError(f"Failed to connect to Livy Server at {self.config.spark_livy_url}: {e}") from e

    async def delete(self, path: str) -> Any:
        """Sends a DELETE request to the Livy REST API."""
        url = f"{self.config.spark_livy_url}{path}"
        headers = {"Accept": "application/json"}

        logger.info(f"Deleting on Livy API: {url}")

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.delete(url, headers=headers)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP {e.response.status_code} error from {url}: {e.response.text}")
                raise RuntimeError(f"Livy API error ({e.response.status_code}): {e.response.text}") from e
            except httpx.RequestError as e:
                logger.error(f"Network/Connection error deleting on {url}: {e}")
                raise RuntimeError(f"Failed to connect to Livy Server at {self.config.spark_livy_url}: {e}") from e

    async def list_sessions(self, from_idx: Optional[int] = None, limit: Optional[int] = None) -> Dict[str, Any]:
        """List active interactive sessions in Livy-Next."""
        params: Dict[str, Any] = {}
        if from_idx is not None:
            params["from"] = from_idx
        if limit is not None:
            params["size"] = limit
        return await self.get("/sessions", params=params if params else None)

    async def get_session(self, session_id: int) -> Dict[str, Any]:
        """Get details and state of a specific session."""
        return await self.get(f"/sessions/{session_id}")

    async def create_session(
        self,
        name: Optional[str] = None,
        kind: str = "spark",
        proxy_user: Optional[str] = None,
        jars: Optional[List[str]] = None,
        conf: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Create a new interactive session and connect to Spark Connect."""
        payload: Dict[str, Any] = {"kind": kind}
        if name:
            payload["name"] = name
        if proxy_user:
            payload["proxyUser"] = proxy_user
        if jars:
            payload["jars"] = jars
        if conf:
            payload["conf"] = conf
        return await self.post("/sessions", json_body=payload)

    async def delete_session(self, session_id: int) -> Dict[str, Any]:
        """Close and terminate a specific interactive session."""
        return await self.delete(f"/sessions/{session_id}")

    async def list_statements(self, session_id: int) -> Dict[str, Any]:
        """List all statements submitted to a session."""
        return await self.get(f"/sessions/{session_id}/statements")

    async def get_statement(self, session_id: int, statement_id: int) -> Dict[str, Any]:
        """Get execution state, progress, and results of a statement."""
        return await self.get(f"/sessions/{session_id}/statements/{statement_id}")

    async def submit_statement(self, session_id: int, code: str) -> Dict[str, Any]:
        """Submit code or SQL statement for execution in a session."""
        return await self.post(f"/sessions/{session_id}/statements", json_body={"code": code})

    async def cancel_statement(self, session_id: int, statement_id: int) -> Dict[str, Any]:
        """Cancel a statement execution inside a session."""
        return await self.post(f"/sessions/{session_id}/statements/{statement_id}/cancel")

    async def run_statement_and_wait(
        self,
        session_id: int,
        code: str,
        timeout_seconds: float = 60.0,
        poll_interval: float = 0.5,
    ) -> Dict[str, Any]:
        """Submit statement and poll until terminal state (available, error, cancelled) or timeout."""
        stmt = await self.submit_statement(session_id, code)
        stmt_id = stmt["id"]

        elapsed = 0.0
        while elapsed < timeout_seconds:
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

            curr_stmt = await self.get_statement(session_id, stmt_id)
            state = curr_stmt.get("state")
            if state in ["available", "error", "cancelled"]:
                return curr_stmt

        raise TimeoutError(
            f"Statement {stmt_id} in session {session_id} timed out after {timeout_seconds}s (last state: {curr_stmt.get('state')})"
        )


class SparkHistoryClient:
    """Async client to interact with the Apache Spark History Server REST API."""

    def __init__(self, config: Optional[SparkLensSettings] = None):
        self.config = config or settings

    def _build_headers_and_auth(self) -> tuple[Dict[str, str], Optional[tuple[str, str]]]:
        headers = {"Accept": "application/json"}
        auth = None

        if self.config.spark_auth_type == "basic" and self.config.spark_username and self.config.spark_password:
            auth = (self.config.spark_username, self.config.spark_password)
        elif self.config.spark_auth_type == "oauth" and self.config.spark_access_token:
            headers["Authorization"] = f"Bearer {self.config.spark_access_token}"

        return headers, auth

    async def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Sends a GET request to the Spark History Server REST API (/api/v1/...)."""
        url = f"{self.config.spark_history_url}/api/v1{path}"
        headers, auth = self._build_headers_and_auth()

        logger.info(f"Querying Spark API: {url} with params {params}")

        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                response = await client.get(url, params=params, headers=headers, auth=auth)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP {e.response.status_code} error from {url}: {e.response.text}")
                raise RuntimeError(f"Spark History API error ({e.response.status_code}): {e.response.text}") from e
            except httpx.RequestError as e:
                logger.error(f"Network/Connection error querying {url}: {e}")
                raise RuntimeError(f"Failed to connect to Spark History Server at {self.config.spark_history_url}: {e}") from e

    async def get_server_version(self) -> Dict[str, Any]:
        """Fetch server-level version from `/api/v1/version` endpoint."""
        try:
            return await self.get("/version")
        except Exception as e:
            logger.warning(f"Could not fetch /version endpoint: {e}")
            return {"spark": "unknown"}

    async def get_applications(self, status: Optional[str] = None, limit: Optional[int] = 20) -> List[Dict[str, Any]]:
        """List applications from Spark History Server."""
        params: Dict[str, Any] = {}
        if status in ["completed", "running"]:
            params["status"] = status
        apps = await self.get("/applications", params=params)
        if isinstance(apps, list):
            return apps[:limit] if limit else apps
        return []

    async def get_application(self, app_id: str) -> Dict[str, Any]:
        """Get application metadata and attempts."""
        return await self.get(f"/applications/{app_id}")

    async def get_jobs(self, app_id: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get jobs for a given Spark application."""
        params = {}
        if status:
            params["status"] = status
        return await self.get(f"/applications/{app_id}/jobs", params=params)

    async def get_stages(self, app_id: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all stages for a given Spark application."""
        params = {}
        if status:
            params["status"] = status
        return await self.get(f"/applications/{app_id}/stages", params=params)

    async def get_stage_details(self, app_id: str, stage_id: int, stage_attempt_id: int = 0) -> Dict[str, Any]:
        """Get detailed execution data for a specific stage attempt."""
        return await self.get(f"/applications/{app_id}/stages/{stage_id}/{stage_attempt_id}")

    async def get_stage_task_summary(
        self, 
        app_id: str, 
        stage_id: int, 
        stage_attempt_id: int = 0,
        quantiles: str = "0.0,0.25,0.5,0.75,0.95,1.0"
    ) -> Dict[str, Any]:
        """Get task metrics summary quantiles for a stage."""
        return await self.get(
            f"/applications/{app_id}/stages/{stage_id}/{stage_attempt_id}/taskSummary",
            params={"quantiles": quantiles}
        )

    async def get_executors(self, app_id: str) -> List[Dict[str, Any]]:
        """Get list of active and dead executors."""
        return await self.get(f"/applications/{app_id}/executors")

    async def get_environment(self, app_id: str) -> Dict[str, Any]:
        """Get Spark environment, JVM settings, classpath, and properties."""
        return await self.get(f"/applications/{app_id}/environment")

    async def get_sql_queries(self, app_id: str, limit: Optional[int] = 20) -> List[Dict[str, Any]]:
        """List SQL query executions (summaries without heavy plans)."""
        queries = await self.get(f"/applications/{app_id}/sql", params={"details": "false"})
        if isinstance(queries, list):
            return queries[:limit] if limit else queries
        return []

    async def get_sql_query_details(self, app_id: str, sql_id: int) -> Dict[str, Any]:
        """Get detailed SQL query execution plans."""
        return await self.get(f"/applications/{app_id}/sql/{sql_id}", params={"details": "true"})

    async def resolve_version(self, app_id: Optional[str] = None) -> SparkVersionInfo:
        """Dynamically resolve the Spark version and runtime features for an application or server."""
        if app_id:
            try:
                env_data = await self.get_environment(app_id)
                return extract_spark_version_from_env(env_data, default_fallback=self.config.spark_default_version)
            except Exception as e:
                logger.warning(f"Could not retrieve /environment for {app_id}, trying server /version: {e}")

        # Fallback to server version endpoint
        try:
            ver_data = await self.get_server_version()
            return extract_spark_version_from_version_endpoint(ver_data, default_fallback=self.config.spark_default_version)
        except Exception as e:
            logger.warning(f"Could not retrieve server /version: {e}")
            major, minor, patch, major_enum = parse_spark_version(self.config.spark_default_version)
            features = get_features_for_version(major_enum, minor)
            return SparkVersionInfo(
                raw_version=self.config.spark_default_version,
                major=major,
                minor=minor,
                patch=patch,
                major_version=major_enum,
                features=features
            )


# Default global client instances
client = SparkHistoryClient()
livy_client = SparkLivyNextClient()
