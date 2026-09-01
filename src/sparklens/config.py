import os
from typing import Optional
from pydantic import BaseModel, Field

class SparkLensSettings(BaseModel):
    """Configuration settings for SparkLens MCP server."""
    spark_history_url: str = Field(
        default="http://localhost:18080",
        description="Root URL of the Apache Spark History Server."
    )
    spark_auth_type: str = Field(
        default="none",
        description="Authentication mechanism: 'none', 'basic', or 'oauth'."
    )
    spark_username: Optional[str] = Field(
        default=None,
        description="Username for basic authentication."
    )
    spark_password: Optional[str] = Field(
        default=None,
        description="Password for basic authentication."
    )
    spark_access_token: Optional[str] = Field(
        default=None,
        description="Bearer token for OAuth/Token authentication."
    )
    spark_default_version: str = Field(
        default="auto",
        description="Default Spark version fallback: 'auto', '3.x', or '4.x'."
    )
    spark_mcp_transport: str = Field(
        default="stdio",
        description="Transport type for FastMCP server: 'stdio' or 'sse'."
    )
    spark_mcp_host: str = Field(
        default="0.0.0.0",
        description="Host for FastMCP server when using SSE transport."
    )
    spark_mcp_port: int = Field(
        default=8030,
        description="Port for FastMCP server when using SSE transport."
    )

    @classmethod
    def from_env(cls) -> "SparkLensSettings":
        """Load configuration from environment variables."""
        return cls(
            spark_history_url=os.environ.get("SPARK_HISTORY_URL", "http://localhost:18080").rstrip("/"),
            spark_auth_type=os.environ.get("SPARK_AUTH_TYPE", "none").lower(),
            spark_username=os.environ.get("SPARK_USERNAME") or None,
            spark_password=os.environ.get("SPARK_PASSWORD") or None,
            spark_access_token=os.environ.get("SPARK_ACCESS_TOKEN") or None,
            spark_default_version=os.environ.get("SPARK_DEFAULT_VERSION", "auto").lower(),
            spark_mcp_transport=os.environ.get("SPARK_MCP_TRANSPORT", "stdio").lower(),
            spark_mcp_host=os.environ.get("SPARK_MCP_HOST", "0.0.0.0"),
            spark_mcp_port=int(os.environ.get("SPARK_MCP_PORT", "8030")),
        )

# Global settings instance
settings = SparkLensSettings.from_env()
