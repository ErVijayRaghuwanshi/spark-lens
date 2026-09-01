"""SparkLens: An MCP server for Apache Spark 3.x and Spark 4.x observability and troubleshooting."""

from sparklens.config import settings, SparkLensSettings
from sparklens.client import client, SparkHistoryClient
from sparklens.version import SparkMajorVersion, SparkVersionInfo, SparkFeatures
from sparklens.server import mcp

__version__ = "0.2.0"
__all__ = [
    "settings",
    "SparkLensSettings",
    "client",
    "SparkHistoryClient",
    "SparkMajorVersion",
    "SparkVersionInfo",
    "SparkFeatures",
    "mcp",
]
