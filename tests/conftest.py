import pytest
from typing import Dict, Any

@pytest.fixture
def mock_spark3_env() -> Dict[str, Any]:
    """Sample /environment response for a Spark 3.5.1 application."""
    return {
        "appId": "app-20260901100000-0001",
        "runtime": {
            "sparkVersion": "3.5.1",
            "javaVersion": "1.8.0_382 (Azul Systems, Inc.)",
            "scalaVersion": "2.12.18"
        },
        "sparkProperties": [
            ["spark.app.name", "SparkPi-3x"],
            ["spark.app.id", "app-20260901100000-0001"],
            ["spark.driver.memory", "2g"],
            ["spark.executor.memory", "4g"],
            ["spark.sql.adaptive.enabled", "false"],
            ["spark.shuffle.service.db.backend", "LEVELDB"],
            ["spark.driver.extraJavaOptions", "-XX:+UseConcMarkSweepGC -Dlog4j.debug"],
            ["spark.sql.legacy.timeParserPolicy", "LEGACY"]
        ],
        "systemProperties": [
            ["java.version", "1.8.0_382"],
            ["scala.version", "2.12.18"],
            ["spark.version", "3.5.1"]
        ]
    }

@pytest.fixture
def mock_spark4_env() -> Dict[str, Any]:
    """Sample /environment response for a Spark 4.0.0 application."""
    return {
        "appId": "app-20260901200000-0002",
        "runtime": {
            "sparkVersion": "4.0.0",
            "javaVersion": "17.0.10 (Eclipse Adoptium)",
            "scalaVersion": "2.13.13"
        },
        "sparkProperties": [
            ["spark.app.name", "SparkETL-4x"],
            ["spark.app.id", "app-20260901200000-0002"],
            ["spark.driver.memory", "4g"],
            ["spark.executor.memory", "8g"],
            ["spark.sql.adaptive.enabled", "true"],
            ["spark.sql.ansi.enabled", "true"],
            ["spark.shuffle.service.db.backend", "ROCKSDB"],
            ["spark.driver.extraJavaOptions", "-XX:+UseG1GC"]
        ],
        "systemProperties": [
            ["java.version", "17.0.10"],
            ["scala.version", "2.13.13"],
            ["spark.version", "4.0.0"]
        ]
    }

@pytest.fixture
def mock_skewed_task_summary() -> Dict[str, Any]:
    """Task summary with severe runtime and spill skew."""
    return {
        "quantiles": [0.0, 0.25, 0.5, 0.75, 0.95, 1.0],
        "executorRunTime": {
            "quantiles": [120.0, 300.0, 500.0, 1200.0, 45000.0, 95000.0] # median 500ms, max 95000ms -> ratio 190x
        },
        "jvmGcTime": {
            "quantiles": [10.0, 20.0, 50.0, 150.0, 2000.0, 8000.0]
        },
        "memoryBytesSpilled": {
            "quantiles": [0.0, 0.0, 0.0, 1024000.0, 52428800.0, 209715200.0]
        },
        "diskBytesSpilled": {
            "quantiles": [0.0, 0.0, 0.0, 0.0, 10485760.0, 104857600.0]
        }
    }

@pytest.fixture
def mock_uniform_task_summary() -> Dict[str, Any]:
    """Task summary with evenly distributed execution."""
    return {
        "quantiles": [0.0, 0.25, 0.5, 0.75, 0.95, 1.0],
        "executorRunTime": {
            "quantiles": [800.0, 950.0, 1000.0, 1050.0, 1100.0, 1200.0] # ratio 1.2x
        },
        "jvmGcTime": {
            "quantiles": [10.0, 15.0, 20.0, 25.0, 30.0, 40.0]
        },
        "memoryBytesSpilled": {
            "quantiles": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        },
        "diskBytesSpilled": {
            "quantiles": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        }
    }
