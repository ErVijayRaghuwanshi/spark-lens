import re
from enum import Enum
from typing import Optional, Dict, Any, List, Tuple
from pydantic import BaseModel, Field


class SparkMajorVersion(str, Enum):
    """Supported Spark Major Versions."""
    SPARK_3_X = "3.x"
    SPARK_4_X = "4.x"
    UNKNOWN = "unknown"


class SparkFeatures(BaseModel):
    """Capabilities and default settings associated with a Spark major/minor version."""
    ansi_sql_default: bool = Field(
        description="Whether ANSI SQL mode (spark.sql.ansi.enabled) is enabled by default."
    )
    aqe_default: bool = Field(
        description="Whether Adaptive Query Execution (spark.sql.adaptive.enabled) is enabled by default."
    )
    default_shuffle_db: str = Field(
        description="Default database backend for External Shuffle Service (LEVELDB or ROCKSDB)."
    )
    supported_java_versions: List[int] = Field(
        description="Officially supported Java LTS versions."
    )
    supported_scala_versions: List[str] = Field(
        description="Supported Scala versions."
    )
    structured_logging: bool = Field(
        description="Whether structured JSON logging framework is natively supported."
    )
    spark_connect_supported: bool = Field(
        description="Whether Spark Connect client-server architecture is supported."
    )
    variant_data_type_supported: bool = Field(
        description="Whether the VARIANT data type is natively supported in Spark SQL."
    )
    collation_supported: bool = Field(
        description="Whether string collation is supported in Spark SQL."
    )


class SparkVersionInfo(BaseModel):
    """Full version and environment capability metadata for a Spark application or server."""
    raw_version: str = Field(default="unknown", description="Original version string (e.g. '3.5.1', '4.0.0').")
    major: Optional[int] = Field(default=None, description="Major version integer (e.g. 3 or 4).")
    minor: Optional[int] = Field(default=None, description="Minor version integer.")
    patch: Optional[int] = Field(default=None, description="Patch version integer.")
    major_version: SparkMajorVersion = Field(
        default=SparkMajorVersion.UNKNOWN,
        description="Categorized major version enum (3.x, 4.x, or unknown)."
    )
    java_version: Optional[str] = Field(default=None, description="Detected Java runtime version.")
    scala_version: Optional[str] = Field(default=None, description="Detected Scala runtime version.")
    ansi_mode_active: Optional[bool] = Field(default=None, description="Effective ANSI SQL mode if known.")
    aqe_active: Optional[bool] = Field(default=None, description="Effective Adaptive Query Execution mode if known.")
    features: SparkFeatures = Field(description="Feature capability matrix for this Spark version.")


def parse_spark_version(version_str: Optional[str]) -> Tuple[Optional[int], Optional[int], Optional[int], SparkMajorVersion]:
    """Parse a Spark version string into (major, minor, patch, major_version_enum).
    
    Handles strings like '3.5.1', '4.0.0-preview2', '4.0.0', '3.4.0-SNAPSHOT', etc.
    """
    if not version_str or not isinstance(version_str, str):
        return None, None, None, SparkMajorVersion.UNKNOWN

    cleaned = version_str.strip().lstrip("v")
    match = re.match(r"^(\d+)(?:\.(\d+))?(?:\.(\d+))?", cleaned)
    if not match:
        return None, None, None, SparkMajorVersion.UNKNOWN

    major = int(match.group(1)) if match.group(1) is not None else None
    minor = int(match.group(2)) if match.group(2) is not None else None
    patch = int(match.group(3)) if match.group(3) is not None else None

    if major == 3:
        major_enum = SparkMajorVersion.SPARK_3_X
    elif major == 4:
        major_enum = SparkMajorVersion.SPARK_4_X
    else:
        major_enum = SparkMajorVersion.UNKNOWN

    return major, minor, patch, major_enum


def get_features_for_version(
    major_enum: SparkMajorVersion, 
    minor: Optional[int] = None
) -> SparkFeatures:
    """Get the feature set for a given Spark major/minor version."""
    if major_enum == SparkMajorVersion.SPARK_4_X:
        return SparkFeatures(
            ansi_sql_default=True,
            aqe_default=True,
            default_shuffle_db="ROCKSDB",
            supported_java_versions=[17, 21],
            supported_scala_versions=["2.13"],
            structured_logging=True,
            spark_connect_supported=True,
            variant_data_type_supported=True,
            collation_supported=True,
        )
    elif major_enum == SparkMajorVersion.SPARK_3_X:
        # AQE default was false in 3.0, 3.1, and enabled by default in 3.2+
        aqe_default = True if (minor is not None and minor >= 2) else False
        # Spark Connect was introduced in 3.4
        spark_connect = True if (minor is not None and minor >= 4) else False
        
        return SparkFeatures(
            ansi_sql_default=False,
            aqe_default=aqe_default,
            default_shuffle_db="LEVELDB",
            supported_java_versions=[8, 11, 17],
            supported_scala_versions=["2.12", "2.13"],
            structured_logging=False,
            spark_connect_supported=spark_connect,
            variant_data_type_supported=False,
            collation_supported=False,
        )
    else:
        # Fallback generic capabilities
        return SparkFeatures(
            ansi_sql_default=False,
            aqe_default=True,
            default_shuffle_db="ROCKSDB",
            supported_java_versions=[8, 11, 17, 21],
            supported_scala_versions=["2.12", "2.13"],
            structured_logging=False,
            spark_connect_supported=True,
            variant_data_type_supported=False,
            collation_supported=False,
        )


def _extract_prop(prop_list_or_dict: Any, key_name: str) -> Optional[str]:
    """Helper to retrieve a property from environment property maps or [ [k, v], ... ] pairs."""
    if isinstance(prop_list_or_dict, dict):
        return prop_list_or_dict.get(key_name)
    elif isinstance(prop_list_or_dict, list):
        for item in prop_list_or_dict:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                if item[0] == key_name:
                    return str(item[1])
            elif isinstance(item, dict):
                if item.get("key") == key_name or item.get("name") == key_name:
                    return str(item.get("value"))
    return None


def extract_spark_version_from_env(
    env_data: Dict[str, Any], 
    default_fallback: str = "auto"
) -> SparkVersionInfo:
    """Extract Spark version, runtime Java/Scala versions, and active configurations from `/environment` response."""
    spark_props = env_data.get("sparkProperties", [])
    system_props = env_data.get("systemProperties", [])
    runtime = env_data.get("runtime", {})

    # Detect raw spark version
    raw_version = (
        _extract_prop(spark_props, "spark.app.version")
        or _extract_prop(spark_props, "spark.version")
        or _extract_prop(system_props, "spark.version")
        or runtime.get("sparkVersion")
    )

    # If raw_version not found in environment, check fallback
    if not raw_version or raw_version == "unknown":
        if default_fallback in ["3.x", "3"]:
            raw_version = "3.5.0"
        elif default_fallback in ["4.x", "4"]:
            raw_version = "4.0.0"
        else:
            raw_version = "unknown"

    major, minor, patch, major_enum = parse_spark_version(raw_version)
    if major_enum == SparkMajorVersion.UNKNOWN and default_fallback != "auto":
        _, _, _, major_enum = parse_spark_version(default_fallback)

    features = get_features_for_version(major_enum, minor)

    # Detect Java runtime version
    java_version = (
        runtime.get("javaVersion")
        or _extract_prop(system_props, "java.version")
        or _extract_prop(system_props, "java.runtime.version")
    )

    # Detect Scala version
    scala_version = (
        runtime.get("scalaVersion")
        or _extract_prop(system_props, "scala.version")
    )

    # Detect ANSI SQL setting
    ansi_val = _extract_prop(spark_props, "spark.sql.ansi.enabled")
    if ansi_val is not None:
        ansi_mode = ansi_val.lower() == "true"
    else:
        ansi_mode = features.ansi_sql_default

    # Detect AQE setting
    aqe_val = _extract_prop(spark_props, "spark.sql.adaptive.enabled")
    if aqe_val is not None:
        aqe_mode = aqe_val.lower() == "true"
    else:
        aqe_mode = features.aqe_default

    return SparkVersionInfo(
        raw_version=raw_version or "unknown",
        major=major,
        minor=minor,
        patch=patch,
        major_version=major_enum,
        java_version=java_version,
        scala_version=scala_version,
        ansi_mode_active=ansi_mode,
        aqe_active=aqe_mode,
        features=features
    )


def extract_spark_version_from_version_endpoint(
    version_data: Dict[str, Any], 
    default_fallback: str = "auto"
) -> SparkVersionInfo:
    """Extract Spark version from `/version` endpoint (e.g. `{"spark": "3.5.1"}`)."""
    raw_version = version_data.get("spark") or version_data.get("version")
    if not raw_version:
        if default_fallback in ["3.x", "3"]:
            raw_version = "3.5.0"
        elif default_fallback in ["4.x", "4"]:
            raw_version = "4.0.0"
        else:
            raw_version = "unknown"

    major, minor, patch, major_enum = parse_spark_version(raw_version)
    if major_enum == SparkMajorVersion.UNKNOWN and default_fallback != "auto":
        _, _, _, major_enum = parse_spark_version(default_fallback)

    features = get_features_for_version(major_enum, minor)

    return SparkVersionInfo(
        raw_version=raw_version or "unknown",
        major=major,
        minor=minor,
        patch=patch,
        major_version=major_enum,
        features=features
    )
