# SparkLens Development Roadmap

This document outlines the development plan and milestone achievements for SparkLens.

---

## Roadmap Overview

```
┌────────────────────────┐      ┌────────────────────────┐      ┌────────────────────────┐
│   Phase 1: Foundation   │ ───> │   Phase 2: Diagnostics  │ ───> │   Phase 3: Multi-Version│
│  - Raw API mapping     │      │  - Skew detection      │      │  - Spark 3.x & 4.x     │
│  - FastMCP integration  │      │  - Failure reasoning   │      │  - ANSI Error Classes  │
└────────────────────────┘      └────────────────────────┘      │  - Migration Auditor   │
                                                                └────────────────────────┘
                                                                            │
                                                                            ▼
                                                                ┌────────────────────────┐
                                                                │    Phase 4: Advanced   │
                                                                │  - Live monitoring     │
                                                                │  - SQL DAG analysis    │
                                                                └────────────────────────┘
```

---

## Detailed Milestones

### Phase 1: Core Foundation & API Mapping (Complete)
- [x] Basic FastMCP server wrapper in Python.
- [x] Connection handler with `SPARK_HISTORY_URL` and authentication support.
- [x] Discovery & Deep-Dive tools (`list_applications`, `get_application`, `get_jobs`, `get_stages`, `get_executors`, `get_environment`, `list_sql_queries`, `get_sql_query_details`).

### Phase 2: Diagnostics & Troubleshooting (Complete)
- [x] **Data Skew Detector (`find_data_skew`)**: Analyzes quantiles for task duration and memory/disk spills.
- [x] **Application Health Analyzer (`analyze_application`)**: High-level failure correlation and duration metrics.
- [x] **Stage Failure Investigator (`find_failed_stages`, `explain_stage_failure`)**: Failure reason extraction and task failure samples.

### Phase 3: Multi-Version Spark 3.x & Spark 4.x Support (Complete)
- [x] **Dynamic Version Resolution (`get_spark_version`)**: Extracts version and runtime Java/Scala metadata per application or server.
- [x] **Spark 4.0 Standardized Error Classes**: Parses and categorizes structured error classes (`CANNOT_DIVIDE_BY_ZERO`, `CAST_INVALID_INPUT`, `ARITHMETIC_OVERFLOW`, `UNRESOLVED_COLUMN`, etc.) and provides ANSI SQL remediation.
- [x] **Spark 3.x to 4.x Migration Auditor (`check_spark_compatibility`)**: Automatically validates configuration against breaking changes (LevelDB removal, Java 8/11 deprecation, Scala 2.12 removal, CMS GC flags).
- [x] **Multi-Version Prompts**: `audit_spark4_migration`, `diagnose_application`, `optimize_application`, `explain_sql_query`.
- [x] **Automated Test Suite**: 29 unit and integration tests with `pytest` and `respx`.

### Phase 4: Advanced Live Diagnostics (Next)
- [ ] **Live Spark UI Integration**: Direct connection to active driver UI (`:4040`) for live application streaming.
- [ ] **Direct Log Parser**: Integration with YARN/Kubernetes log aggregators to pull executor stdout/stderr.
