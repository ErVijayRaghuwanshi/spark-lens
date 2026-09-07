import json
from http.server import HTTPServer, BaseHTTPRequestHandler
import sys

class MockSparkHistoryHandler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, data: any):
        payload = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        path = self.path.split("?")[0].rstrip("/")
        
        if path == "/api/v1/version":
            return self._send_json(200, {"spark": "4.0.0"})

        elif path == "/api/v1/applications":
            return self._send_json(200, [
                {
                    "id": "app-spark4-ecommerce",
                    "name": "ECommerceRevenue-Spark4",
                    "attempts": [{"attemptId": "1", "duration": 340000, "completed": True, "sparkUser": "data-team"}]
                },
                {
                    "id": "app-spark3-legacy-pipeline",
                    "name": "LegacyDataPipeline-Spark3",
                    "attempts": [{"attemptId": "1", "duration": 520000, "completed": True, "sparkUser": "etl-runner"}]
                }
            ])

        elif path == "/api/v1/applications/app-spark4-ecommerce":
            return self._send_json(200, {
                "id": "app-spark4-ecommerce",
                "name": "ECommerceRevenue-Spark4",
                "attempts": [{"attemptId": "1", "duration": 340000, "completed": True}]
            })

        elif path == "/api/v1/applications/app-spark4-ecommerce/jobs":
            return self._send_json(200, [
                {"jobId": 1, "name": "ReadAndFilter", "status": "SUCCEEDED", "stageIds": [1]},
                {
                    "jobId": 2, 
                    "name": "CalculateRevenueRatio", 
                    "status": "FAILED", 
                    "stageIds": [2],
                    "failureReason": "Job 2 failed due to stage 2.0 failure: SparkArithmeticException: [CANNOT_DIVIDE_BY_ZERO] Division by zero."
                }
            ])

        elif path == "/api/v1/applications/app-spark4-ecommerce/stages":
            return self._send_json(200, [
                {
                    "stageId": 1,
                    "attemptId": 0,
                    "name": "Stage 1: Scan & Aggregate",
                    "status": "COMPLETE",
                    "numFailedTasks": 0,
                    "numCompleteTasks": 200,
                    "executorRunTime": 45000
                },
                {
                    "stageId": 2,
                    "attemptId": 0,
                    "name": "Stage 2: Compute Margins",
                    "status": "FAILED",
                    "failureReason": "org.apache.spark.SparkArithmeticException: [CANNOT_DIVIDE_BY_ZERO] Division by zero. To return NULL instead, use 'try_divide'.",
                    "numFailedTasks": 1,
                    "numCompleteTasks": 199,
                    "executorRunTime": 68000
                }
            ])

        elif path == "/api/v1/applications/app-spark4-ecommerce/stages/2/0":
            return self._send_json(200, {
                "stageId": 2,
                "attemptId": 0,
                "name": "Stage 2: Compute Margins",
                "status": "FAILED",
                "failureReason": "org.apache.spark.SparkArithmeticException: [CANNOT_DIVIDE_BY_ZERO] Division by zero. To return NULL instead, use 'try_divide'.",
                "numFailedTasks": 1,
                "numCompleteTasks": 199,
                "tasks": {
                    "task-199": {
                        "taskId": 199,
                        "host": "worker-node-4.prod",
                        "status": "FAILED",
                        "errorMessage": "SparkArithmeticException: [CANNOT_DIVIDE_BY_ZERO] Division by zero. SQLSTATE: 22012"
                    }
                }
            })

        elif path.startswith("/api/v1/applications/app-spark4-ecommerce/stages/1/0/taskSummary") or path.startswith("/api/v1/applications/app-spark4-ecommerce/stages/2/0/taskSummary"):
            return self._send_json(200, {
                "quantiles": [0.0, 0.25, 0.5, 0.75, 0.95, 1.0],
                "executorRunTime": {"quantiles": [150.0, 280.0, 450.0, 1200.0, 48000.0, 92000.0]},
                "jvmGcTime": {"quantiles": [10.0, 20.0, 40.0, 100.0, 1800.0, 7500.0]},
                "memoryBytesSpilled": {"quantiles": [0.0, 0.0, 0.0, 524288.0, 31457280.0, 157286400.0]},
                "diskBytesSpilled": {"quantiles": [0.0, 0.0, 0.0, 0.0, 10485760.0, 83886080.0]}
            })

        elif path == "/api/v1/applications/app-spark4-ecommerce/executors":
            return self._send_json(200, [
                {"id": "driver", "hostPort": "driver.cluster:4040", "isActive": True, "totalCores": 4, "maxMemory": 4294967296},
                {"id": "1", "hostPort": "worker-1:7337", "isActive": True, "totalCores": 8, "maxMemory": 8589934592, "totalGCTime": 3400},
                {"id": "2", "hostPort": "worker-2:7337", "isActive": True, "totalCores": 8, "maxMemory": 8589934592, "totalGCTime": 8200}
            ])

        elif path == "/api/v1/applications/app-spark4-ecommerce/environment":
            return self._send_json(200, {
                "appId": "app-spark4-ecommerce",
                "runtime": {
                    "sparkVersion": "4.0.0",
                    "javaVersion": "17.0.10 (Eclipse Adoptium)",
                    "scalaVersion": "2.13.13"
                },
                "sparkProperties": [
                    ["spark.app.name", "ECommerceRevenue-Spark4"],
                    ["spark.app.id", "app-spark4-ecommerce"],
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
            })

        elif path == "/api/v1/applications/app-spark4-ecommerce/sql":
            return self._send_json(200, [
                {
                    "id": 1,
                    "status": "COMPLETED",
                    "description": "SELECT region, SUM(sales) / NULLIF(COUNT(orders), 0) FROM transactions GROUP BY region",
                    "duration": 45000,
                    "runningJobIds": [],
                    "successJobIds": [1],
                    "failedJobIds": []
                }
            ])

        elif path == "/api/v1/applications/app-spark4-ecommerce/sql/1":
            return self._send_json(200, {
                "id": 1,
                "status": "COMPLETED",
                "description": "SELECT region, SUM(sales) / NULLIF(COUNT(orders), 0) FROM transactions GROUP BY region",
                "duration": 45000,
                "planDescription": "== Physical Plan ==\nAdaptiveSparkPlan(isFinalPlan=true)\n+- HashAggregate(keys=[region], functions=[sum(sales), count(orders)])\n   +- Exchange hashpartitioning(region, 200)\n      +- FileScan parquet [region,sales,orders] (Batched: true, DataFilters: [], Format: Parquet)"
            })

        # Spark 3.x Legacy Pipeline Endpoints
        elif path == "/api/v1/applications/app-spark3-legacy-pipeline/environment":
            return self._send_json(200, {
                "appId": "app-spark3-legacy-pipeline",
                "runtime": {
                    "sparkVersion": "3.5.1",
                    "javaVersion": "1.8.0_382 (Azul Systems, Inc.)",
                    "scalaVersion": "2.12.18"
                },
                "sparkProperties": [
                    ["spark.app.name", "LegacyDataPipeline-Spark3"],
                    ["spark.app.id", "app-spark3-legacy-pipeline"],
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
            })

        else:
            return self._send_json(404, {"error": f"Endpoint {path} not found"})

def run(port: int = 18080):
    server_address = ("127.0.0.1", port)
    httpd = HTTPServer(server_address, MockSparkHistoryHandler)
    print(f"Mock Spark History Server running on http://127.0.0.1:{port}")
    httpd.serve_forever()

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 18080
    run(port)
