import json

from conftest import bad_rows_table, parquet_bytes
from fake_gateway import Plan


def test_smoke(fake, mcp):
    schema = fake.add_schema("orders", {"properties": {"id": {"type": "integer"}}})
    fake.add_dataset("orders", schema)
    print(mcp.ok("list_datasets"))
    upload = fake.add_upload()
    bad = parquet_bytes(bad_rows_table(30), row_group_size=10)
    fake.planner = lambda body: Plan(
        statuses=["running", "failed"],
        error={"code": "BAD_ROWS_THRESHOLD_EXCEEDED", "message": "too many"},
        result={
            "counts": {"total_rows": 40, "invalid_rows": 30},
            "validation_summary": {"VALIDATION_ERROR:age": 20},
            "warnings": [],
            "error": {
                "code": "BAD_ROWS_THRESHOLD_EXCEEDED",
                "message": "too many",
                "retryable": False,
            },
        },
        artifacts=[("bad_rows", "bad_rows.parquet", bad, 30)],
    )
    run = mcp.ok(
        "run_job",
        {"upload_id": upload["id"], "schema": {"properties": {"id": {"type": "integer"}}}},
    )
    print(json.dumps(run, indent=1)[:3000])
    print(json.dumps(mcp.ok("explain_failure", {"job_id": run["job"]["id"]}), indent=1)[:3000])
    print(
        json.dumps(
            mcp.ok("get_bad_rows", {"job_id": run["job"]["id"], "offset": 15, "limit": 3}),
            indent=1,
        )[:3000]
    )
    print(mcp.refused("get_job", {"job_id": "nope"}))
