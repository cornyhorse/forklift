# Lead notes (round 4)

## Done by the lead
- docs/design/platform.md §6.1: stale "a publish ... is not resumed" fixed (commit 883b8f2, pushed).
- Engine fix 1 (MCPL found): validate_schema hitting its bad-rows threshold listed bad_rows.parquet
  inside the job's private .forklift-job-* dir, removed before run() returns -> the worker refused
  the whole result ("listed the artifact ... but did not write it"): a schema check against an
  upload failed as INTERNAL and lost its report. Fix: runner._keep_sample_bad_rows moves the
  file next to the report (out/bad_rows.parquet) and points the message there.
  Test: TestValidateSchema::test_a_threshold_failure_keeps_the_samples_bad_rows_next_to_the_report.
- Engine fix 2 (MCPL found): a schema whose properties / a property / required has the wrong
  shape failed as INTERNAL (AttributeError) or, for "required": "id", as COLUMN_MISSING naming
  'i', 'd'. Fix: schema_processor.check_schema_shape in load_schema -> SCHEMA_INVALID with the
  key named. Tests: TestSchemaShape. (The gateway's review_document already flags these.)

## To do at integration
- Apply MCPL's patch to tests/unit-tests/test_wp6_review_fixes.py (test_extras pins the extras).
- test_only_expected_jobs_have_their_own_permissions (engine tests) fails with new workflow
  files (helm.yml ...): update its expectation for every new workflow, or fold jobs into test.yml.
- CHANGELOG Fixed: the two engine fixes above.
- HELM done ($S/round4/HELM.md). Items: (a) EDGE told to use `dstdomain -n` (bare-IP PTR bypass);
  (b) OIDC env names: chart took them from OIDC's oidc/config.py, a render test fails on drift;
  (c) dispatch/publish/sweep_retention ignore SIGTERM as PID 1 (Compose waits out the timeout):
      add a SIGTERM handler in the looping commands (stop after the current pass / at once while
      sleeping) after OIDC finishes with services/web; chart works around with shareProcessNamespace;
  (d) install PyJWT[crypto] into $S/webvenv (+webvenv313) before the gateway gate;
  (e) HELM edited create_worker_token.py (+ new tests/test_create_worker_token.py) — check it
      merges with OIDC's view of services/web (OIDC's git status also shows that file modified).
