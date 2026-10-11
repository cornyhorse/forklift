# Round 4 (integrations) — state, 2026-10-11 11:00 UTC

TEMPORARY: this directory (`.wip-round4/`) holds the agents' reports and the independent reviews
so the work survives a reclaimed container. Remove it and clean the history before the PR.

All background agents stopped at once on the account's weekly usage limit ("resets 6am UTC").

## Committed on this branch (reviewed or under review)
- 156d6f8 engine fixes (validate_schema keeps the sample's bad rows; check_schema_shape)
- e77ca04 forklift-mcp (local stdio, M2) + Helm chart (M4)
- 4f3cb7c Compose caddy + egress-proxy profiles; worker FORKLIFT_STORE_PROXY
- ca466a4 forklift-client + Airflow
- 8e23f3b OIDC sign-in
- 6416b1d SIGTERM handling of dispatch/publish/sweep_retention; CI (test.yml platform-e2e
  with client + Dex, core-install forklift-mcp check, fast-test clients/, publish.yaml v* only,
  publish-client.yml)

## In this WIP commit (unfinished, may not pass tests)
- MCPL's review fixes, partial: src/forklift/mcp/{server,tools,values}.py, new previous.py,
  staging.py, tests test_mcp_previous.py, test_mcp_review_fixes.py, test_mcp_staging.py.
  Its last step was "the previous-run module" (finding 3). Status per finding unknown: re-check
  every finding of REVIEW-MCPL.md against the code.
- MCPR (remote MCP proxy), partial: services/mcp/** (package, tools.py, parquet.py, fake
  gateway tests). Its last step was "the image: a pinned constraints file, then the
  Dockerfile". Brief: MCPR_PROMPT.md (+ BRIEF.md contract). Missing at least: Dockerfile,
  README, e2e test deploy/compose/e2e/test_mcp.py, .github/workflows/mcp.yml, docs section in
  docs/platform/mcp.md, the report.

## Open review findings (each report has file:line, reproduction, suggested fix)
- REVIEW-MCPL.md: 14 should-fix + nits (MCPL started fixing, see above); 6 engine observations
  E1-E6 (ENGFIX agent was given them; it changed NO files before stopping).
- REVIEW-HELM.md: B1 virtual-hosted store hosts; S1-S14; N1-N14. Decisions already given to
  the HELM agent (it changed nothing before stopping): S10 document only (no internal TLS);
  S7 no silent Landlock downgrade under gVisor; S13 per-job memory below the container limit;
  remove shareProcessNamespace (commands now handle SIGTERM); pin Squid by stable tag + digest.
- REVIEW-CLIENT.md: B1 download to "out/" overwrites; S1-S8 (Airflow idempotency on real
  kill/clear paths, ca_file adds to system CAs, multipart part-URL refresh, trigger executor,
  sensor outage, validate_schema timeout, TLS error classes); nits. Decisions were sent to the
  CLIENT agent (it changed nothing before stopping): see LEAD message in this file's history:
  on_kill cancels only when stopped for good (else opt-in); key changes on clear and per DAG
  run incarnation; ca_file adds.
- REVIEW-EDGE.md: B1 IP-literal allow entries in Squid bypass; S1 Caddy logs presigned URLs;
  S2 Squid adds Cache-Control to PUTs; S3 https proxy refusal treated as transient; S4/S6 docs;
  S5 redirect drops the https port; S7 pin Squid by digest; N1-N7.
- REVIEW-OIDC.md: F1 BLOCKING email-link Unicode case-folding takeover (when
  FORKLIFT_OIDC_LINK_VERIFIED_EMAIL is on); F2 group lists split on spaces; F3 password
  sign-in after SSO start gets a 10-min session; F4 anonymous audit/DB writes; F5 no negative
  discovery cache (thread exhaustion); F6 role mapping ignores email_verified; F7 docs (API
  tokens keep roles); N1-N8.
- Lead's own: gateway upload completion is not idempotent after a store-side completion that
  was not recorded (services/web uploads.complete_upload: on complete_multipart failure, accept
  an object that exists with the declared size, as queue.py does for outputs) — not started.

## Still to do after the fixes
- The `mcp` Compose profile service (from MCPR's report) and platform-e2e with it.
- Shared docs from the reports' "exact text" sections: CHANGELOG (Added/Security/Changed/Fixed,
  incl. the two engine fixes and the SIGTERM fix), docs/design/platform.md (§1, §3.1, §5.1,
  §5.4, §5.7, §6.1, §6.2, §7.3 "over TLS" correction, §8 + residual risks, §9.1, §9.2,
  §10.1, §10.2, §11, §13), docs/DOCUMENTATION_INDEX.md, root README.md, docs/guides/USAGE.md.
- Full verification: engine suite (100%), gateway (100%, 3.12 + 3.13), worker (100%, 3.12-3.14
  + integration), client (3.9-3.14 + Airflow 3.2/3.3), mcp server, Helm render/kubeconform/k3s,
  Compose e2e (default + caddy + egress-proxy + Dex + mcp).
- Remove .wip-round4/, squash the WIP history into clean commits, push (force-with-lease),
  open ONE PR with the template-less body as in earlier rounds.
