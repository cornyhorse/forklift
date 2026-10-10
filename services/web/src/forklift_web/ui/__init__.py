"""The HTML user interface of forklift-web: server-rendered Django templates with HTMX.

Every view calls the service layer (``forklift_web.services``) with the signed-in user's actor;
the service layer checks the permission policy and writes the audit log, so a view never
decides on its own what someone may do. Views use :func:`forklift_web.policy.allowed` only to
hide controls a role cannot use, and show ``ServiceError.message`` when a call is refused.

==================  ===================================================================
``views.account``   home page, own password, own API tokens
``views.catalog``   schemas (versions, diffs, the JSON editor, live validation), datasets
``views.work``      uploads (the browser PUTs to presigned URLs), jobs, artifact downloads
``views.admin``     /admin/: overview, users, tokens, workers, connections, retention,
                    audit log, installation settings, all jobs
==================  ===================================================================

The gateway never reads object contents: previews, validation reports and generated schemas
are fetched by the browser from the store (presigned GETs from ``/api/v1/artifacts/{id}/
download``, which checks the raw-rows rules and audits the download) and rendered by
``static/ui/forklift.js``. HTMX (vendored in ``static/ui/vendor``) polls job status and
swaps partial templates; pages work without JavaScript except for uploads and those viewers.
"""
