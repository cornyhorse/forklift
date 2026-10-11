"""Invariants of the rendered chart, for every values file in ci/ and for values that must fail.

Every pod is Pod Security "restricted"; every pod of the chart is selected by the default-deny
NetworkPolicy and by one policy of its own; workers have no ingress; the gateway's internal port
admits the workers only; nothing secret is in a ConfigMap, an annotation or a literal env value;
the hooks run in the right order with what they need; each store-egress mode allows what it says
and nothing more; and the settings are the ones the gateway and the worker read.
"""

from __future__ import annotations

import base64
import ipaddress
import json
import re
from pathlib import Path

import jsonschema
import pytest
import yaml
from conftest import CHART, NAMESPACE, RELEASE, REPOSITORY, RenderError, render

FULLNAME = f"{RELEASE}-forklift"
INSTANCE = {"app.kubernetes.io/name": "forklift", "app.kubernetes.io/instance": RELEASE}
SECRET_NAME = re.compile(
    r"(PASSWORD|SECRET_KEY|SECRETS_KEYS|ACCESS_KEY_ID|SECRET_ACCESS_KEY|CLIENT_SECRET|TOKEN)$"
)
PRIVATE = [
    ipaddress.ip_network(n)
    for n in (
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "100.64.0.0/10",
        "169.254.0.0/16",
        "127.0.0.0/8",
    )
]
SCHEMA = json.loads((CHART / "values.schema.json").read_text())


def containers(spec):
    return spec.get("initContainers", []) + spec["containers"]


def selects(policy, labels) -> bool:
    selector = policy["spec"]["podSelector"]
    assert set(selector) <= {"matchLabels"}, "the chart's policies select by labels only"
    return all(labels.get(k) == v for k, v in selector.get("matchLabels", {}).items())


def policies_for(rendered, labels):
    return [p for p in rendered.kind("NetworkPolicy") if selects(p, labels)]


def component_of(labels):
    return labels.get("app.kubernetes.io/component")


def env_of(container):
    return {item["name"]: item for item in container.get("env", [])}


def peer_component(peer):
    return peer.get("podSelector", {}).get("matchLabels", {}).get("app.kubernetes.io/component")


# --------------------------------------------------------------------------- Pod Security


def test_every_pod_meets_pod_security_restricted(ci):
    for workload, labels, spec in ci.pod_specs():
        name = f"{workload['kind']} {workload['metadata']['name']}"
        pod = spec["securityContext"]
        assert pod["runAsNonRoot"] is True and pod["runAsUser"] > 0, name
        assert pod["seccompProfile"]["type"] in ("RuntimeDefault", "Localhost"), name
        assert spec["automountServiceAccountToken"] is False, name
        assert spec["enableServiceLinks"] is False, name  # no FORKLIFT_*_PORT=tcp://... variables
        for key in ("hostNetwork", "hostPID", "hostIPC"):
            assert not spec.get(key), name
        assert not [v for v in spec.get("volumes", []) if "hostPath" in v], name
        for container in containers(spec):
            context = container["securityContext"]
            assert context["allowPrivilegeEscalation"] is False, name
            assert context["readOnlyRootFilesystem"] is True, name
            assert context["capabilities"] == {"drop": ["ALL"]}, name
            assert not context.get("privileged"), name
            assert not [p for p in container.get("ports", []) if "hostPort" in p], name
            resources = container["resources"]
            for kind in ("requests", "limits"):
                assert {"cpu", "memory"} <= set(resources[kind]), f"{name}: {kind}"


def test_images_run_as_their_own_users_and_are_pinned(ci):
    for workload, labels, spec in ci.pod_specs():
        uid = spec["securityContext"]["runAsUser"]
        assert uid == (13 if component_of(labels) == "egress-proxy" else 10001), labels
        for container in containers(spec):
            image = container["image"]
            assert not image.endswith(":latest") and (":" in image or "@" in image), image


def test_writable_paths_are_empty_dirs_and_scratch_is_bounded(ci):
    for workload, labels, spec in ci.pod_specs():
        volumes = {v["name"]: v for v in spec.get("volumes", [])}
        assert volumes["tmp"]["emptyDir"]["sizeLimit"], labels
        if component_of(labels) == "worker":
            assert volumes["scratch"]["emptyDir"]["sizeLimit"], labels
            token = volumes["token"]["secret"]
            assert token["items"][0]["path"] == "worker-token" and token["defaultMode"] == 0o440
            (mount,) = [m for m in spec["containers"][0]["volumeMounts"] if m["name"] == "token"]
            assert mount == {"name": "token", "mountPath": "/run/secrets", "readOnly": True}


def test_disruption_budgets_where_more_than_one_replica(ci):
    budgets = {pdb["metadata"]["name"] for pdb in ci.kind("PodDisruptionBudget")}
    scaled = {o["spec"]["scaleTargetRef"]["name"]: o for o in ci.kind("ScaledObject")}
    for workload in ci.kind("Deployment"):
        name = workload["metadata"]["name"]
        replicas = workload["spec"].get("replicas")
        if replicas is None:
            replicas = scaled[name]["spec"]["minReplicaCount"]
        assert (name in budgets) == (replicas > 1), name


# --------------------------------------------------------------------------- NetworkPolicies


def test_every_chart_pod_is_covered_by_default_deny_and_a_policy_of_its_own(ci):
    deny = ci.get("NetworkPolicy", f"{FULLNAME}-default-deny")
    assert deny["spec"]["podSelector"] == {"matchLabels": INSTANCE}
    assert deny["spec"]["policyTypes"] == ["Ingress", "Egress"]
    assert "ingress" not in deny["spec"] and "egress" not in deny["spec"]
    for workload, labels, spec in ci.pod_specs():
        assert selects(deny, labels), workload["metadata"]["name"]
        own = [p for p in policies_for(ci, labels) if p is not deny]
        assert len(own) == 1, (workload["metadata"]["name"], [p["metadata"]["name"] for p in own])
        assert own[0]["spec"]["policyTypes"] == ["Ingress", "Egress"]


def test_no_rule_admits_everyone(ci):
    """A rule without peers (from/to empty or missing) would admit every address on its ports."""
    for policy in ci.kind("NetworkPolicy"):
        for direction, key in (("ingress", "from"), ("egress", "to")):
            for rule in policy["spec"].get(direction) or []:
                assert rule.get(
                    key
                ), f"{policy['metadata']['name']}: {direction} rule without {key}"
                assert rule.get(
                    "ports"
                ), f"{policy['metadata']['name']}: {direction} rule without ports"
                for peer in rule[key]:
                    assert peer, policy["metadata"]["name"]
                    block = peer.get("ipBlock")
                    if block and block["cidr"] in ("0.0.0.0/0", "::/0"):
                        assert block.get("except"), "the internet, never every address"


def test_workers_have_no_ingress_and_reach_only_dns_the_internal_port_and_the_store(ci):
    for workload, labels, spec in ci.pod_specs():
        if component_of(labels) != "worker":
            continue
        for policy in policies_for(ci, labels):
            assert not policy["spec"].get("ingress"), policy["metadata"]["name"]
        (own,) = [
            p
            for p in policies_for(ci, labels)
            if p["metadata"]["name"] != f"{FULLNAME}-default-deny"
        ]
        rules = own["spec"]["egress"]
        assert rules[0]["ports"] == [
            {"protocol": "UDP", "port": 53},
            {"protocol": "TCP", "port": 53},
        ]
        assert [peer_component(p) for p in rules[1]["to"]] == ["gateway"]
        assert rules[1]["ports"] == [{"protocol": "TCP", "port": 8081}]
        for rule in rules[2:]:
            assert rule.get("to") and rule.get("ports"), "no rule may allow everything"
            for peer in rule["to"]:
                assert peer_component(peer) != "gateway", "only the internal port of the gateway"


def test_the_internal_port_admits_the_workers_only(ci):
    gateway_labels = ci.get("Deployment", f"{FULLNAME}-gateway")["spec"]["template"]["metadata"][
        "labels"
    ]
    for policy in policies_for(ci, gateway_labels):
        for rule in policy["spec"].get("ingress", []):
            ports = [p["port"] for p in rule.get("ports", [])]
            assert ports, f"{policy['metadata']['name']}: an ingress rule without ports opens 8081"
            if 8081 in ports:
                assert ports == [8081]
                assert [peer_component(p) for p in rule["from"]] == ["worker"]
                assert rule["from"][0]["podSelector"]["matchLabels"] == {
                    **INSTANCE,
                    "app.kubernetes.io/component": "worker",
                }
                assert "namespaceSelector" not in rule["from"][0]
    internal = ci.get("Service", f"{FULLNAME}-gateway-internal")
    assert internal["spec"]["type"] == "ClusterIP"
    for ingress in ci.kind("Ingress"):
        for rule in ingress["spec"]["rules"]:
            for path in rule["http"]["paths"]:
                assert path["backend"]["service"]["name"] != f"{FULLNAME}-gateway-internal"


def test_the_public_port_admits_the_ingress_controller_and_the_mcp_proxy(ci_file):
    rendered = ci_file("mcp-oidc")
    policy = rendered.get("NetworkPolicy", f"{FULLNAME}-gateway")
    public = [r for r in policy["spec"]["ingress"] if [p["port"] for p in r["ports"]] == [8080]]
    (rule,) = public
    controller, mcp = rule["from"]
    assert controller["namespaceSelector"] == {
        "matchLabels": {"kubernetes.io/metadata.name": "ingress-nginx"}
    }
    assert peer_component(mcp) == "mcp"
    without_ingress = ci_file("no-network").get("NetworkPolicy", f"{FULLNAME}-gateway")
    assert [r["ports"] for r in without_ingress["spec"]["ingress"]] == [
        [{"protocol": "TCP", "port": 8081}]
    ]


def test_the_dispatcher_sends_webhooks_to_public_addresses_only(ci):
    policy = ci.get("NetworkPolicy", f"{FULLNAME}-dispatcher")
    webhooks = [
        r
        for r in policy["spec"]["egress"]
        if r["ports"][0]["port"] == 443
        and "ipBlock" in r["to"][0]
        and r["to"][0]["ipBlock"]["cidr"] == "0.0.0.0/0"
    ]
    assert webhooks, "the dispatcher needs a route out for webhooks"
    for rule in webhooks:
        excluded = [ipaddress.ip_network(c) for c in rule["to"][0]["ipBlock"]["except"]]
        for network in PRIVATE:
            assert any(network.subnet_of(e) for e in excluded), network
    assert "ingress" not in policy["spec"]


def test_the_mcp_proxy_reaches_only_the_gateways_public_port(ci_file):
    rendered = ci_file("mcp-oidc")
    policy = rendered.get("NetworkPolicy", f"{FULLNAME}-mcp")
    dns, gateway = policy["spec"]["egress"]
    assert [peer_component(p) for p in gateway["to"]] == ["gateway"]
    assert gateway["ports"] == [{"protocol": "TCP", "port": 8080}]
    (ingress,) = policy["spec"]["ingress"]
    assert ingress["ports"] == [{"protocol": "TCP", "port": 8000}]


def test_the_oidc_provider_is_reachable_from_the_gateway_only(ci_file):
    rendered = ci_file("mcp-oidc")
    keycloak = {
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "keycloak"}},
        "podSelector": {"matchLabels": {"app": "keycloak"}},
    }
    for policy in rendered.kind("NetworkPolicy"):
        found = [r for r in policy["spec"].get("egress", []) if keycloak in r.get("to", [])]
        if policy["metadata"]["name"] == f"{FULLNAME}-gateway":
            assert [r["ports"] for r in found] == [[{"protocol": "TCP", "port": 8443}]]
        else:
            assert not found, policy["metadata"]["name"]


def test_pod_selector_mode_reaches_the_bundled_rustfs(ci_file):
    rendered = ci_file("rustfs")
    worker = rendered.get("NetworkPolicy", f"{FULLNAME}-worker-batch")
    store = worker["spec"]["egress"][2]
    assert [peer_component(p) for p in store["to"]] == ["rustfs"]
    assert store["ports"] == [{"protocol": "TCP", "port": 9000}]
    rustfs = rendered.get("NetworkPolicy", f"{FULLNAME}-rustfs")
    assert rustfs["spec"]["egress"] == []
    first, *_ = rustfs["spec"]["ingress"]
    sources = [peer_component(p) for p in first["from"]]
    assert sources[:6] == [
        "gateway",
        "dispatcher",
        "publisher",
        "sweeper",
        "store-setup",
        "worker",
    ]
    assert {
        "podSelector": {"matchLabels": {"app.kubernetes.io/name": "ingress-nginx"}},
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "ingress-nginx"}},
    } in first["from"]


def test_cidr_mode_reaches_the_stores_ranges_on_its_port(ci_file):
    rendered = ci_file("cidr")
    for name in ("worker-interactive", "worker-batch", "worker-sql", "gateway", "publisher"):
        policy = rendered.get("NetworkPolicy", f"{FULLNAME}-{name}")
        store = [
            r
            for r in policy["spec"]["egress"]
            if {"ipBlock": {"cidr": "198.51.100.0/24"}} in r["to"]
        ]
        assert store == [
            {
                "to": [
                    {"ipBlock": {"cidr": "198.51.100.0/24"}},
                    {"ipBlock": {"cidr": "203.0.113.16/28"}},
                ],
                "ports": [{"protocol": "TCP", "port": 443}],
            }
        ], name
    sql = rendered.get("NetworkPolicy", f"{FULLNAME}-worker-sql")["spec"]["egress"]
    assert sql[-1] == {
        "to": [{"ipBlock": {"cidr": "10.30.0.0/16"}}],
        "ports": [{"protocol": "TCP", "port": 5432}, {"protocol": "TCP", "port": 1433}],
    }
    batch = rendered.get("NetworkPolicy", f"{FULLNAME}-worker-batch")["spec"]["egress"]
    assert len(batch) == 3  # the sql group's database rule is its own


def test_proxy_mode_sends_the_workers_through_squid_which_reaches_only_the_store(ci_file):
    rendered = ci_file("proxy")
    for name in ("interactive", "batch"):
        policy = rendered.get("NetworkPolicy", f"{FULLNAME}-worker-{name}")
        dns, gateway, proxy = policy["spec"]["egress"]
        assert [peer_component(p) for p in proxy["to"]] == ["egress-proxy"]
        assert proxy["ports"] == [{"protocol": "TCP", "port": 3128}]
        worker = rendered.get("Deployment", f"{FULLNAME}-worker-{name}")
        env = env_of(worker["spec"]["template"]["spec"]["containers"][0])
        assert env["FORKLIFT_STORE_PROXY"]["value"] == f"http://{FULLNAME}-egress-proxy:3128"
        assert env["FORKLIFT_WORKER_STORE_HOSTS"]["value"] == "storage.example.net"
    squid = rendered.get("NetworkPolicy", f"{FULLNAME}-egress-proxy")
    (ingress,) = squid["spec"]["ingress"]
    assert [peer_component(p) for p in ingress["from"]] == ["worker"] and ingress["ports"] == [
        {"protocol": "TCP", "port": 3128}
    ]
    dns, upstream = squid["spec"]["egress"]
    assert upstream["ports"] == [{"protocol": "TCP", "port": 443}]
    assert (
        upstream["to"][0]["ipBlock"]["cidr"] == "0.0.0.0/0"
        and "10.0.0.0/8" in upstream["to"][0]["ipBlock"]["except"]
    )
    config = rendered.get("ConfigMap", f"{FULLNAME}-egress-proxy")["data"]
    rules = [line for line in config["allow.conf"].splitlines() if not line.startswith("#")]
    assert rules == [
        "acl forklift_store_1 dstdomain -n storage.example.net",
        "acl forklift_store_1_port port 443",
        "http_access allow CONNECT forklift_store_1 forklift_store_1_port",
        "acl forklift_store_2 dstdomain -n forklift.storage.example.net",
        "acl forklift_store_2_port port 443",
        "http_access allow CONNECT forklift_store_2 forklift_store_2_port",
    ]
    squid = [
        line for line in config["squid.conf"].splitlines() if line and not line.startswith("#")
    ]
    access = [line for line in squid if line.startswith(("http_access", "include"))]
    assert access == ["include /etc/forklift-squid/allow.conf", "http_access deny all"]
    assert "strip_query_terms on" in squid  # presigned URLs' signatures never reach a log
    assert "cache deny all" in squid
    # The gateway processes hold the store's credentials and call it directly
    gateway = rendered.get("NetworkPolicy", f"{FULLNAME}-gateway")["spec"]["egress"]
    assert gateway[2]["ports"] == [{"protocol": "TCP", "port": 443}]
    assert gateway[2]["to"][0]["ipBlock"]["cidr"] == "0.0.0.0/0"


def directives(text):
    lines = (line.strip() for line in text.splitlines())
    return [line for line in lines if line and not line.startswith("#")]


def test_the_egress_proxy_matches_the_compose_profile():
    """Same directives as deploy/compose/egress-proxy/squid.conf, but the include path (the chart
    renders the allow file itself) and comments."""
    compose = (REPOSITORY / "deploy/compose/egress-proxy/squid.conf").read_text()
    chart = render(CHART / "ci/proxy-values.yaml").get("ConfigMap", f"{FULLNAME}-egress-proxy")
    expected = [
        line.replace("/run/forklift-egress/allow.conf", "/etc/forklift-squid/allow.conf")
        for line in directives(compose)
    ]
    assert directives(chart["data"]["squid.conf"]) == expected


def test_proxy_allow_list_from_urls(ci_file):
    rendered = render(
        None,
        extra=with_(
            networkPolicies__storeEgress={"mode": "proxy"},
            egressProxy={"allow": ["https://s3.example.org:8443", "http://10.1.2.3:9000"]},
        ),
    )
    allow = rendered.get("ConfigMap", f"{FULLNAME}-egress-proxy")["data"]["allow.conf"]
    assert [line for line in allow.splitlines() if not line.startswith("#")] == [
        "acl forklift_store_1 dstdomain -n s3.example.org",
        "acl forklift_store_1_port port 8443",
        "http_access allow CONNECT forklift_store_1 forklift_store_1_port",
        "acl forklift_store_2 dst 10.1.2.3",
        "acl forklift_store_2_port port 9000",
        "http_access allow !CONNECT plain_http forklift_store_2 forklift_store_2_port",
    ]
    upstream = rendered.get("NetworkPolicy", f"{FULLNAME}-egress-proxy")["spec"]["egress"][1]
    assert upstream["ports"] == [
        {"protocol": "TCP", "port": 8443},
        {"protocol": "TCP", "port": 9000},
    ]


def test_without_proxy_mode_there_is_no_egress_proxy(ci):
    if any(
        d["kind"] == "NetworkPolicy" and d["metadata"]["name"].endswith("egress-proxy")
        for d in ci.documents
    ):
        return
    assert not [d for d in ci.documents if "egress-proxy" in d["metadata"]["name"]]
    for workload, labels, spec in ci.pod_specs():
        for container in containers(spec):
            assert "FORKLIFT_STORE_PROXY" not in env_of(container)


# --------------------------------------------------------------------------- secrets


def test_nothing_secret_in_config_maps_annotations_or_literal_env(ci):
    secrets = [v for v in ci.secret_values() if len(v) >= 8]  # what the chart generated
    for document in ci.documents:
        annotations = json.dumps(document["metadata"].get("annotations", {}))
        assert not [s for s in secrets if s in annotations], document["metadata"]["name"]
        if document["kind"] == "ConfigMap":
            for key, value in document["data"].items():
                assert not SECRET_NAME.search(key), key
                assert not [s for s in secrets if s in value], key
    for workload, labels, spec in ci.pod_specs():
        for container in containers(spec):
            for item in container.get("env", []):
                if (
                    SECRET_NAME.search(item["name"])
                    and item["name"] != "FORKLIFT_WORKER_TOKEN_FILE"
                ):
                    assert "secretKeyRef" in item.get("valueFrom", {}), item["name"]
                assert not [s for s in secrets if s in str(item.get("value", ""))], item["name"]


def test_generated_secrets_are_hooks_kept_across_upgrades(ci_file):
    rendered = ci_file("rustfs")
    names = {s["metadata"]["name"]: s for s in rendered.kind("Secret")}
    assert set(names) == {
        f"{FULLNAME}-secrets",
        f"{FULLNAME}-admin",
        f"{FULLNAME}-worker-token",
        f"{FULLNAME}-rustfs",
    }
    for secret in names.values():
        annotations = secret["metadata"]["annotations"]
        assert annotations["helm.sh/hook"] == "pre-install,pre-upgrade"
        assert annotations["helm.sh/hook-delete-policy"] == "before-hook-creation"

    def value(name, key):
        return base64.b64decode(names[f"{FULLNAME}-{name}"]["data"][key]).decode()

    # The formats the gateway expects: create_worker_token's own, a Fernet key (32 random bytes,
    # URL-safe base64), a long signing key
    assert re.fullmatch(r"fkw_[A-Za-z0-9_-]{43}", value("worker-token", "token"))
    assert len(base64.urlsafe_b64decode(value("secrets", "secrets-keys"))) == 32
    assert re.fullmatch(r"[A-Za-z0-9]{64}", value("secrets", "secret-key"))
    assert re.fullmatch(r"[A-Za-z0-9]{24}", value("admin", "password"))
    assert {value("rustfs", "access-key-id"), value("rustfs", "secret-access-key")}
    assert not ci_file("cidr").kind("Secret"), "existing Secrets everywhere: the chart makes none"


# --------------------------------------------------------------------------- hooks


def hook(document):
    annotations = document["metadata"].get("annotations", {})
    if "helm.sh/hook" not in annotations:
        return None
    return set(annotations["helm.sh/hook"].split(",")), int(annotations["helm.sh/hook-weight"])


def test_bootstrap_runs_before_the_gateway_with_hooks_it_needs_made_first(ci):
    job = ci.get("Job", f"{FULLNAME}-bootstrap")
    phases, weight = hook(job)
    assert phases == {"pre-install", "pre-upgrade"}
    spec = job["spec"]["template"]["spec"]
    steps = [c["name"] for c in containers(spec)]
    assert steps[:2] == ["migrate", "admin"]
    # What the pre-install Job uses must exist before it: hooks of lower weight or existing Secrets
    referenced = {spec["serviceAccountName"]}
    for container in containers(spec):
        assert "envFrom" not in container, "the ConfigMap is not there before an install"
        for item in container.get("env", []):
            if "valueFrom" in item:
                referenced.add(item["valueFrom"]["secretKeyRef"]["name"])
    for volume in spec.get("volumes", []):
        if "secret" in volume:
            referenced.add(volume["secret"]["secretName"])
    made = {
        d["metadata"]["name"]: d for d in ci.documents if d["kind"] in ("Secret", "ServiceAccount")
    }
    for name in referenced:
        if name in made:
            made_phases, made_weight = hook(made[name])
            assert "pre-install" in made_phases and made_weight < weight, name
    policy = ci.get("NetworkPolicy", f"{FULLNAME}-bootstrap")
    policy_phases, policy_weight = hook(policy)
    assert policy_phases == {"pre-install", "pre-upgrade"} and policy_weight < weight
    for document in ci.documents:
        if document["kind"] in ("Deployment", "StatefulSet"):
            assert hook(document) is None


def test_the_steps_mirror_the_compose_init(ci_file):
    external = ci_file("default").get("Job", f"{FULLNAME}-bootstrap")["spec"]["template"]["spec"]
    commands = {c["name"]: c["command"] for c in containers(external)}
    assert commands == {
        "migrate": ["forklift-web", "migrate", "--no-input"],
        "admin": ["forklift-web", "bootstrap_admin", "--username", "admin"],
        "worker-token": [
            "forklift-web",
            "create_worker_token",
            "--name",
            "helm",
            "--from-file",
            "/run/secrets/worker-token",
        ],
        "cors": ["forklift-web", "configure_cors", "--origin", "http://localhost:8080"],
    }
    admin = env_of([c for c in containers(external) if c["name"] == "admin"][0])
    assert admin["FORKLIFT_ADMIN_PASSWORD"]["valueFrom"]["secretKeyRef"] == {
        "name": f"{FULLNAME}-admin",
        "key": "password",
    }
    assert not [
        c
        for c in containers(external)
        if c["name"] != "admin" and "FORKLIFT_ADMIN_PASSWORD" in env_of(c)
    ]
    # An admin-made token is not registered; the bundled store is set up after the install
    cidr = ci_file("cidr").get("Job", f"{FULLNAME}-bootstrap")["spec"]["template"]["spec"]
    assert [c["name"] for c in containers(cidr)] == ["migrate", "admin", "cors"]
    assert containers(cidr)[-1]["command"][-2:] == ["--origin", "https://forklift.example.org"]
    bundled = ci_file("rustfs")
    assert [
        c["name"]
        for c in containers(
            bundled.get("Job", f"{FULLNAME}-bootstrap")["spec"]["template"]["spec"]
        )
    ] == ["migrate", "admin", "worker-token"]
    setup = bundled.get("Job", f"{FULLNAME}-store-setup")
    assert hook(setup)[0] == {"post-install", "post-upgrade"}
    assert [c["name"] for c in containers(setup["spec"]["template"]["spec"])] == ["bucket", "cors"]


# --------------------------------------------------------------------------- isolation profiles


def worker_pod(rendered, group):
    return rendered.get("Deployment", f"{FULLNAME}-worker-{group}")["spec"]["template"]["spec"]


def test_isolation_profiles(ci_file):
    standard = worker_pod(ci_file("default"), "batch")
    assert standard["securityContext"]["seccompProfile"] == {"type": "RuntimeDefault"}
    env = env_of(standard["containers"][0])
    assert env["FORKLIFT_WORKER_ISOLATION"]["value"] == "standard"
    assert env["FORKLIFT_WORKER_LANDLOCK"]["value"] == "required"
    assert env["FORKLIFT_WORKER_GATEWAY"]["value"] == f"http://{FULLNAME}-gateway-internal:8081"
    assert "runtimeClassName" not in standard and "FORKLIFT_WORKER_MAX_JOBS" not in env

    no_network = ci_file("no-network")
    interactive = worker_pod(no_network, "interactive")
    assert interactive["securityContext"]["seccompProfile"] == {
        "type": "Localhost",
        "localhostProfile": "forklift/worker-no-network.json",
    }
    assert (
        env_of(interactive["containers"][0])["FORKLIFT_WORKER_ISOLATION"]["value"] == "no-network"
    )
    batch = worker_pod(no_network, "batch")
    assert (
        batch["securityContext"]["seccompProfile"]["localhostProfile"]
        == "profiles/forklift-no-network.json"
    )
    assert env_of(batch["containers"][0])["FORKLIFT_WORKER_STAGE_MAX_BYTES"]["value"] == "1GiB"

    sandboxed = worker_pod(ci_file("sandboxed"), "sandboxed")
    assert sandboxed["runtimeClassName"] == "gvisor"
    env = env_of(sandboxed["containers"][0])
    assert (
        env["FORKLIFT_WORKER_MAX_JOBS"]["value"] == "1"
        and env["FORKLIFT_WORKER_CONCURRENCY"]["value"] == "1"
    )
    assert env["FORKLIFT_WORKER_ISOLATION"]["value"] == "no-network"
    assert sandboxed["securityContext"]["seccompProfile"]["type"] == "Localhost"


def test_workers_drain_before_they_are_killed(ci_file):
    rendered = ci_file("keda")
    batch = worker_pod(rendered, "batch")
    env = env_of(batch["containers"][0])
    assert env["FORKLIFT_WORKER_DRAIN_SECONDS"]["value"] == "3600"
    assert batch["terminationGracePeriodSeconds"] == 3600 + 10 + 15


def test_keda_scales_on_the_queued_and_running_jobs_of_the_groups_lanes(ci_file):
    rendered = ci_file("keda")
    batch = rendered.get("ScaledObject", f"{FULLNAME}-worker-batch")
    assert "replicas" not in rendered.get("Deployment", f"{FULLNAME}-worker-batch")["spec"]
    assert batch["spec"]["minReplicaCount"] == 0 and batch["spec"]["maxReplicaCount"] == 20
    (trigger,) = batch["spec"]["triggers"]
    assert trigger["type"] == "postgresql"
    assert trigger["metadata"]["query"] == (
        "SELECT COUNT(*) FROM core_job WHERE status IN ('queued', 'running') "
        "AND lane IN ('batch', 'sql')"
    )
    assert trigger["metadata"]["targetQueryValue"] == "2"  # the group's concurrency
    assert trigger["metadata"]["userName"] == "forklift_keda"
    auth = rendered.get("TriggerAuthentication", trigger["authenticationRef"]["name"])
    assert auth["spec"]["secretTargetRef"] == [
        {"parameter": "password", "name": "forklift-keda-db", "key": "password"}
    ]
    big = rendered.get("ScaledObject", f"{FULLNAME}-worker-big")
    assert big["spec"]["triggers"][0]["metadata"]["targetQueryValue"] == "1"
    assert (
        not rendered.kind("ScaledObject")[0]["spec"].get("triggers")[0]["metadata"].get("password")
    )


def test_keda_query_names_the_gateways_real_table_and_columns():
    migration = (
        REPOSITORY / "services/web/src/forklift_web/core/migrations/0001_initial.py"
    ).read_text()
    job = migration[migration.index('name="Job"') :]
    job = job[: job.index("migrations.CreateModel")] if "migrations.CreateModel" in job else job
    assert re.search(r'"lane",\s*models\.CharField', job) and re.search(
        r'"status",\s*models\.CharField', job
    )
    choices = (REPOSITORY / "services/web/src/forklift_web/core/choices.py").read_text()
    assert 'QUEUED = "queued"' in choices and 'RUNNING = "running"' in choices
    assert (
        'label = "core"' in (REPOSITORY / "services/web/src/forklift_web/core/apps.py").read_text()
    )


# --------------------------------------------------------------------------- settings


def gateway_env(rendered):
    config = rendered.get("ConfigMap", f"{FULLNAME}-gateway")["data"]
    pod = rendered.get("Deployment", f"{FULLNAME}-gateway")["spec"]["template"]["spec"]
    return config, env_of(pod["containers"][0])


def test_the_background_processes_run_as_in_compose(ci_file):
    compose = yaml.safe_load((REPOSITORY / "deploy/compose/docker-compose.yml").read_text())
    rendered = ci_file("default")
    for name in ("dispatcher", "publisher", "sweeper"):
        service = compose["services"][name]
        spec = rendered.get("Deployment", f"{FULLNAME}-{name}")["spec"]["template"]["spec"]
        (container,) = spec["containers"]
        assert container["command"] == service["command"], name
        health = service["healthcheck"]["test"][1:]  # ["python", "-c", "..."] after "CMD"
        assert container["livenessProbe"]["exec"]["command"] == health, name
        assert container["startupProbe"]["exec"]["command"] == health, name
        # PID 1 ignores the SIGTERM these commands do not handle: the pause process is PID 1
        assert spec["shareProcessNamespace"] is True, name


def test_the_gateway_gets_every_setting_compose_gives_it(ci):
    compose = yaml.safe_load((REPOSITORY / "deploy/compose/docker-compose.yml").read_text())
    names = set(compose["x-gateway"]["environment"])
    config, secret_env = gateway_env(ci)
    assert names <= set(config) | set(secret_env), names - set(config) - set(secret_env)
    worker = compose["services"]["worker"]
    flags = {arg.split("=")[0] for arg in worker["command"]}
    assert flags == {"--gateway", "--lanes", "--concurrency", "--store-host", "--landlock"}
    for workload, labels, spec in ci.pod_specs():
        if component_of(labels) == "worker":
            env = env_of(spec["containers"][0])
            for name in (
                "GATEWAY",
                "LANES",
                "CONCURRENCY",
                "STORE_HOSTS",
                "LANDLOCK",
                "LOG_LEVEL",
            ):
                assert f"FORKLIFT_WORKER_{name}" in env


def settings_read_by(path: Path, pattern: str):
    return set(re.findall(pattern, path.read_text()))


def test_every_gateway_setting_is_set_by_the_chart_or_accepted_as_extra_env(ci_file):
    names = settings_read_by(
        REPOSITORY / "services/web/src/forklift_web/settings.py", r'"(FORKLIFT_[A-Z0-9_]+)"'
    )
    names |= {
        f"FORKLIFT_S3_{p}_{k}"
        for p in ("UPLOAD", "DOWNLOAD", "DELETE")
        for k in ("ACCESS_KEY_ID", "SECRET_ACCESS_KEY")
    }
    names -= {"FORKLIFT_S3_"}
    config, secret_env = gateway_env(ci_file("cidr"))
    extra = SCHEMA["properties"]["gateway"]["properties"]["extraEnv"]
    secret_extra = SCHEMA["properties"]["gateway"]["properties"]["extraSecretEnv"]["items"][
        "properties"
    ]["name"]
    for name in sorted(names):
        if name in config or name in secret_env:
            continue
        accepted = []
        for schema, value in ((extra, {name: "x"}), ({"type": "string", **secret_extra}, name)):
            try:
                jsonschema.validate(value, schema)
                accepted.append(schema)
            except jsonschema.ValidationError:
                pass
        assert accepted, f"{name} is neither set by the chart nor settable through it"


def test_every_single_sign_on_setting_is_set_by_the_chart_or_accepted_in_oidc_settings(ci_file):
    config = REPOSITORY / "services/web/src/forklift_web/oidc/config.py"
    if not config.exists():
        pytest.skip("the gateway has no single sign-on settings in this checkout")
    block = re.search(r"NAMES = \{(.*?)\}", config.read_text(), re.S).group(1)
    names = {"FORKLIFT_OIDC_" + name for name in re.findall(r'"([A-Z_]+)"', block)}
    pod = ci_file("mcp-oidc").get("Deployment", f"{FULLNAME}-gateway")["spec"]["template"]["spec"]
    values = yaml.safe_load((CHART / "ci/mcp-oidc-values.yaml").read_text())
    passed_through = set(values["gateway"]["oidc"]["settings"])
    set_by_chart = set(env_of(pod["containers"][0])) - passed_through
    set_by_chart |= {"FORKLIFT_OIDC_CLIENT_SECRET"}  # the chart uses CLIENT_SECRET_FILE instead
    settings = SCHEMA["properties"]["gateway"]["properties"]["oidc"]["properties"]["settings"]
    accepted = set(settings["propertyNames"]["enum"])
    assert names - set_by_chart == accepted, "gateway.oidc.settings must take exactly the others"


def test_every_worker_setting_is_set_by_the_chart_or_accepted_in_settings():
    options = (REPOSITORY / "services/worker/src/forklift_worker/settings.py").read_text()
    names = set(re.findall(r'variable="(FORKLIFT_[A-Z0-9_]+)"', options))
    for block in re.findall(r"Option\((.*?)\n    \)", options, re.S):
        name = re.match(r'\s*"([a-z_]+)"', block).group(1)
        if "variable=" not in block:
            names.add("FORKLIFT_WORKER_" + name.upper())
    assert {
        "FORKLIFT_WORKER_GATEWAY",
        "FORKLIFT_WORKER_STORE_HOSTS",
        "FORKLIFT_WORKER_ID",
    } <= names
    settings = SCHEMA["definitions"]["worker"]["properties"]["settings"]
    managed = set(settings["propertyNames"]["not"]["enum"])
    for name in sorted(names - managed):
        jsonschema.validate({name: "x"}, settings)  # settable through workers[].settings


def test_public_url_hosts_and_origins(ci_file):
    config, _ = gateway_env(ci_file("mcp-oidc"))
    assert config["FORKLIFT_PUBLIC_URL"] == "https://forklift.example.org"
    assert config["FORKLIFT_CSRF_TRUSTED_ORIGINS"] == "https://forklift.example.org"
    assert (
        config["FORKLIFT_BEHIND_TLS_PROXY"] == "true"
        and config["FORKLIFT_SECURE_COOKIES"] == "true"
    )
    assert config["FORKLIFT_TRUSTED_PROXIES"] == "1"
    hosts = config["FORKLIFT_ALLOWED_HOSTS"].split(",")
    assert hosts[:3] == ["localhost", "127.0.0.1", "forklift.example.org"]
    for service in (f"{FULLNAME}-gateway", f"{FULLNAME}-gateway-internal"):
        assert {
            service,
            f"{service}.{NAMESPACE}.svc",
            f"{service}.{NAMESPACE}.svc.cluster.local",
        } <= set(hosts)
    assert not [k for k in config if "OIDC" in k], "single sign-on is the web pods' own"
    default, _ = gateway_env(ci_file("default"))
    assert (
        default["FORKLIFT_PUBLIC_URL"] == "http://localhost:8080"
        and default["FORKLIFT_TRUSTED_PROXIES"] == "0"
    )
    assert not [k for k in default if k.startswith("FORKLIFT_OIDC_")]
    cidr, cidr_secrets = gateway_env(ci_file("cidr"))
    assert cidr["FORKLIFT_TRUSTED_PROXIES"] == "2" and "FORKLIFT_DB_USER" not in cidr
    assert cidr_secrets["FORKLIFT_DB_USER"]["valueFrom"]["secretKeyRef"] == {
        "name": "forklift-db",
        "key": "username",
    }
    assert cidr_secrets["FORKLIFT_S3_DELETE_ACCESS_KEY_ID"]["valueFrom"]["secretKeyRef"] == {
        "name": "forklift-s3-delete",
        "key": "id",
    }


def test_single_sign_on_reaches_the_gateways_web_pods_only(ci_file):
    rendered = ci_file("mcp-oidc")
    pod = rendered.get("Deployment", f"{FULLNAME}-gateway")["spec"]["template"]["spec"]
    env = {k: v.get("value") for k, v in env_of(pod["containers"][0]).items()}
    assert {k: v for k, v in env.items() if "OIDC" in k or k == "FORKLIFT_PASSWORD_SIGN_IN"} == {
        "FORKLIFT_OIDC_ISSUER": "https://login.example.org/realms/forklift",
        "FORKLIFT_OIDC_CLIENT_ID": "forklift",
        "FORKLIFT_OIDC_CLIENT_SECRET_FILE": "/etc/forklift/oidc/client-secret",
        "FORKLIFT_OIDC_SCOPES": "openid email profile",
        "FORKLIFT_OIDC_CA_FILE": "/etc/forklift/oidc-ca/ca.crt",
        "FORKLIFT_PASSWORD_SIGN_IN": "false",
        "FORKLIFT_OIDC_ROLE_CLAIM": "groups",
        "FORKLIFT_OIDC_ADMIN_GROUPS": "forklift-admins",
    }
    volumes = {v["name"]: v for v in pod["volumes"]}
    assert volumes["oidc"]["secret"] == {
        "secretName": "forklift-oidc",
        "items": [{"key": "client-secret", "path": "client-secret"}],
        "defaultMode": 0o440,
    }
    assert volumes["oidc-ca"]["configMap"]["name"] == "keycloak-ca"
    for workload, labels, spec in rendered.pod_specs():
        if component_of(labels) != "gateway":
            for container in containers(spec):
                assert not [n for n in env_of(container) if "OIDC" in n], workload["metadata"][
                    "name"
                ]
            assert "oidc" not in {v["name"] for v in spec.get("volumes", [])}


def test_the_mcp_proxy_settings(ci_file):
    pod = ci_file("mcp-oidc").get("Deployment", f"{FULLNAME}-mcp")["spec"]["template"]["spec"]
    env = {k: v["value"] for k, v in env_of(pod["containers"][0]).items()}
    assert env["FORKLIFT_MCP_API_URL"] == f"http://{FULLNAME}-gateway:8080"
    assert env["FORKLIFT_MCP_ALLOWED_HOSTS"].split(",") == [
        "forklift.example.org",
        f"{FULLNAME}-mcp",
        f"{FULLNAME}-mcp.{NAMESPACE}.svc",
        "mcp.internal.example.org",
    ]
    assert pod["containers"][0]["readinessProbe"]["httpGet"]["path"] == "/healthz"
    ingress = ci_file("mcp-oidc").get("Ingress", FULLNAME)
    paths = ingress["spec"]["rules"][0]["http"]["paths"]
    assert [(p["path"], p["backend"]["service"]["name"]) for p in paths] == [
        ("/mcp", f"{FULLNAME}-mcp"),
        ("/", f"{FULLNAME}-gateway"),
    ]


# --------------------------------------------------------------------------- values that fail

MINIMAL = {
    "postgres": {"host": "db", "existingSecret": "db"},
    "objectStore": {"endpoint": "https://s3.example.org", "existingSecret": "s3"},
    "networkPolicies": {
        "postgres": {"to": [{"ipBlock": {"cidr": "10.0.0.5/32"}}]},
        "storeEgress": {"mode": "cidr", "cidrs": ["203.0.113.0/24"]},
    },
}


def with_(**changes):
    values = json.loads(json.dumps(MINIMAL))
    for path, value in changes.items():
        node = values
        *parents, last = path.split("__")
        for key in parents:
            node = node.setdefault(key, {})
        node[last] = value
    return values


@pytest.mark.parametrize(
    "values, message",
    [
        (with_(postgres__host=""), "postgres.host is required"),
        (with_(postgres__existingSecret=""), "postgres.existingSecret is required"),
        (with_(objectStore__endpoint=""), "objectStore.endpoint is required"),
        (
            with_(networkPolicies__storeEgress={"mode": ""}),
            "networkPolicies.storeEgress.mode is required with an external store",
        ),
        (
            with_(networkPolicies__storeEgress={"mode": "cidr"}),
            "networkPolicies.storeEgress.cidrs is required in mode cidr",
        ),
        (with_(networkPolicies__postgres={"to": []}), "networkPolicies.postgres.to is required"),
        (
            with_(gateway__ingress={"enabled": True}, networkPolicies__ingressController=[]),
            "networkPolicies.ingressController is empty",
        ),
        (
            with_(workers=[{"name": "a", "lanes": ["batch"], "isolation": "sandboxed"}]),
            "isolation sandboxed needs runtimeClassName",
        ),
        (
            with_(workers=[{"name": "a", "lanes": ["sql"], "isolation": "no-network"}]),
            "the no-network profile refuses it",
        ),
        (
            with_(workers=[{"name": "a", "lanes": ["batch"]}, {"name": "a", "lanes": ["sql"]}]),
            'the name "a" is used twice',
        ),
        (
            with_(
                gateway__oidc={
                    "enabled": True,
                    "issuer": "https://id.example.org",
                    "clientId": "f",
                }
            ),
            "gateway.oidc.clientSecret.existingSecret is required",
        ),
        (
            with_(gateway__oidc={"settings": {"FORKLIFT_OIDC_ENABLED": "true"}}),
            "FORKLIFT_OIDC_ENABLED",
        ),
        (
            with_(gateway__extraEnv={"FORKLIFT_DB_PASSWORD": "x"}),
            "invalid propertyName 'FORKLIFT_DB_PASSWORD'",
        ),
        (
            with_(gateway__extraEnv={"FORKLIFT_SOMETHING_TOKEN": "x"}),
            "invalid propertyName 'FORKLIFT_SOMETHING_TOKEN'",
        ),
        (with_(gateway__ingres={"enabled": True}), "ingres"),
        (with_(workers=[{"name": "Batch", "lanes": ["batch"]}]), "name"),
        (with_(workers=[{"name": "a", "lanes": ["batch'); DROP TABLE core_job; --"]}]), "lanes"),
        (
            with_(workerDefaults={"settings": {"FORKLIFT_WORKER_GATEWAY": "http://elsewhere"}}),
            "invalid propertyName 'FORKLIFT_WORKER_GATEWAY'",
        ),
    ],
    ids=lambda v: v if isinstance(v, str) else None,
)
def test_values_that_cannot_work_are_refused_with_the_reason(values, message):
    with pytest.raises(RenderError) as raised:
        render(None, extra=values)
    assert message in str(raised.value)


def test_the_minimal_values_render():
    rendered = render(None, extra=MINIMAL)
    assert rendered.get("Deployment", f"{FULLNAME}-gateway")["spec"]["replicas"] == 2


def test_network_policies_off_still_renders_and_warns():
    rendered = render(None, extra=with_(networkPolicies={"enabled": False}))
    assert not rendered.kind("NetworkPolicy")
