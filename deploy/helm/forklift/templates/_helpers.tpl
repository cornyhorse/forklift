{{/* ------------------------------------------------------------------ names and labels */}}

{{- define "forklift.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* The release's full name; component names append to it, so it stays at most 40 characters. */}}
{{- define "forklift.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 40 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 40 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 40 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "forklift.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Labels that select every pod of this release (the default-deny NetworkPolicy uses them). */}}
{{- define "forklift.instanceLabels" -}}
app.kubernetes.io/name: {{ include "forklift.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Selector labels of one component: (dict "root" $ "component" "gateway") */}}
{{- define "forklift.selectorLabels" -}}
{{ include "forklift.instanceLabels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- if .group }}
forklift.cornyhorse.dev/worker-group: {{ .group }}
{{- end }}
{{- end -}}

{{/* Every label of one component's resources. */}}
{{- define "forklift.labels" -}}
{{ include "forklift.selectorLabels" . }}
app.kubernetes.io/part-of: forklift
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
helm.sh/chart: {{ include "forklift.chart" .root }}
{{- with .root.Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- define "forklift.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "forklift.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* An image reference: (dict "root" $ "image" .Values.image.web) */}}
{{- define "forklift.image" -}}
{{- $registry := .root.Values.image.registry -}}
{{- $tag := default (default .root.Chart.AppVersion .root.Values.image.tag) .image.tag -}}
{{- $name := ternary (printf "%s/%s" $registry .image.name) .image.name (ne $registry "") -}}
{{- if .image.digest -}}
{{- printf "%s@%s" $name .image.digest -}}
{{- else -}}
{{- printf "%s:%s" $name $tag -}}
{{- end -}}
{{- end -}}

{{/* A third-party image: {repository, tag, digest} */}}
{{- define "forklift.externalImage" -}}
{{- if .digest -}}
{{- printf "%s@%s" .repository .digest -}}
{{- else -}}
{{- printf "%s:%s" .repository .tag -}}
{{- end -}}
{{- end -}}

{{- define "forklift.imagePullSecrets" -}}
{{- with .Values.image.pullSecrets }}
imagePullSecrets:
{{- range . }}
  - name: {{ . }}
{{- end }}
{{- end }}
{{- end -}}

{{/* ------------------------------------------------------------------ security contexts */}}

{{/* Pod Security "restricted": (dict "uid" 10001 "seccomp" (dict "type" "RuntimeDefault")) */}}
{{- define "forklift.podSecurityContext" -}}
securityContext:
  runAsNonRoot: true
  runAsUser: {{ .uid | default 10001 }}
  runAsGroup: {{ .uid | default 10001 }}
  fsGroup: {{ .uid | default 10001 }}
  fsGroupChangePolicy: OnRootMismatch
  seccompProfile:
    {{- toYaml (.seccomp | default (dict "type" "RuntimeDefault")) | nindent 4 }}
{{- end -}}

{{- define "forklift.containerSecurityContext" -}}
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  runAsNonRoot: true
  capabilities:
    drop: [ALL]
{{- end -}}

{{/* ------------------------------------------------------------------ URLs and names */}}

{{- define "forklift.gatewayService" -}}
{{- printf "%s-gateway" (include "forklift.fullname" .) -}}
{{- end -}}

{{- define "forklift.gatewayInternalService" -}}
{{- printf "%s-gateway-internal" (include "forklift.fullname" .) -}}
{{- end -}}

{{- define "forklift.rustfsService" -}}
{{- printf "%s-rustfs" (include "forklift.fullname" .) -}}
{{- end -}}

{{- define "forklift.egressProxyService" -}}
{{- printf "%s-egress-proxy" (include "forklift.fullname" .) -}}
{{- end -}}

{{- define "forklift.publicUrl" -}}
{{- if .Values.publicUrl -}}
{{- .Values.publicUrl | trimSuffix "/" -}}
{{- else if .Values.gateway.ingress.enabled -}}
{{- printf "%s://%s" (ternary "https" "http" .Values.gateway.ingress.tls) .Values.gateway.ingress.host -}}
{{- else -}}
http://localhost:8080
{{- end -}}
{{- end -}}

{{/* scheme://host[:port] of a URL */}}
{{- define "forklift.origin" -}}
{{- $url := urlParse . -}}
{{- printf "%s://%s" $url.scheme $url.host -}}
{{- end -}}

{{- define "forklift.behindTlsProxy" -}}
{{- if kindIs "bool" .Values.gateway.behindTlsProxy -}}
{{- .Values.gateway.behindTlsProxy -}}
{{- else -}}
{{- and .Values.gateway.ingress.enabled .Values.gateway.ingress.tls -}}
{{- end -}}
{{- end -}}

{{/* ------------------------------------------------------------------ the object store */}}

{{- define "forklift.s3.endpoint" -}}
{{- if .Values.objectStore.endpoint -}}
{{- .Values.objectStore.endpoint -}}
{{- else if .Values.rustfs.enabled -}}
{{- /* The full name: Squid (proxy mode) does not resolve single-label names */ -}}
{{- printf "http://%s.%s.svc.%s:%v" (include "forklift.rustfsService" .) .Release.Namespace .Values.clusterDomain .Values.rustfs.service.port -}}
{{- else -}}
{{- fail "objectStore.endpoint is required (the S3-compatible store's URL, e.g. https://s3.example.org), unless rustfs.enabled=true runs the bundled RustFS." -}}
{{- end -}}
{{- end -}}

{{- define "forklift.s3.workerEndpoint" -}}
{{- default (include "forklift.s3.endpoint" .) .Values.objectStore.workerEndpoint -}}
{{- end -}}

{{- define "forklift.s3.publicEndpoint" -}}
{{- if .Values.objectStore.publicEndpoint -}}
{{- .Values.objectStore.publicEndpoint -}}
{{- else if and .Values.rustfs.enabled .Values.rustfs.ingress.enabled -}}
{{- printf "%s://%s" (ternary "https" "http" .Values.rustfs.ingress.tls) .Values.rustfs.ingress.host -}}
{{- else if .Values.rustfs.enabled -}}
http://localhost:9000
{{- else -}}
{{- include "forklift.s3.endpoint" . -}}
{{- end -}}
{{- end -}}

{{/* host[:port] of a URL as the worker's --store-host expects it */}}
{{- define "forklift.hostPort" -}}
{{- (urlParse .).host | lower -}}
{{- end -}}

{{/* The port of a URL (explicit, else 443 for https and 80 for http) */}}
{{- define "forklift.urlPort" -}}
{{- $url := urlParse . -}}
{{- $parts := splitList ":" $url.host -}}
{{- if gt (len $parts) 1 -}}
{{- last $parts -}}
{{- else -}}
{{- ternary "443" "80" (eq $url.scheme "https") -}}
{{- end -}}
{{- end -}}

{{/* The store's ports for NetworkPolicies, as a YAML list of numbers */}}
{{- define "forklift.storePorts" -}}
{{- if .Values.networkPolicies.storeEgress.ports -}}
{{- toYaml .Values.networkPolicies.storeEgress.ports -}}
{{- else -}}
{{- $ports := list (include "forklift.urlPort" (include "forklift.s3.endpoint" .) | atoi) (include "forklift.urlPort" (include "forklift.s3.workerEndpoint" .) | atoi) -}}
{{- toYaml (uniq $ports) -}}
{{- end -}}
{{- end -}}

{{- define "forklift.storeSecretName" -}}
{{- if .Values.objectStore.existingSecret -}}
{{- .Values.objectStore.existingSecret -}}
{{- else if .Values.rustfs.enabled -}}
{{- default (printf "%s-rustfs" (include "forklift.fullname" .)) .Values.rustfs.existingSecret -}}
{{- else -}}
{{- fail "objectStore.existingSecret is required: a Secret with the store's access key id and secret access key (keys objectStore.accessKeyIdKey and objectStore.secretAccessKeyKey)." -}}
{{- end -}}
{{- end -}}

{{/* The storeEgress mode, with the default resolved */}}
{{- define "forklift.storeMode" -}}
{{- $mode := .Values.networkPolicies.storeEgress.mode -}}
{{- if $mode -}}
{{- $mode -}}
{{- else if .Values.rustfs.enabled -}}
podSelector
{{- else if .Values.networkPolicies.enabled -}}
{{- fail "networkPolicies.storeEgress.mode is required with an external store: cidr (the store's address ranges in networkPolicies.storeEgress.cidrs), proxy (an allow-listing egress proxy) or podSelector (an in-cluster store, networkPolicies.storeEgress.podSelector); or set networkPolicies.enabled=false." -}}
{{- else -}}
none
{{- end -}}
{{- end -}}

{{- define "forklift.useEgressProxy" -}}
{{- if and .Values.networkPolicies.enabled (eq (include "forklift.storeMode" .) "proxy") -}}true{{- end -}}
{{- end -}}

{{/* ------------------------------------------------------------------ gateway settings */}}

{{- define "forklift.secretsSecretName" -}}
{{- default (printf "%s-secrets" (include "forklift.fullname" .)) .Values.secrets.existingSecret -}}
{{- end -}}

{{- define "forklift.adminSecretName" -}}
{{- default (printf "%s-admin" (include "forklift.fullname" .)) .Values.admin.existingSecret -}}
{{- end -}}

{{- define "forklift.workerTokenSecretName" -}}
{{- default (printf "%s-worker-token" (include "forklift.fullname" .)) .Values.workerToken.existingSecret -}}
{{- end -}}

{{- define "forklift.allowedHosts" -}}
{{- $hosts := list "localhost" "127.0.0.1" -}}
{{- if .Values.gateway.ingress.enabled -}}
{{- $hosts = append $hosts .Values.gateway.ingress.host -}}
{{- end -}}
{{- $publicHost := (urlParse (include "forklift.publicUrl" .)).host | splitList ":" | first -}}
{{- $hosts = append $hosts $publicHost -}}
{{- range $service := list (include "forklift.gatewayService" .) (include "forklift.gatewayInternalService" .) -}}
{{- $hosts = concat $hosts (list $service (printf "%s.%s" $service $.Release.Namespace) (printf "%s.%s.svc" $service $.Release.Namespace) (printf "%s.%s.svc.%s" $service $.Release.Namespace $.Values.clusterDomain)) -}}
{{- end -}}
{{- $hosts = concat $hosts .Values.gateway.allowedHosts -}}
{{- uniq $hosts | join "," -}}
{{- end -}}

{{/* The non-secret settings of every gateway process, as a YAML map (the ConfigMap's data) */}}
{{- define "forklift.gatewaySettings" -}}
{{- $g := .Values.gateway -}}
{{- $public := include "forklift.publicUrl" . -}}
{{- $tls := eq (include "forklift.behindTlsProxy" .) "true" -}}
{{- $secure := ternary $g.secureCookies $tls (kindIs "bool" $g.secureCookies) -}}
{{- $proxies := ternary $g.trustedProxies (ternary 1 0 $g.ingress.enabled) (not (kindIs "invalid" $g.trustedProxies)) -}}
{{- $origins := uniq (prepend $g.csrfTrustedOrigins (include "forklift.origin" $public)) -}}
FORKLIFT_PUBLIC_PORT: "8080"
FORKLIFT_INTERNAL_PORT: "8081"
FORKLIFT_PUBLIC_URL: {{ $public | quote }}
FORKLIFT_ALLOWED_HOSTS: {{ include "forklift.allowedHosts" . | quote }}
FORKLIFT_CSRF_TRUSTED_ORIGINS: {{ join "," $origins | quote }}
FORKLIFT_BEHIND_TLS_PROXY: {{ $tls | quote }}
FORKLIFT_SECURE_COOKIES: {{ $secure | quote }}
FORKLIFT_TRUSTED_PROXIES: {{ $proxies | int | quote }}
FORKLIFT_SESSION_SECONDS: {{ $g.sessionSeconds | int | quote }}
FORKLIFT_PASSWORD_MIN_LENGTH: {{ $g.passwordMinLength | int | quote }}
FORKLIFT_LOGIN_REDIRECT_URL: {{ $g.loginRedirectUrl | quote }}
FORKLIFT_API_DOCS: {{ $g.apiDocs | quote }}
FORKLIFT_LOG_LEVEL: {{ $g.logLevel | quote }}
FORKLIFT_WEBHOOK_ALLOWED_HOSTS: {{ join "," $g.webhooks.allowedHosts | quote }}
FORKLIFT_WEBHOOK_ALLOW_HTTP: {{ $g.webhooks.allowHttp | quote }}
FORKLIFT_WEB_WORKERS: {{ $g.server.workers | int | quote }}
FORKLIFT_WEB_THREADS: {{ $g.server.threads | int | quote }}
FORKLIFT_WEB_TIMEOUT: {{ $g.server.timeoutSeconds | int | quote }}
FORKLIFT_DB_HOST: {{ required "postgres.host is required (the PostgreSQL server's host name)" .Values.postgres.host | quote }}
FORKLIFT_DB_PORT: {{ .Values.postgres.port | int | quote }}
FORKLIFT_DB_NAME: {{ .Values.postgres.database | quote }}
{{- if not .Values.postgres.userKey }}
FORKLIFT_DB_USER: {{ .Values.postgres.user | quote }}
{{- end }}
FORKLIFT_DB_SSLMODE: {{ .Values.postgres.sslmode | quote }}
FORKLIFT_DB_CONN_MAX_AGE: {{ .Values.postgres.connMaxAge | int | quote }}
FORKLIFT_S3_ENDPOINT_URL: {{ include "forklift.s3.endpoint" . | quote }}
FORKLIFT_S3_PUBLIC_ENDPOINT_URL: {{ include "forklift.s3.publicEndpoint" . | quote }}
FORKLIFT_S3_WORKER_ENDPOINT_URL: {{ include "forklift.s3.workerEndpoint" . | quote }}
FORKLIFT_S3_BUCKET: {{ .Values.objectStore.bucket | quote }}
FORKLIFT_S3_REGION: {{ .Values.objectStore.region | quote }}
FORKLIFT_S3_ADDRESSING_STYLE: {{ .Values.objectStore.addressingStyle | quote }}
FORKLIFT_S3_CONNECT_TIMEOUT: {{ .Values.objectStore.connectTimeout | int | quote }}
FORKLIFT_S3_READ_TIMEOUT: {{ .Values.objectStore.readTimeout | int | quote }}
{{- range $name, $value := $g.extraEnv }}
{{ $name }}: {{ $value | toString | quote }}
{{- end }}
{{- end -}}

{{/* Environment variables of every gateway process that come from Secrets (a YAML list) */}}
{{- define "forklift.gatewaySecretEnv" -}}
{{- $store := include "forklift.storeSecretName" . -}}
- name: FORKLIFT_SECRET_KEY
  valueFrom: {secretKeyRef: {name: {{ include "forklift.secretsSecretName" . }}, key: {{ .Values.secrets.secretKeyKey }}}}
- name: FORKLIFT_SECRETS_KEYS
  valueFrom: {secretKeyRef: {name: {{ include "forklift.secretsSecretName" . }}, key: {{ .Values.secrets.secretsKeysKey }}}}
- name: FORKLIFT_DB_PASSWORD
  valueFrom: {secretKeyRef: {name: {{ required "postgres.existingSecret is required: a Secret with the database password (key postgres.passwordKey)" .Values.postgres.existingSecret }}, key: {{ .Values.postgres.passwordKey }}}}
{{- if .Values.postgres.userKey }}
- name: FORKLIFT_DB_USER
  valueFrom: {secretKeyRef: {name: {{ .Values.postgres.existingSecret }}, key: {{ .Values.postgres.userKey }}}}
{{- end }}
- name: FORKLIFT_S3_ACCESS_KEY_ID
  valueFrom: {secretKeyRef: {name: {{ $store }}, key: {{ .Values.objectStore.accessKeyIdKey }}}}
- name: FORKLIFT_S3_SECRET_ACCESS_KEY
  valueFrom: {secretKeyRef: {name: {{ $store }}, key: {{ .Values.objectStore.secretAccessKeyKey }}}}
{{- range $purpose, $credential := .Values.objectStore.purposeCredentials }}
- name: FORKLIFT_S3_{{ upper $purpose }}_ACCESS_KEY_ID
  valueFrom: {secretKeyRef: {name: {{ $credential.existingSecret }}, key: {{ $credential.accessKeyIdKey | default "access-key-id" }}}}
- name: FORKLIFT_S3_{{ upper $purpose }}_SECRET_ACCESS_KEY
  valueFrom: {secretKeyRef: {name: {{ $credential.existingSecret }}, key: {{ $credential.secretAccessKeyKey | default "secret-access-key" }}}}
{{- end }}
{{- range .Values.gateway.extraSecretEnv }}
- name: {{ .name }}
  valueFrom: {secretKeyRef: {name: {{ .secretName }}, key: {{ .key }}}}
{{- end }}
{{- end -}}

{{/* Single sign-on (forklift_web.oidc.config): the gateway's web pods only, which serve the
sign-in; FORKLIFT_OIDC_ISSUER turns it on. The client secret is a mounted file, never an
environment variable. A YAML list of env entries. */}}
{{- define "forklift.gatewayOidcEnv" -}}
{{- with .Values.gateway.oidc }}
{{- if .enabled }}
- {name: FORKLIFT_OIDC_ISSUER, value: {{ required "gateway.oidc.issuer is required with gateway.oidc.enabled (the provider's issuer URL)" .issuer | quote }}}
- {name: FORKLIFT_OIDC_CLIENT_ID, value: {{ required "gateway.oidc.clientId is required with gateway.oidc.enabled" .clientId | quote }}}
- {name: FORKLIFT_OIDC_CLIENT_SECRET_FILE, value: /etc/forklift/oidc/client-secret}
- {name: FORKLIFT_OIDC_SCOPES, value: {{ join " " .scopes | quote }}}
{{- if .caBundle.configMap }}
- {name: FORKLIFT_OIDC_CA_FILE, value: /etc/forklift/oidc-ca/ca.crt}
{{- end }}
- {name: FORKLIFT_PASSWORD_SIGN_IN, value: {{ .passwordSignIn | quote }}}
{{- range $name, $value := .settings }}
- {name: {{ $name }}, value: {{ $value | toString | quote }}}
{{- end }}
{{- end }}
{{- end }}
{{- end -}}

{{/* The gateway processes' volumes and mounts: a small /tmp (heartbeats, gunicorn) */}}
{{- define "forklift.tmpVolume" -}}
- name: tmp
  emptyDir: {medium: Memory, sizeLimit: 64Mi}
{{- end -}}

{{/* ------------------------------------------------------------------ workers */}}

{{/* A deep merge of .src into a copy of .dst (maps merge, other values replace, also zero
values); keys in .replace are replaced whole. Returns YAML. */}}
{{- define "forklift.deepMerge" -}}
{{- $dst := deepCopy .dst -}}
{{- $replace := .replace | default list -}}
{{- range $key, $value := .src -}}
{{- if and (kindIs "map" $value) (kindIs "map" (get $dst $key)) (not (has $key $replace)) -}}
{{- $_ := set $dst $key (include "forklift.deepMerge" (dict "dst" (get $dst $key) "src" $value) | fromYaml) -}}
{{- else -}}
{{- $_ := set $dst $key $value -}}
{{- end -}}
{{- end -}}
{{- toYaml $dst -}}
{{- end -}}

{{/* The worker groups with workerDefaults applied: YAML {workers: [...]} */}}
{{- define "forklift.workers" -}}
{{- $groups := list -}}
{{- $names := list -}}
{{- range $entry := .Values.workers -}}
{{- $w := include "forklift.deepMerge" (dict "dst" $.Values.workerDefaults "src" $entry "replace" (list "resources" "affinity" "seccompProfile")) | fromYaml -}}
{{- if has $w.name $names -}}
{{- fail (printf "workers: the name %q is used twice; every worker group needs a name of its own" $w.name) -}}
{{- end -}}
{{- $names = append $names $w.name -}}
{{- if eq $w.isolation "sandboxed" -}}
{{- if not $w.runtimeClassName -}}
{{- fail (printf "workers[%s]: isolation sandboxed needs runtimeClassName (a sandboxed runtime such as gvisor or kata installed on the nodes)" $w.name) -}}
{{- end -}}
{{- end -}}
{{- $engine := ternary $w.sandboxedEngine $w.isolation (eq $w.isolation "sandboxed") -}}
{{- if and (eq $engine "no-network") (has "sql" $w.lanes) -}}
{{- fail (printf "workers[%s]: the sql lane needs network access to databases; the no-network profile refuses it (give sql its own worker group with isolation standard)" $w.name) -}}
{{- end -}}
{{- $_ := set $w "engineIsolation" $engine -}}
{{- $groups = append $groups $w -}}
{{- end -}}
{{- toYaml (dict "workers" $groups) -}}
{{- end -}}

{{- define "forklift.workerComponent" -}}
worker
{{- end -}}

{{/* ------------------------------------------------------------------ NetworkPolicy pieces */}}

{{/* Egress to cluster DNS */}}
{{- define "forklift.np.dns" -}}
- to:
    {{- toYaml .Values.networkPolicies.dns.to | nindent 4 }}
  ports:
    {{- range .Values.networkPolicies.dns.ports }}
    - {protocol: UDP, port: {{ . }}}
    - {protocol: TCP, port: {{ . }}}
    {{- end }}
{{- end -}}

{{/* A peer selecting one component of this release: (dict "root" $ "component" "gateway") */}}
{{- define "forklift.np.peer" -}}
- podSelector:
    matchLabels:
      {{- include "forklift.selectorLabels" . | nindent 6 }}
{{- end -}}

{{/* Egress to PostgreSQL */}}
{{- define "forklift.np.postgres" -}}
{{- if not .Values.networkPolicies.postgres.to -}}
{{- fail "networkPolicies.postgres.to is required while NetworkPolicies are on: where PostgreSQL is, as NetworkPolicy peers (e.g. [{ipBlock: {cidr: 10.0.12.5/32}}] or a namespaceSelector and podSelector)" -}}
{{- end -}}
- to:
    {{- toYaml .Values.networkPolicies.postgres.to | nindent 4 }}
  ports:
    - {protocol: TCP, port: {{ .Values.postgres.port }}}
{{- end -}}

{{/* Peers that are the store, for the workers and the gateway processes in podSelector and
cidr modes */}}
{{- define "forklift.np.storePeers" -}}
{{- $mode := include "forklift.storeMode" . -}}
{{- $se := .Values.networkPolicies.storeEgress -}}
{{- if eq $mode "podSelector" -}}
{{- if and .Values.rustfs.enabled (not $se.podSelector) -}}
{{ include "forklift.np.peer" (dict "root" . "component" "rustfs") }}
{{- else -}}
{{- if not $se.podSelector -}}
{{- fail "networkPolicies.storeEgress.podSelector is required in mode podSelector without the bundled RustFS (the store's pod labels)" -}}
{{- end -}}
{{- $peer := dict "podSelector" $se.podSelector -}}
{{- with $se.namespaceSelector }}{{- $_ := set $peer "namespaceSelector" . -}}{{- end -}}
{{- toYaml (list $peer) -}}
{{- end -}}
{{- else if eq $mode "cidr" -}}
{{- if not $se.cidrs -}}
{{- fail "networkPolicies.storeEgress.cidrs is required in mode cidr (the store's address ranges, e.g. [203.0.113.0/24])" -}}
{{- end -}}
{{- $peers := list -}}
{{- range $se.cidrs -}}
{{- $peers = append $peers (dict "ipBlock" (dict "cidr" .)) -}}
{{- end -}}
{{- toYaml $peers -}}
{{- end -}}
{{- end -}}

{{/* The store's ports as NetworkPolicy ports */}}
{{- define "forklift.np.storePorts" -}}
{{- $ports := list -}}
{{- if and .Values.rustfs.enabled (eq (include "forklift.storeMode" .) "podSelector") (not .Values.networkPolicies.storeEgress.podSelector) (not .Values.networkPolicies.storeEgress.ports) -}}
{{- $ports = list .Values.rustfs.service.port -}}
{{- else -}}
{{- $ports = include "forklift.storePorts" . | fromYamlArray -}}
{{- end -}}
{{- $rules := list -}}
{{- range $ports -}}
{{- $rules = append $rules (dict "protocol" "TCP" "port" (int .)) -}}
{{- end -}}
{{- toYaml $rules -}}
{{- end -}}

{{/* Egress of the gateway processes to the store (they call it directly in every mode) */}}
{{- define "forklift.np.gatewayStore" -}}
{{- $mode := include "forklift.storeMode" . -}}
- to:
    {{- if eq $mode "proxy" }}
    {{- if .Values.networkPolicies.storeEgress.gatewayTo }}
    {{- toYaml .Values.networkPolicies.storeEgress.gatewayTo | nindent 4 }}
    {{- else if .Values.rustfs.enabled }}
    {{- include "forklift.np.peer" (dict "root" . "component" "rustfs") | nindent 4 }}
    {{- else }}
    {{- include "forklift.np.internet" . | nindent 4 }}
    {{- end }}
    {{- else }}
    {{- include "forklift.np.storePeers" . | nindent 4 }}
    {{- end }}
  ports:
    {{- if and (eq $mode "proxy") .Values.rustfs.enabled (not .Values.networkPolicies.storeEgress.gatewayTo) }}
    - {protocol: TCP, port: {{ .Values.rustfs.service.port }}}
    {{- else }}
    {{- include "forklift.np.storePorts" . | nindent 4 }}
    {{- end }}
{{- end -}}

{{/* "The internet" as ipBlock peers */}}
{{- define "forklift.np.internet" -}}
{{- $peers := list -}}
{{- range .Values.networkPolicies.internet -}}
{{- $block := dict "cidr" .cidr -}}
{{- with .except }}{{- $_ := set $block "except" . -}}{{- end -}}
{{- $peers = append $peers (dict "ipBlock" $block) -}}
{{- end -}}
{{- toYaml $peers -}}
{{- end -}}

{{/* The ingress controller's pods as peers (never an empty list: that would admit everyone) */}}
{{- define "forklift.np.ingressController" -}}
{{- if not .Values.networkPolicies.ingressController -}}
{{- fail "networkPolicies.ingressController is empty while an ingress is enabled: name the ingress controller's pods (namespaceSelector and podSelector), or add other sources with networkPolicies.gateway.extraIngress" -}}
{{- end -}}
{{- toYaml .Values.networkPolicies.ingressController -}}
{{- end -}}

{{/* The policy types every chart policy has */}}
{{- define "forklift.np.types" -}}
policyTypes: [Ingress, Egress]
{{- end -}}

{{/* Lane names as an SQL list: 'batch', 'sql' (lane names are [a-z0-9_-] only, by the schema) */}}
{{- define "forklift.sqlList" -}}
{{- $quoted := list -}}
{{- range . -}}
{{- if not (regexMatch "^[a-z][a-z0-9_-]{0,31}$" .) -}}
{{- fail (printf "%q is not a lane name (lower-case letters, digits, '-' and '_')" .) -}}
{{- end -}}
{{- $quoted = append $quoted (printf "'%s'" .) -}}
{{- end -}}
{{- join ", " $quoted -}}
{{- end -}}

{{/* Hook annotations: (dict "hooks" "pre-install,pre-upgrade" "weight" -10 "delete" "before-hook-creation") */}}
{{- define "forklift.hookAnnotations" -}}
helm.sh/hook: {{ .hooks }}
helm.sh/hook-weight: {{ .weight | quote }}
helm.sh/hook-delete-policy: {{ .delete | default "before-hook-creation" }}
{{- end -}}

{{/* A value kept from the existing Secret (lookup), else a new one:
(dict "root" $ "secret" name "key" key "new" value) -> the base64 data */}}
{{- define "forklift.keptSecretValue" -}}
{{- $existing := lookup "v1" "Secret" .root.Release.Namespace .secret -}}
{{- if and $existing $existing.data (hasKey $existing.data .key) -}}
{{- index $existing.data .key -}}
{{- else -}}
{{- .new | b64enc -}}
{{- end -}}
{{- end -}}

{{/* The egress proxy's allowed destinations: a YAML list of {scheme, host, port} from
egressProxy.allow (URLs, as FORKLIFT_EGRESS_PROXY_ALLOW in Compose), by default the workers'
store endpoint (and, with virtual-host addressing, the bucket's host name under it). */}}
{{- define "forklift.egressProxyAllow" -}}
{{- $urls := .Values.egressProxy.allow -}}
{{- if not $urls -}}
{{- $endpoint := include "forklift.s3.workerEndpoint" . -}}
{{- $url := urlParse $endpoint -}}
{{- $urls = list (printf "%s://%s" $url.scheme $url.host) -}}
{{- if eq .Values.objectStore.addressingStyle "virtual" -}}
{{- $urls = append $urls (printf "%s://%s.%s" $url.scheme .Values.objectStore.bucket $url.host) -}}
{{- end -}}
{{- end -}}
{{- $entries := list -}}
{{- range $urls -}}
{{- $url := urlParse . -}}
{{- if or (not (has $url.scheme (list "http" "https"))) (not $url.host) (not (has $url.path (list "" "/"))) $url.query -}}
{{- fail (printf "egressProxy.allow: %q is not an http:// or https:// URL with a host and nothing after it but an optional port" .) -}}
{{- end -}}
{{- $parts := splitList ":" $url.host -}}
{{- $entries = append $entries (dict "scheme" $url.scheme "host" (first $parts) "port" (include "forklift.urlPort" .)) -}}
{{- end -}}
{{- toYaml $entries -}}
{{- end -}}

{{/* A request to Squid from 127.0.0.1 (its log leaves those out): Squid answers, not just its port */}}
{{- define "forklift.egressProxyProbe" -}}
["bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/3128 && printf 'GET http://127.0.0.1/ HTTP/1.0\\r\\n\\r\\n' >&3 && read -r -t 4 status <&3 && [[ $status == HTTP/* ]]"]
{{- end -}}
