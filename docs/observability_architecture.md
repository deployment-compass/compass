"""
COMPASS OBSERVABILITY ARCHITECTURE – ENVIRONMENT-FLEXIBLE DESIGN

This document explains how Compass supports both local (Node Exporter) and
Kubernetes deployments without requiring separate code paths or architecture changes.
"""

# ============================================================================

# DESIGN PRINCIPLES

# ============================================================================

"""

1. SINGLE ADAPTER FOR ALL ENVIRONMENTS

   PrometheusAdapter is the unified entry point for all infrastructure and
   application metrics. It works with:

   ✓ Local Node Exporter (process_* metrics)
   ✓ Docker Compose with process metrics
   ✓ Kubernetes with container metrics + K8s labels

   No branching on environment; no separate "NodeExporterAdapter" or "K8sAdapter".
   The adapter discovers what's available and adapts queries.

2. DYNAMIC DISCOVERY > HARDCODED ASSUMPTIONS

   LabelDiscovery probes the live Prometheus instance to detect:
   - Which metric families exist (container_* vs process_*)
   - Which labels are populated (service, app, handler, job, etc.)
   - Which K8s labels are available (namespace, pod, container)

   Results are cached per-process with a TTL, so discovery only costs a few
   HTTP round-trips on startup and cache expiry, not per-query.

3. SERVICE + ENVIRONMENT = PRIMARY IDENTITY

   Regardless of deployment topology, every metric is identified by:
   - service: The logical service name (checkout-api, inventory-api, etc.)
   - environment: The deployment environment (local, staging, prod)

   These are discovered from labels in a prioritized order:
   - service label (most common)
   - app label (Kubernetes convention)
   - app_kubernetes_io_name (K8s standard)
   - handler, route, endpoint, job (fallbacks)

   K8s context (namespace, pod, container) is OPTIONAL ENRICHMENT only,
   never the primary identity.

4. K8S CONTEXT IS OPTIONAL ENRICHMENT

   When running on Kubernetes, the adapter discovers and attaches:
   - namespace: The K8s namespace (if present)
   - pod: The pod name (if present)
   - container: The container name (if present)

   These are stored in MetricSample and accessible to downstream layers
   for detailed debugging, but the anomaly-detection model only needs
   service + environment + metrics.

# ============================================================================

# ARCHITECTURE DETECTION

# ============================================================================

The adapter detects the deployment topology by looking at which CPU/memory
metric families have actual data:

MICROSERVICE (Kubernetes / containerized):

- container_cpu_usage_seconds_total exists and has values
- container_memory_working_set_bytes exists and has values
- K8s labels (namespace, pod, container) likely present
- PromQL queries include "container!=""" filters

MONOLITH (VM / bare metal / Node Exporter):

- process_cpu_seconds_total exists and has values
- process_resident_memory_bytes exists and has values
- K8s labels not present
- PromQL queries do NOT include container filters

This is detected automatically by LabelDiscovery.discover() and baked into
the LabelSchema that all downstream code uses.

# ============================================================================

# QUERY RESOLUTION STRATEGY

# ============================================================================

For each metric (request_rate, error_rate, p95_latency, cpu_usage, memory_usage),
the adapter uses a 2-tier fallback:

Tier 1: Recording Rule (if it exists)

- Prometheus recording rules pre-compute expensive aggregations
- e.g., "test_compass:service:p95_latency_seconds"
- Supports explicit overrides via RecordingRuleResolver with config
- If rule exists but no data for this target → fallback to Tier 2

Tier 2: Direct PromQL

- Built dynamically by PromQLBuilder based on discovered schema
- Adapts automatically to available metric families
- e.g., Uses container_cpu_usage_seconds_total on K8s, process_cpu_seconds_total on VM
- Includes appropriate target matchers (service, namespace, etc.)

This ensures that even if recording rules are missing, metrics still work.

# ============================================================================

# EXAMPLE: LOCAL DOCKER COMPOSE (NODE EXPORTER)

# ============================================================================

Setup:

- Docker Compose with services: checkout-api, inventory-api, order-api
- Node Exporter container scraping host (PID 1 is docker-compose)
- Prometheus scraping /metrics endpoints + Node Exporter
- Services labeled: service=checkout-api, environment=local
- No K8s labels

What happens:

1. LabelDiscovery.discover() runs:
   - Queries /api/v1/label/service/values → finds ["checkout-api", "inventory-api", "order-api"]
   - Queries /api/v1/label/namespace/values → empty (K8s not running)
   - Checks for metric_exists("container_cpu_usage_seconds_total") → False (no container metrics)
   - Checks for metric_exists("process_cpu_seconds_total") → True (Node Exporter has these)

2. LabelSchema is built:
   - architecture: MONOLITH
   - http_group_label: "service"
   - process_group_label: "service"
   - cpu_metric: "process_cpu_seconds_total"
   - memory_metric: "process_resident_memory_bytes"
   - namespace_label: None
   - pod_label: None
   - container_label: None

3. PrometheusAdapter.query("checkout-api", "local", 300) builds PromQL:
   - For CPU: sum(rate(process_cpu_seconds_total{service="checkout-api"}[5m])) by (service)
   - For memory: sum(process_resident_memory_bytes{service="checkout-api"}) by (service)
   - Queries succeed, returns metrics

4. Layer 3 receives MetricsContext:
   - service: "checkout-api"
   - environment: "local"
   - cpu_usage: 0.15 cores
   - memory_usage: 256MB
   - namespace: None (not applicable)

# ============================================================================

# EXAMPLE: KUBERNETES DEPLOYMENT

# ============================================================================

Setup:

- Kubernetes cluster running Compass operator
- Services deployed as Deployments/StatefulSets
- cAdvisor/kubelet metrics collected by Prometheus
- Applications expose /metrics endpoints (Prometheus scrapes via ServiceMonitor)
- Metrics labeled: service=checkout-api, namespace=compass, pod=checkout-api-7f8c9d4k2

What happens:

1. LabelDiscovery.discover() runs:
   - Queries /api/v1/label/service/values → finds service labels
   - Queries /api/v1/label/namespace/values → finds K8s namespaces
   - Checks for metric_exists("container_cpu_usage_seconds_total") → True
   - Checks for metric_exists("process_cpu_seconds_total") → False (in K8s, only container metrics)

2. LabelSchema is built:
   - architecture: MICROSERVICE
   - http_group_label: "service"
   - process_group_label: "service"
   - cpu_metric: "container_cpu_usage_seconds_total"
   - memory_metric: "container_memory_working_set_bytes"
   - namespace_label: "namespace"
   - pod_label: "pod"
   - container_label: "container"

3. PrometheusAdapter.query("checkout-api", "prod", 300) builds PromQL:
   - For CPU: sum(rate(container_cpu_usage_seconds_total{container!="", service="checkout-api", namespace="prod"}[5m])) by (service)
   - For memory: sum(container_memory_working_set_bytes{container!="", service="checkout-api", namespace="prod"}) by (service)
   - Queries succeed, returns metrics + K8s context

4. Layer 3 receives MetricsContext:
   - service: "checkout-api"
   - environment: "prod"
   - cpu_usage: 2.5 cores
   - memory_usage: 1.2GB
   - namespace: "compass"
   - pod: "checkout-api-7f8c9d4k2"
   - container: "checkout-api"

The anomaly-detection model runs the exact same code in both cases, only the
values and optional K8s enrichment change.

# ============================================================================

# CONFIGURATION FOR DIFFERENT ENVIRONMENTS

# ============================================================================

LOCAL DOCKER COMPOSE (.env):

    PROMETHEUS_URL=http://localhost:9090
    LOKI_URL=http://localhost:3100

    # No need to set anything else; discovery is automatic
    # If you have recording rules, they'll be auto-discovered too

Kubernetes (values.yaml or ConfigMap):

    PROMETHEUS_URL=http://prometheus.compass.svc:9090
    LOKI_URL=http://loki.compass.svc:3100

    # Same code; discovery finds namespace, pod, container labels automatically
    # Record rules in your K8s cluster are auto-discovered

Bare Metal / VM with Node Exporter:

    PROMETHEUS_URL=http://prometheus.corp.internal:9090
    LOKI_URL=http://loki.corp.internal:3100

    # Discovery detects process_* metrics, same as local Docker Compose

All three use the exact same adapter code and ContextBuilder.

# ============================================================================

# EXTENDING TO KUBERNETES-SPECIFIC SIGNALS

# ============================================================================

Pod-level signals (CrashLoopBackOff, OOMKilled, pod restarts, rollout status)
come from the Kubernetes API, NOT Prometheus. These are handled by the
separate KubernetesAdapter (kubernetes_watch.py).

Design keeps these separate from generic metrics:

- PrometheusAdapter: Generic metrics (CPU, memory, latency, errors)
- KubernetesAdapter: K8s-specific events (pod status, rollout progress)
- LokiAdaptor: Logs

Layer 3 receives all three and correlates them for root-cause analysis.
A high memory usage (Prometheus) + OOMKilled event (K8s) → likely memory leak.

# ============================================================================

# MIGRATION PATH: LOCAL → KUBERNETES

# ============================================================================

1. Start with local Node Exporter + Docker Compose
   - Set PROMETHEUS_URL=http://localhost:9090 in .env
   - Discovery automatically finds process_* metrics
   - Everything works

2. Move to Kubernetes (no code changes needed)
   - Deploy Prometheus + Loki + KubernetesAdapter to cluster
   - Update .env: PROMETHEUS_URL=http://prometheus.compass.svc:9090
   - Discovery automatically finds container_* metrics + K8s labels
   - Same adapter code works; metrics just have K8s enrichment now

3. Optionally enable Kubernetes API signals
   - KubernetesAdapter starts watching pod events
   - Layer 3 gets more context (CrashLoopBackOff, OOMKilled, etc.)
   - No changes to PrometheusAdapter or ContextBuilder

# ============================================================================

# TESTING BOTH ENVIRONMENTS LOCALLY

# ============================================================================

To test against both Node Exporter and K8s-like metrics locally:

1. Run docker-compose.yml with Node Exporter:
   docker-compose up -d
   → Prometheus scrapes /metrics + Node Exporter
   → LabelDiscovery finds process_* metrics, MONOLITH architecture

2. For K8s simulation, either:
   a. Use Kind or minikube locally:
   kind create cluster
   helm install kube-state-metrics kube-state-metrics/kube-state-metrics
   helm install prometheus prometheus-community/prometheus
   → Prometheus scrapes container metrics + K8s labels

   b. Or mock K8s metrics in a separate Prometheus job:
   - Create process_cpu_seconds_total with different label names
   - Create container_cpu_usage_seconds_total with namespace/pod labels
   - Prometheus discovers container_* first → MICROSERVICE architecture

See examples_environment_flexibility.py for working code examples.

# ============================================================================

# SUMMARY

# ============================================================================

Key takeaway: Compass uses DISCOVERY, not CONFIGURATION, to adapt to
different environments. The same adapter code runs on:

✓ Local Node Exporter (process metrics)
✓ Docker Compose (process metrics)
✓ Kubernetes (container metrics + K8s labels)
✓ Bare metal / VM (Node Exporter)
✓ Hybrid deployments (mix of VM + K8s)

No separate adapters, no environment checks, no conditional logic.
Just dynamic discovery + parameterized PromQL.
"""
