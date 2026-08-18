"""
Example usage of PrometheusAdapter and ContextBuilder in different environments.

Shows how the same code works seamlessly for:
1. Local Docker Compose with Node Exporter (process metrics)
2. Kubernetes cluster (container metrics + K8s labels)
3. Bare metal / VM (Node Exporter)

The adapter auto-discovers the environment and adapts queries accordingly.
"""

import asyncio
from compass.ingestion.adaptors.prometheous.prometheous import PrometheusAdapter
from compass.ingestion.adaptors.loki import LokiAdaptor
from compass.ingestion.context_builder import ContextBuilder


# ============================================================================
# SCENARIO 1: Local Docker Compose with Node Exporter
# ============================================================================

async def example_local_node_exporter():
    """
    Setup:
    - Docker Compose running services (checkout-api, inventory-api, etc.)
    - Node Exporter scraping host metrics (process_cpu_seconds_total, process_resident_memory_bytes)
    - Prometheus scraping /metrics endpoints + Node Exporter
    - No K8s labels (docker-compose.yml uses service names only)
    
    Expected behavior:
    - Discovery finds: process_* metrics (not container_*) → MONOLITH architecture
    - Labels: service=checkout-api, environment=local (if set)
    - K8s labels: None (not available in Docker Compose)
    """
    
    adapter = PrometheusAdapter(
        base_url="http://localhost:9090",  # Local Prometheus
        schema_cache_ttl_seconds=300,
    )
    
    # Check what discovery found
    schema = await adapter.get_schema()
    print("=== Local Node Exporter Discovery ===")
    print(f"Architecture: {schema.architecture}")  # Should be MONOLITH
    print(f"HTTP grouping label: {schema.http_group_label}")
    print(f"Process grouping label: {schema.process_group_label}")
    print(f"CPU metric: {schema.cpu_metric}")  # Should be process_cpu_seconds_total
    print(f"Memory metric: {schema.memory_metric}")  # Should be process_resident_memory_bytes
    print(f"Environment label: {schema.environment_label}")
    print(f"K8s namespace label: {schema.namespace_label}")  # Should be None
    print(f"K8s pod label: {schema.pod_label}")  # Should be None
    print(f"K8s container label: {schema.container_label}")  # Should be None
    print()
    
    # Query metrics for a single service
    metrics = await adapter.query(
        service="checkout-api",
        environment="local",
        window_seconds=300,  # 5 min window
    )
    print("=== Metrics for checkout-api (Local) ===")
    print(f"Request rate: {metrics.get('request_rate')}")
    print(f"Error rate: {metrics.get('error_rate')}")
    print(f"P95 latency: {metrics.get('p95_latency')}")
    print(f"CPU usage: {metrics.get('cpu_usage')}")
    print(f"Memory usage: {metrics.get('memory_usage')}")
    print()
    
    # Collect all services at once
    result = await adapter.collect(window="5m")
    print(f"=== Fleet-wide Collection (Local) ===")
    print(f"Collected {len(result.samples)} metric samples")
    print(f"Errors: {result.errors}")
    
    # Normalize into the model-friendly format
    normalized = result.to_normalized_dict()
    for target, metrics_dict in normalized.items():
        print(f"  {target}: {metrics_dict}")
    print()
    
    await adapter.aclose()


# ============================================================================
# SCENARIO 2: Kubernetes cluster
# ============================================================================

async def example_kubernetes():
    """
    Setup:
    - Kubernetes cluster running Compass Operator
    - cAdvisor/kubelet scraping container metrics (container_cpu_usage_seconds_total, container_memory_working_set_bytes)
    - Prometheus scraping container metrics + K8s labels (namespace, pod, container)
    - Applications expose /metrics endpoints (scraped via service labels)
    
    Expected behavior:
    - Discovery finds: container_* metrics → MICROSERVICE architecture
    - Labels: service=checkout-api, namespace=compass, pod=checkout-api-7f8c9d4k2, container=checkout-api
    - K8s labels: automatically discovered and attached to each metric sample
    """
    
    adapter = PrometheusAdapter(
        base_url="http://prometheus.compass.svc:9090",  # In-cluster Prometheus
        schema_cache_ttl_seconds=300,
    )
    
    # Check what discovery found
    schema = await adapter.get_schema()
    print("=== Kubernetes Discovery ===")
    print(f"Architecture: {schema.architecture}")  # Should be MICROSERVICE
    print(f"HTTP grouping label: {schema.http_group_label}")
    print(f"Process grouping label: {schema.process_group_label}")
    print(f"CPU metric: {schema.cpu_metric}")  # Should be container_cpu_usage_seconds_total
    print(f"Memory metric: {schema.memory_metric}")  # Should be container_memory_working_set_bytes
    print(f"Environment label: {schema.environment_label}")
    print(f"K8s namespace label: {schema.namespace_label}")  # Should be "namespace"
    print(f"K8s pod label: {schema.pod_label}")  # Should be "pod"
    print(f"K8s container label: {schema.container_label}")  # Should be "container"
    print()
    
    # Query metrics for a single service in a specific namespace
    metrics = await adapter.query(
        service="checkout-api",
        environment="prod",  # Could also use namespace if environment_label == namespace_label
        window_seconds=300,
    )
    print("=== Metrics for checkout-api (K8s) ===")
    print(f"Request rate: {metrics.get('request_rate')}")
    print(f"Error rate: {metrics.get('error_rate')}")
    print(f"P95 latency: {metrics.get('p95_latency')}")
    print(f"CPU usage: {metrics.get('cpu_usage')}")
    print(f"Memory usage: {metrics.get('memory_usage')}")
    print()
    
    # Collect all services and get K8s context
    result = await adapter.collect(window="5m")
    print(f"=== Fleet-wide Collection (K8s) ===")
    print(f"Collected {len(result.samples)} metric samples with K8s context")
    
    # Show samples with K8s enrichment
    for sample in result.samples[:5]:  # Show first 5 samples
        print(f"  {sample.target} ({sample.metric.value}): {sample.value}")
        if sample.namespace or sample.pod or sample.container:
            print(f"    → namespace={sample.namespace}, pod={sample.pod}, container={sample.container}")
    print()
    
    await adapter.aclose()


# ============================================================================
# SCENARIO 3: Using ContextBuilder (works for all environments)
# ============================================================================

async def example_context_builder_local():
    """
    ContextBuilder orchestrates metrics + logs for Layer 3 (AI reasoning).
    Same interface works for all environments — no branching needed.
    """
    
    prometheus = PrometheusAdapter(base_url="http://localhost:9090")
    loki = LokiAdaptor(base_url="http://localhost:3100")
    
    builder = ContextBuilder(prometheus, loki)
    
    # Build complete context for Layer 3
    result = await builder.build(
        service="checkout-api",
        environment="local",
        window_seconds=300,
    )
    
    print("=== Context for Layer 3 (Local) ===")
    print(f"Service: {result.metrics.service}")
    print(f"Environment: {result.metrics.environment}")
    print(f"Architecture: {result.metrics.architecture}")
    print()
    print("Metrics:")
    print(f"  Request rate: {result.metrics.request_rate}")
    print(f"  Error rate: {result.metrics.error_rate}")
    print(f"  P95 latency: {result.metrics.p95_latency}")
    print(f"  CPU usage: {result.metrics.cpu_usage}")
    print(f"  Memory usage: {result.metrics.memory_usage}")
    print()
    print(f"Log lines (sample): {result.log_lines[:3]}")
    print()
    
    await prometheus.aclose()


async def example_context_builder_k8s():
    """
    Same ContextBuilder, same code, different environment.
    The adapter auto-adapts to K8s without any code changes.
    """
    
    prometheus = PrometheusAdapter(base_url="http://prometheus.compass.svc:9090")
    loki = LokiAdaptor(base_url="http://loki.compass.svc:3100")
    
    builder = ContextBuilder(prometheus, loki)
    
    # Exact same call works in K8s too
    result = await builder.build(
        service="checkout-api",
        environment="prod",
        window_seconds=300,
    )
    
    print("=== Context for Layer 3 (K8s) ===")
    print(f"Service: {result.metrics.service}")
    print(f"Environment: {result.metrics.environment}")
    print(f"Architecture: {result.metrics.architecture}")
    print(f"K8s enrichment: namespace={result.metrics.namespace}, pod={result.metrics.pod}")
    print()
    print("Metrics:")
    print(f"  Request rate: {result.metrics.request_rate}")
    print(f"  Error rate: {result.metrics.error_rate}")
    print(f"  P95 latency: {result.metrics.p95_latency}")
    print(f"  CPU usage: {result.metrics.cpu_usage}")
    print(f"  Memory usage: {result.metrics.memory_usage}")
    print()
    
    await prometheus.aclose()


async def main():
    """Run all examples (modify to run only one if needed)."""
    print("=" * 70)
    print("COMPASS OBSERVABILITY LAYER - ENVIRONMENT FLEXIBILITY EXAMPLES")
    print("=" * 70)
    print()
    
    # NOTE: Uncomment examples as needed for your environment
    # await example_local_node_exporter()
    # await example_kubernetes()
    # await example_context_builder_local()
    # await example_context_builder_k8s()
    
    print("Examples ready — uncomment and run for your environment.")


if __name__ == "__main__":
    asyncio.run(main())
