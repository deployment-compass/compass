"""
Unit tests for the environment-flexible observability architecture.

Demonstrates PrometheusAdapter, ContextBuilder, and discovery working correctly
for both Node Exporter and Kubernetes environments.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

from compass.ingestion.adaptors.prometheous.prometheous import PrometheusAdapter
from compass.ingestion.adaptors.prometheous.prom_models import (
    ArchitectureMode,
    LabelSchema,
    MetricType,
    MetricSample,
    DataSource,
    MetricsContext,
)
from compass.ingestion.context_builder import ContextBuilder


class TestLabelDiscoveryNodeExporter:
    """Tests for Node Exporter environment discovery."""

    @pytest.mark.asyncio
    async def test_discover_node_exporter_environment(self):
        """
        Simulates a local Docker Compose environment with Node Exporter.
        Discovery should find:
        - process_cpu_seconds_total (not container_cpu_usage_seconds_total)
        - service label (not namespace/pod)
        → Architecture: MONOLITH
        """
        # Mock Prometheus responses
        client = AsyncMock()
        
        # Label queries
        async def mock_label_query(url, **kwargs):
            query = kwargs.get("params", {}).get("query", "")
            
            # Service label exists
            if "label/service/values" in url:
                response = MagicMock()
                response.json.return_value = {"data": ["checkout-api", "inventory-api"]}
                response.raise_for_status = MagicMock()
                return response
            
            # K8s labels don't exist
            if "label/namespace/values" in url or "label/pod/values" in url:
                response = MagicMock()
                response.json.return_value = {"data": []}
                response.raise_for_status = MagicMock()
                return response
            
            # Metric existence checks
            if 'count(container_cpu_usage_seconds_total)' in query:
                response = MagicMock()
                response.json.return_value = {"data": {"result": []}}  # Empty
                response.raise_for_status = MagicMock()
                return response
            
            if 'count(process_cpu_seconds_total)' in query:
                response = MagicMock()
                response.json.return_value = {"data": {"result": [{"value": [0, "1.5"]}]}}  # Has data
                response.raise_for_status = MagicMock()
                return response
        
        client.get = mock_label_query
        
        # Test discovery
        from compass.ingestion.adaptors.prometheous.prom_label_discovery import LabelDiscovery
        discovery = LabelDiscovery(client, "http://localhost:9090")
        schema = await discovery.discover()
        
        assert schema.architecture == ArchitectureMode.MONOLITH
        assert schema.http_group_label == "service"
        assert schema.cpu_metric == "process_cpu_seconds_total"
        assert schema.memory_metric == "process_resident_memory_bytes"
        assert schema.namespace_label is None  # K8s label not available


class TestLabelDiscoveryKubernetes:
    """Tests for Kubernetes environment discovery."""

    @pytest.mark.asyncio
    async def test_discover_kubernetes_environment(self):
        """
        Simulates a Kubernetes cluster environment.
        Discovery should find:
        - container_cpu_usage_seconds_total (not process_cpu_seconds_total)
        - namespace, pod, container labels (K8s-specific)
        → Architecture: MICROSERVICE
        """
        # Mock Prometheus responses for K8s
        client = AsyncMock()
        
        async def mock_label_query(url, **kwargs):
            query = kwargs.get("params", {}).get("query", "")
            
            # Service label exists
            if "label/service/values" in url:
                response = MagicMock()
                response.json.return_value = {"data": ["checkout-api"]}
                response.raise_for_status = MagicMock()
                return response
            
            # K8s labels exist
            if "label/namespace/values" in url:
                response = MagicMock()
                response.json.return_value = {"data": ["compass", "default"]}
                response.raise_for_status = MagicMock()
                return response
            
            if "label/pod/values" in url:
                response = MagicMock()
                response.json.return_value = {"data": ["checkout-api-7f8c9d4k2"]}
                response.raise_for_status = MagicMock()
                return response
            
            # Metric existence checks
            if 'count(container_cpu_usage_seconds_total)' in query:
                response = MagicMock()
                response.json.return_value = {"data": {"result": [{"value": [0, "10.5"]}]}}  # Has data
                response.raise_for_status = MagicMock()
                return response
            
            if 'count(process_cpu_seconds_total)' in query:
                response = MagicMock()
                response.json.return_value = {"data": {"result": []}}  # No data
                response.raise_for_status = MagicMock()
                return response
        
        client.get = mock_label_query
        
        # Test discovery
        from compass.ingestion.adaptors.prometheous.prom_label_discovery import LabelDiscovery
        discovery = LabelDiscovery(client, "http://prometheus.svc:9090")
        schema = await discovery.discover()
        
        assert schema.architecture == ArchitectureMode.MICROSERVICE
        assert schema.http_group_label == "service"
        assert schema.cpu_metric == "container_cpu_usage_seconds_total"
        assert schema.memory_metric == "container_memory_working_set_bytes"
        assert schema.namespace_label == "namespace"  # K8s label discovered
        assert schema.pod_label == "pod"


class TestPrometheusAdapterNodeExporter:
    """Tests for PrometheusAdapter with Node Exporter metrics."""

    @pytest.mark.asyncio
    async def test_query_local_environment(self):
        """
        Test that PrometheusAdapter.query() works correctly for Node Exporter.
        Should return metrics without K8s labels.
        """
        client = AsyncMock()
        
        # Mock the discovery response
        with patch.object(
            PrometheusAdapter, "_ensure_client"
        ) as mock_ensure:
            adapter = PrometheusAdapter("http://localhost:9090")
            
            # Setup mock schema
            mock_schema = LabelSchema(
                architecture=ArchitectureMode.MONOLITH,
                http_group_label="service",
                process_group_label="service",
                cpu_metric="process_cpu_seconds_total",
                memory_metric="process_resident_memory_bytes",
            )
            
            # Mock the query responses
            async def mock_query(url, params):
                query = params.get("query", "")
                response = MagicMock()
                
                # Return reasonable values for each metric type
                if "request_rate" in query or "http_requests_total" in query:
                    response.json.return_value = {
                        "data": {"result": [{"value": [0, "100.5"]}]}
                    }
                elif "error_rate" in query:
                    response.json.return_value = {
                        "data": {"result": [{"value": [0, "0.02"]}]}
                    }
                elif "p95" in query or "histogram_quantile" in query:
                    response.json.return_value = {
                        "data": {"result": [{"value": [0, "0.15"]}]}
                    }
                elif "cpu" in query:
                    response.json.return_value = {
                        "data": {"result": [{"value": [0, "0.5"]}]}
                    }
                elif "memory" in query:
                    response.json.return_value = {
                        "data": {"result": [{"value": [0, "256000000"]}]}
                    }
                else:
                    response.json.return_value = {"data": {"result": []}}
                
                response.raise_for_status = MagicMock()
                return response
            
            adapter._client = client
            adapter._discovery = AsyncMock()
            adapter._discovery.discover = AsyncMock(return_value=mock_schema)
            adapter._rule_resolver = AsyncMock()
            adapter._rule_resolver.resolve = AsyncMock(return_value=None)  # No recording rules
            
            client.get = mock_query
            
            # Call query
            result = await adapter.query("checkout-api", "local", 300)
            
            # Verify results
            assert result[MetricType.REQUEST_RATE.value] is not None
            assert result[MetricType.ERROR_RATE.value] is not None
            assert result[MetricType.P95_LATENCY.value] is not None
            assert result[MetricType.CPU_USAGE.value] is not None
            assert result[MetricType.MEMORY_USAGE.value] is not None


class TestMetricsContext:
    """Tests for the normalized MetricsContext data structure."""

    def test_metrics_context_creation_local(self):
        """MetricsContext for local environment (no K8s labels)."""
        context = MetricsContext(
            service="checkout-api",
            environment="local",
            request_rate=100.5,
            error_rate=0.02,
            p95_latency=0.15,
            cpu_usage=0.5,
            memory_usage=256000000,
            architecture=ArchitectureMode.MONOLITH,
        )
        
        assert context.service == "checkout-api"
        assert context.environment == "local"
        assert context.cpu_usage == 0.5
        assert context.namespace is None  # Not applicable
        assert context.pod is None
        assert context.architecture == ArchitectureMode.MONOLITH

    def test_metrics_context_creation_kubernetes(self):
        """MetricsContext for Kubernetes environment (with K8s labels)."""
        context = MetricsContext(
            service="checkout-api",
            environment="prod",
            request_rate=100.5,
            error_rate=0.02,
            p95_latency=0.15,
            cpu_usage=2.5,
            memory_usage=1200000000,
            namespace="compass",
            pod="checkout-api-7f8c9d4k2",
            container="checkout-api",
            architecture=ArchitectureMode.MICROSERVICE,
        )
        
        assert context.service == "checkout-api"
        assert context.environment == "prod"
        assert context.cpu_usage == 2.5
        assert context.namespace == "compass"  # K8s enrichment
        assert context.pod == "checkout-api-7f8c9d4k2"
        assert context.container == "checkout-api"
        assert context.architecture == ArchitectureMode.MICROSERVICE


class TestContextBuilder:
    """Tests for ContextBuilder orchestration."""

    @pytest.mark.asyncio
    async def test_context_builder_local(self):
        """ContextBuilder works correctly for local Node Exporter."""
        # Mock adapters
        prometheus_mock = AsyncMock(spec=PrometheusAdapter)
        prometheus_mock.query = AsyncMock(
            return_value={
                "request_rate": 100.5,
                "error_rate": 0.02,
                "p95_latency": 0.15,
                "cpu_usage": 0.5,
                "memory_usage": 256000000,
            }
        )
        prometheus_mock.get_schema = AsyncMock(
            return_value=LabelSchema(
                architecture=ArchitectureMode.MONOLITH,
                http_group_label="service",
                process_group_label="service",
                cpu_metric="process_cpu_seconds_total",
                memory_metric="process_resident_memory_bytes",
            )
        )
        
        loki_mock = AsyncMock()
        loki_mock.query = AsyncMock(
            return_value={"lines": ["error: timeout", "error: 503 service unavailable"]}
        )
        
        builder = ContextBuilder(prometheus_mock, loki_mock)
        result = await builder.build("checkout-api", "local", 300)
        
        assert result.metrics.service == "checkout-api"
        assert result.metrics.environment == "local"
        assert result.metrics.cpu_usage == 0.5
        assert result.metrics.namespace is None
        assert len(result.log_lines) == 2

    @pytest.mark.asyncio
    async def test_context_builder_kubernetes(self):
        """ContextBuilder works correctly for Kubernetes (same code, different data)."""
        # Mock adapters
        prometheus_mock = AsyncMock(spec=PrometheusAdapter)
        prometheus_mock.query = AsyncMock(
            return_value={
                "request_rate": 150.0,
                "error_rate": 0.01,
                "p95_latency": 0.25,
                "cpu_usage": 2.5,
                "memory_usage": 1200000000,
            }
        )
        prometheus_mock.get_schema = AsyncMock(
            return_value=LabelSchema(
                architecture=ArchitectureMode.MICROSERVICE,
                http_group_label="service",
                process_group_label="service",
                cpu_metric="container_cpu_usage_seconds_total",
                memory_metric="container_memory_working_set_bytes",
                namespace_label="namespace",
                pod_label="pod",
                container_label="container",
            )
        )
        
        loki_mock = AsyncMock()
        loki_mock.query = AsyncMock(return_value={"lines": []})
        
        builder = ContextBuilder(prometheus_mock, loki_mock)
        result = await builder.build("checkout-api", "prod", 300)
        
        assert result.metrics.service == "checkout-api"
        assert result.metrics.environment == "prod"
        assert result.metrics.cpu_usage == 2.5
        assert result.metrics.architecture == ArchitectureMode.MICROSERVICE
        # Note: pod/namespace not populated here (would need collect() for that)
        # but schema indicates K8s context is available


if __name__ == "__main__":
    # Run with: pytest compass/test_observability_architecture.py -v
    pytest.main([__file__, "-v"])
