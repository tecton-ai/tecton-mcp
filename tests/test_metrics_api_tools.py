"""
Unit tests for metrics API tools.
"""

import os
import pytest
from unittest.mock import patch, MagicMock
from tecton_mcp.tools.metrics_api_tools import (
    parse_openmetrics_to_readable,
    query_tecton_metrics,
    get_tecton_cluster_url
)


class TestParseOpenMetricsToReadable:
    """Test the OpenMetrics parsing functionality."""
    
    def test_parse_simple_metrics(self):
        """Test parsing simple metrics without labels."""
        metrics_text = """
# HELP tecton_feature_store_requests_total Total number of feature store requests
# TYPE tecton_feature_store_requests_total counter
tecton_feature_store_requests_total 1234
tecton_materialization_jobs_running 5
"""
        result = parse_openmetrics_to_readable(metrics_text)
        
        assert "Current Tecton Metrics:" in result
        assert "tecton_feature_store_requests_total: 1234.0" in result
        assert "tecton_materialization_jobs_running: 5.0" in result
    
    def test_parse_metrics_with_labels(self):
        """Test parsing metrics with labels."""
        metrics_text = """
# HELP tecton_feature_store_latency_seconds Feature store latency
# TYPE tecton_feature_store_latency_seconds histogram
tecton_feature_store_latency_seconds{quantile="0.5",service="online"} 0.045
tecton_feature_store_latency_seconds{quantile="0.95",service="online"} 0.123
"""
        result = parse_openmetrics_to_readable(metrics_text)
        
        assert "tecton_feature_store_latency_seconds: 0.045" in result
        assert "quantile=0.5, service=online" in result
        assert "tecton_feature_store_latency_seconds: 0.123" in result
        assert "quantile=0.95, service=online" in result
    
    def test_parse_metrics_always_includes_labels(self):
        """Test that metrics always include labels when present."""
        metrics_text = """
# HELP tecton_feature_store_latency_seconds Feature store latency
# TYPE tecton_feature_store_latency_seconds histogram
tecton_feature_store_latency_seconds{quantile="0.5",service="online"} 0.045
"""
        result = parse_openmetrics_to_readable(metrics_text)
        
        assert "tecton_feature_store_latency_seconds: 0.045" in result
        assert "quantile=0.5, service=online" in result
    
    def test_filter_metrics(self):
        """Test filtering metrics with regex pattern."""
        metrics_text = """
# HELP tecton_feature_store_requests_total Total requests
# TYPE tecton_feature_store_requests_total counter
tecton_feature_store_requests_total 1234
# HELP tecton_materialization_jobs_running Running jobs
# TYPE tecton_materialization_jobs_running gauge
tecton_materialization_jobs_running 5
# HELP tecton_feature_store_latency_seconds Latency
# TYPE tecton_feature_store_latency_seconds histogram
tecton_feature_store_latency_seconds 0.045
"""
        result = parse_openmetrics_to_readable(metrics_text, metric_filter="tecton_feature_store.*")
        
        assert "tecton_feature_store_requests_total: 1234.0" in result
        assert "tecton_feature_store_latency_seconds: 0.045" in result
        assert "tecton_materialization_jobs_running" not in result
    
    def test_no_metrics_found(self):
        """Test handling when no metrics match the filter."""
        metrics_text = """
# HELP tecton_feature_store_requests_total Total requests
# TYPE tecton_feature_store_requests_total counter
tecton_feature_store_requests_total 1234
"""
        result = parse_openmetrics_to_readable(metrics_text, metric_filter="nonexistent.*")
        
        assert "No metrics found matching the filter criteria." in result


class TestQueryTectonMetrics:
    """Test the metrics querying functionality."""
    
    @patch.dict(os.environ, {}, clear=True)
    def test_no_api_key(self):
        """Test behavior when TECTON_API_KEY is not set."""
        result = query_tecton_metrics()
        assert "Error: TECTON_API_KEY environment variable not set" in result
    
    @patch.dict(os.environ, {"TECTON_API_KEY": "test-key"})
    @patch('tecton_mcp.tools.metrics_api_tools.get_tecton_cluster_url')
    @patch('requests.get')
    def test_successful_query(self, mock_get, mock_cluster_url):
        """Test successful metrics query."""
        mock_cluster_url.return_value = "https://test.tecton.ai"
        
        mock_response = MagicMock()
        mock_response.text = """
# HELP tecton_feature_store_requests_total Total requests
# TYPE tecton_feature_store_requests_total counter
tecton_feature_store_requests_total 1234
# HELP tecton_materialization_jobs_running Running jobs
# TYPE tecton_materialization_jobs_running gauge
tecton_materialization_jobs_running 5
"""
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = query_tecton_metrics()
        
        assert "Current Tecton Metrics:" in result
        assert "tecton_feature_store_requests_total: 1234.0" in result
        assert "tecton_materialization_jobs_running: 5.0" in result
        
        # Verify the request was made correctly
        mock_get.assert_called_once_with(
            "https://test.tecton.ai/api/v1/observability/metrics",
            headers={"Authorization": "Tecton-key test-key"},
            timeout=30
        )
    
    @patch.dict(os.environ, {"TECTON_API_KEY": "test-key"})
    @patch('tecton_mcp.tools.metrics_api_tools.get_tecton_cluster_url')
    @patch('requests.get')
    def test_raw_format(self, mock_get, mock_cluster_url):
        """Test querying with raw format."""
        mock_cluster_url.return_value = "https://test.tecton.ai"
        
        mock_response = MagicMock()
        mock_response.text = "raw_metrics_data"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = query_tecton_metrics(output_format="raw")
        
        assert result == "raw_metrics_data"
    
    @patch.dict(os.environ, {"TECTON_API_KEY": "test-key"})
    @patch('tecton_mcp.tools.metrics_api_tools.get_tecton_cluster_url')
    @patch('requests.get')
    def test_authentication_error(self, mock_get, mock_cluster_url):
        """Test handling of authentication errors."""
        mock_cluster_url.return_value = "https://test.tecton.ai"
        
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        
        mock_get.return_value = mock_response
        mock_get.side_effect = requests.exceptions.HTTPError(response=mock_response)
        
        result = query_tecton_metrics()
        
        assert "Error: Authentication failed" in result
    
    @patch.dict(os.environ, {"TECTON_API_KEY": "test-key"})
    @patch('tecton_mcp.tools.metrics_api_tools.get_tecton_cluster_url')
    @patch('requests.get')
    def test_permission_error(self, mock_get, mock_cluster_url):
        """Test handling of permission errors."""
        mock_cluster_url.return_value = "https://test.tecton.ai"
        
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.text = "Forbidden"
        
        mock_get.return_value = mock_response
        mock_get.side_effect = requests.exceptions.HTTPError(response=mock_response)
        
        result = query_tecton_metrics()
        
        assert "Error: Access denied" in result


class TestGetTectonClusterUrl:
    """Test the cluster URL retrieval functionality."""
    
    @patch('tecton_mcp.tools.metrics_api_tools.cluster_url')
    def test_get_cluster_url(self, mock_cluster_url):
        """Test getting the cluster URL."""
        mock_cluster_url.return_value = "https://test.tecton.ai"
        
        result = get_tecton_cluster_url()
        
        assert result == "https://test.tecton.ai"
        mock_cluster_url.assert_called_once()
