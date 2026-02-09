#!/usr/bin/env python3
"""
Prometheus Metrics Wrapper

Default label을 자동으로 적용하는 Prometheus metrics wrapper 클래스들을 제공합니다.
"""

from typing import Dict, Any, Optional, List, Union
from prometheus_client import Counter as PrometheusCounter, Gauge as PrometheusGauge, Histogram as PrometheusHistogram, Info as PrometheusInfo
from prometheus_client.core import CollectorRegistry


class BaseMetricWrapper:
    """Base class for metric wrappers with default labels"""
    
    def __init__(self, metric_instance, default_labels: Dict[str, str] = None):
        """
        Initialize the wrapper
        
        Args:
            metric_instance: Prometheus metric instance
            default_labels: Default labels to apply to all metric operations
        """
        self._metric = metric_instance
        self._default_labels = default_labels or {}
    
    def _merge_labels(self, additional_labels: Dict[str, str] = None) -> Dict[str, str]:
        """Merge default labels with additional labels"""
        labels = self._default_labels.copy()
        if additional_labels:
            labels.update(additional_labels)
        return labels
    
    def labels(self, **labels):
        """Return labeled metric with default labels merged"""
        merged_labels = self._merge_labels(labels)
        return self._metric.labels(**merged_labels)


class CounterWrapper(BaseMetricWrapper):
    """Wrapper for Prometheus Counter with default labels"""
    
    def inc(self, amount: float = 1, **labels):
        """Increment counter with default labels"""
        self.labels(**labels).inc(amount)
    
    def set(self, value: float, **labels):
        """Set counter value directly (for internal use)"""
        labeled_metric = self.labels(**labels)
        labeled_metric._value._value = value
    
    def get(self, **labels) -> float:
        """Get current counter value"""
        return self.labels(**labels)._value._value


class GaugeWrapper(BaseMetricWrapper):
    """Wrapper for Prometheus Gauge with default labels"""
    
    def set(self, value: float, **labels):
        """Set gauge value with default labels"""
        self.labels(**labels).set(value)
    
    def inc(self, amount: float = 1, **labels):
        """Increment gauge with default labels"""
        self.labels(**labels).inc(amount)
    
    def dec(self, amount: float = 1, **labels):
        """Decrement gauge with default labels"""
        self.labels(**labels).dec(amount)
    
    def set_to_current_time(self, **labels):
        """Set gauge to current Unix timestamp"""
        self.labels(**labels).set_to_current_time()
    
    def track_inprogress(self, **labels):
        """Track function in progress (decorator)"""
        return self.labels(**labels).track_inprogress()
    
    def time(self, **labels):
        """Time function execution (decorator)"""
        return self.labels(**labels).time()
    
    def get(self, **labels) -> float:
        """Get current gauge value"""
        return self.labels(**labels)._value._value


class HistogramWrapper(BaseMetricWrapper):
    """Wrapper for Prometheus Histogram with default labels"""
    
    def observe(self, amount: float, **labels):
        """Record observation with default labels"""
        self.labels(**labels).observe(amount)
    
    def time(self, **labels):
        """Time function execution (decorator)"""
        return self.labels(**labels).time()


class InfoWrapper(BaseMetricWrapper):
    """Wrapper for Prometheus Info with default labels"""
    
    def info(self, info_dict: Dict[str, str], **labels):
        """Set info with default labels"""
        self.labels(**labels).info(info_dict)


class MetricFactory:
    """Factory class to create metrics with default labels"""
    
    def __init__(self, default_labels: Dict[str, str] = None, registry: CollectorRegistry = None):
        """
        Initialize metric factory
        
        Args:
            default_labels: Default labels to apply to all metrics
            registry: Prometheus registry to use
        """
        self.default_labels = default_labels or {}
        self.registry = registry
    
    def counter(self, name: str, documentation: str, labelnames: List[str] = None) -> CounterWrapper:
        """Create a Counter with default labels"""
        labelnames = labelnames or []
        # Merge default label names with additional label names
        all_labelnames = list(self.default_labels.keys()) + labelnames
        
        metric = PrometheusCounter(
            name=name,
            documentation=documentation,
            labelnames=all_labelnames,
            registry=self.registry
        )
        
        return CounterWrapper(metric, self.default_labels)
    
    def gauge(self, name: str, documentation: str, labelnames: List[str] = None) -> GaugeWrapper:
        """Create a Gauge with default labels"""
        labelnames = labelnames or []
        all_labelnames = list(self.default_labels.keys()) + labelnames
        
        metric = PrometheusGauge(
            name=name,
            documentation=documentation,
            labelnames=all_labelnames,
            registry=self.registry
        )
        
        return GaugeWrapper(metric, self.default_labels)
    
    def histogram(self, name: str, documentation: str, labelnames: List[str] = None, 
                  buckets: List[float] = None) -> HistogramWrapper:
        """Create a Histogram with default labels"""
        labelnames = labelnames or []
        all_labelnames = list(self.default_labels.keys()) + labelnames
        
        kwargs = {
            'name': name,
            'documentation': documentation,
            'labelnames': all_labelnames,
            'registry': self.registry
        }
        
        if buckets is not None:
            kwargs['buckets'] = buckets
        
        metric = PrometheusHistogram(**kwargs)
        
        return HistogramWrapper(metric, self.default_labels)
    
    def info(self, name: str, documentation: str, labelnames: List[str] = None) -> InfoWrapper:
        """Create an Info metric with default labels"""
        labelnames = labelnames or []
        all_labelnames = list(self.default_labels.keys()) + labelnames
        
        metric = PrometheusInfo(
            name=name,
            documentation=documentation,
            labelnames=all_labelnames,
            registry=self.registry
        )
        
        return InfoWrapper(metric, self.default_labels)
