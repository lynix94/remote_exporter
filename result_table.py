#!/usr/bin/env python3
"""
Result Table - Shared metrics collection result storage

This module provides a centralized result table that multiple exporters
can update asynchronously. It wraps Prometheus CollectorRegistry with
a cleaner interface for metric collection management.
"""

import threading
import logging
from typing import Dict, Any, Optional
from prometheus_client.core import CollectorRegistry
from prometheus_client import generate_latest


class ResultTable:
    """
    Shared result table for multiple exporters to update metrics asynchronously.
    
    This class wraps a Prometheus CollectorRegistry and provides thread-safe
    access for multiple exporters to register and update metrics concurrently.
    """
    
    def __init__(self):
        """Initialize the result table with a shared registry"""
        self.registry = CollectorRegistry()
        self._lock = threading.Lock()
        self.logger = logging.getLogger(__name__)
        self.logger.info("ResultTable initialized with shared registry")
    
    def get_registry(self) -> CollectorRegistry:
        """
        Get the underlying Prometheus registry.
        
        Returns:
            CollectorRegistry: Shared Prometheus registry
        """
        return self.registry
    
    def generate_metrics(self) -> bytes:
        """
        Generate Prometheus-formatted metrics from the registry.
        
        This method is thread-safe and can be called concurrently.
        
        Returns:
            bytes: Prometheus-formatted metrics
        """
        with self._lock:
            return generate_latest(self.registry)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the result table.
        
        Returns:
            dict: Statistics including number of registered collectors
        """
        with self._lock:
            collectors = list(self.registry._collector_to_names.keys()) if hasattr(self.registry, '_collector_to_names') else []
            return {
                'num_collectors': len(collectors),
                'registry_id': id(self.registry)
            }
    
    def __repr__(self):
        stats = self.get_stats()
        return f"ResultTable(collectors={stats['num_collectors']}, registry_id={stats['registry_id']})"
