#!/usr/bin/env python3
"""
Metrics Cache - Background metrics cache manager

This module provides a metrics cache that is periodically updated by a
background thread. HTTP requests serve pre-generated, compressed metrics
for improved performance.
"""

import time
import gzip
import threading
import logging
from io import BytesIO
from typing import Optional, Tuple
from prometheus_client import CONTENT_TYPE_LATEST


class MetricsCache:
    """
    Background metrics cache with automatic compression.
    
    This class manages a cache of Prometheus metrics that is periodically
    updated by a background thread. It supports gzip compression for
    efficient network transfer.
    """
    
    def __init__(self, result_table, update_interval: float = 10.0, compress_level: int = 6):
        """
        Initialize the metrics cache.
        
        Args:
            result_table: ResultTable instance to generate metrics from
            update_interval: How often to update the cache (seconds)
            compress_level: Gzip compression level (1-9, higher = better compression)
        """
        self.result_table = result_table
        self.update_interval = update_interval
        self.compress_level = compress_level
        
        # Cache storage
        self._raw_data: Optional[bytes] = None
        self._compressed_data: Optional[bytes] = None
        self._timestamp: float = 0
        self._lock = threading.Lock()
        
        # Background thread
        self._running = False
        self._update_thread: Optional[threading.Thread] = None
        
        self.logger = logging.getLogger(__name__)
    
    def start(self):
        """Start the background cache updater thread"""
        if self._running:
            self.logger.warning("MetricsCache already running")
            return
        
        self._running = True
        self._update_thread = threading.Thread(
            target=self._update_loop,
            daemon=True,
            name='metrics-cache-updater'
        )
        self._update_thread.start()
        self.logger.info(f"MetricsCache started (update_interval={self.update_interval}s)")
    
    def stop(self):
        """Stop the background cache updater thread"""
        self._running = False
        if self._update_thread:
            self._update_thread.join(timeout=5)
        self.logger.info("MetricsCache stopped")
    
    def _update_loop(self):
        """Background loop to update metrics cache"""
        self.logger.info("MetricsCache updater thread started")
        
        # Initial update
        try:
            self._update_cache()
        except Exception as e:
            self.logger.error(f"Error in initial metrics cache update: {e}", exc_info=True)
        
        # Periodic updates
        while self._running:
            try:
                time.sleep(self.update_interval)
                self._update_cache()
            except Exception as e:
                self.logger.error(f"Error updating metrics cache: {e}", exc_info=True)
    
    def _update_cache(self):
        """Update the metrics cache with fresh data"""
        # Generate metrics from result table
        raw_data = self.result_table.generate_metrics()
        
        # Compress data
        compressed_buffer = BytesIO()
        with gzip.GzipFile(fileobj=compressed_buffer, mode='wb', compresslevel=self.compress_level) as f:
            f.write(raw_data)
        compressed_data = compressed_buffer.getvalue()
        
        # Update cache atomically
        with self._lock:
            self._raw_data = raw_data
            self._compressed_data = compressed_data
            self._timestamp = time.time()
        
        # Log statistics
        if len(raw_data) > 0:
            ratio = 100 * len(compressed_data) / len(raw_data)
            self.logger.debug(
                f"Cache updated: {len(raw_data)} bytes raw, "
                f"{len(compressed_data)} bytes compressed ({ratio:.1f}%)"
            )
        else:
            self.logger.debug("Cache updated: empty metrics")
    
    def get_metrics(self, accept_gzip: bool = False) -> Tuple[bytes, dict]:
        """
        Get cached metrics data.
        
        Args:
            accept_gzip: Whether to return gzip-compressed data
        
        Returns:
            tuple: (data, headers) where headers is a dict of HTTP headers
        """
        with self._lock:
            # Check if cache is ready
            if self._raw_data is None:
                return None, None
            
            if accept_gzip and self._compressed_data:
                return self._compressed_data, {
                    'Content-Type': CONTENT_TYPE_LATEST,
                    'Content-Encoding': 'gzip',
                    'Content-Length': str(len(self._compressed_data))
                }
            else:
                return self._raw_data, {
                    'Content-Type': CONTENT_TYPE_LATEST,
                    'Content-Length': str(len(self._raw_data))
                }
    
    def is_ready(self) -> bool:
        """Check if cache has been initialized with data"""
        with self._lock:
            return self._raw_data is not None
    
    def get_stats(self) -> dict:
        """Get cache statistics"""
        with self._lock:
            return {
                'ready': self._raw_data is not None,
                'raw_size': len(self._raw_data) if self._raw_data else 0,
                'compressed_size': len(self._compressed_data) if self._compressed_data else 0,
                'last_update': self._timestamp,
                'age': time.time() - self._timestamp if self._timestamp > 0 else None
            }
    
    def __repr__(self):
        stats = self.get_stats()
        return (f"MetricsCache(ready={stats['ready']}, "
                f"raw_size={stats['raw_size']}, "
                f"compressed_size={stats['compressed_size']})")
