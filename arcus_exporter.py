#!/usr/bin/env python3
"""
Arcus Prometheus Exporter

Extends Memcached exporter with Arcus-specific collection metrics.
Supports ZooKeeper-based service discovery.
"""

import time
import logging
import re
import threading
import asyncio
import sys
import argparse
from typing import Dict, Any, List

# Import base memcached exporter
from memcached_exporter import MemcachedClient, MemcachedPrometheusExporter

# ZooKeeper import
try:
    from kazoo.client import KazooClient
    ZOOKEEPER_AVAILABLE = True
except ImportError:
    ZOOKEEPER_AVAILABLE = False


class ArcusPrometheusExporter(MemcachedPrometheusExporter):
    """Arcus exporter extending Memcached exporter with Arcus-specific metrics and ZooKeeper support"""
    
    def __init__(self, zookeeper_addr: str, cloud_name: str = '.*', 
                 exporter_port: int = 9150, collect_interval: float = 3.0,
                 default_labels: Dict[str, str] = None,
                 max_concurrent: int = 10,
                 enable_node_metrics: bool = True,
                 exclude_k8s_node: bool = True,
                 ssh_username: str = None,
                 ssh_key_file: str = None,
                 ssh_port: int = 22):
        
        # ZooKeeper settings
        self.zookeeper_addr = zookeeper_addr
        self.cloud_name = cloud_name
        self.zk_client = None
        self.cloud_instance_map = {}  # Maps instance address to cloud name
        self._cloud_map_lock = threading.Lock()
        
        # Initialize base class with empty list (will be populated by ZooKeeper)
        super().__init__(
            memcached_addrs=[],
            exporter_port=exporter_port,
            collect_interval=collect_interval,
            default_labels=default_labels,
            max_concurrent=max_concurrent,
            enable_node_metrics=enable_node_metrics,
            exclude_k8s_node=exclude_k8s_node,
            ssh_username=ssh_username,
            ssh_key_file=ssh_key_file,
            ssh_port=ssh_port,
            use_cloud_label=True,
            zookeeper_addr=zookeeper_addr
        )
        
        # Setup ZooKeeper watcher
        if self.zookeeper_addr and self.cloud_name:
            self._setup_zookeeper_watcher()
    
    def _init_metrics(self):
        """Initialize Prometheus metrics including Arcus-specific ones"""
        # Call parent to initialize base memcached metrics
        super()._init_metrics()
        
        # Add Arcus-specific metrics
        try:
            # Arcus Collection Commands
            self.arcus_lop_commands_total = self.metric_factory.gauge(
                'arcus_lop_commands_total',
                'Total number of LOP commands',
                labelnames=['operation', 'address', 'cloud', 'zookeeper']
            )
            
            self.arcus_sop_commands_total = self.metric_factory.gauge(
                'arcus_sop_commands_total',
                'Total number of SOP commands',
                labelnames=['operation', 'address', 'cloud', 'zookeeper']
            )
            
            self.arcus_bop_commands_total = self.metric_factory.gauge(
                'arcus_bop_commands_total',
                'Total number of BOP commands',
                labelnames=['operation', 'address', 'cloud', 'zookeeper']
            )
            
            self.arcus_mop_commands_total = self.metric_factory.gauge(
                'arcus_mop_commands_total',
                'Total number of MOP commands',
                labelnames=['operation', 'address', 'cloud', 'zookeeper']
            )
            
            # Arcus Collection Hit/Miss metrics
            self.arcus_lop_hits_total = self.metric_factory.gauge(
                'arcus_lop_hits_total',
                'Total number of LOP hits',
                labelnames=['operation', 'address', 'cloud', 'zookeeper']
            )
            
            self.arcus_lop_misses_total = self.metric_factory.gauge(
                'arcus_lop_misses_total',
                'Total number of LOP misses',
                labelnames=['operation', 'address', 'cloud', 'zookeeper']
            )
            
            self.arcus_sop_hits_total = self.metric_factory.gauge(
                'arcus_sop_hits_total',
                'Total number of SOP hits',
                labelnames=['operation', 'address', 'cloud', 'zookeeper']
            )
            
            self.arcus_sop_misses_total = self.metric_factory.gauge(
                'arcus_sop_misses_total',
                'Total number of SOP misses',
                labelnames=['operation', 'address', 'cloud', 'zookeeper']
            )
            
            self.arcus_bop_hits_total = self.metric_factory.gauge(
                'arcus_bop_hits_total',
                'Total number of BOP hits',
                labelnames=['operation', 'address', 'cloud', 'zookeeper']
            )
            
            self.arcus_bop_misses_total = self.metric_factory.gauge(
                'arcus_bop_misses_total',
                'Total number of BOP misses',
                labelnames=['operation', 'address', 'cloud', 'zookeeper']
            )
            
            self.arcus_mop_hits_total = self.metric_factory.gauge(
                'arcus_mop_hits_total',
                'Total number of MOP hits',
                labelnames=['operation', 'address', 'cloud', 'zookeeper']
            )
            
            self.arcus_mop_misses_total = self.metric_factory.gauge(
                'arcus_mop_misses_total',
                'Total number of MOP misses',
                labelnames=['operation', 'address', 'cloud', 'zookeeper']
            )
            
            # Arcus Additional metrics
            self.arcus_heartbeat_count = self.metric_factory.gauge(
                'arcus_heartbeat_count_total',
                'Total number of heartbeat packets',
                labelnames=['address', 'cloud', 'zookeeper']
            )
            
            self.arcus_heartbeat_latency = self.metric_factory.gauge(
                'arcus_heartbeat_latency_microseconds',
                'Heartbeat latency in microseconds',
                labelnames=['address', 'cloud', 'zookeeper']
            )
            
            self.arcus_prefix_count = self.metric_factory.gauge(
                'arcus_prefix_count',
                'Current number of prefixes',
                labelnames=['address', 'cloud', 'zookeeper']
            )
            
            self.arcus_sticky_items = self.metric_factory.gauge(
                'arcus_sticky_items',
                'Current number of sticky items',
                labelnames=['address', 'cloud', 'zookeeper']
            )
            
            self.arcus_sticky_bytes = self.metric_factory.gauge(
                'arcus_sticky_bytes',
                'Current sticky memory usage in bytes',
                labelnames=['address', 'cloud', 'zookeeper']
            )
            
            self.arcus_outofmemorys = self.metric_factory.gauge(
                'arcus_outofmemorys_total',
                'Total number of out of memory events',
                labelnames=['address', 'cloud', 'zookeeper']
            )
        
        except ValueError as e:
            if 'Duplicated timeseries' in str(e):
                # Metrics already registered, get them from the registry
                self.logger.info("Arcus metrics already registered in shared registry, reusing them")
                # Import wrappers
                from prometheus_wrapper import GaugeWrapper
                # Get existing metrics from registry
                for collector in self.registry._collector_to_names:
                    if hasattr(collector, '_name'):
                        metric_name = collector._name
                        if metric_name == 'arcus_lop_commands_total':
                            self.arcus_lop_commands_total = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'arcus_sop_commands_total':
                            self.arcus_sop_commands_total = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'arcus_bop_commands_total':
                            self.arcus_bop_commands_total = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'arcus_mop_commands_total':
                            self.arcus_mop_commands_total = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'arcus_lop_hits_total':
                            self.arcus_lop_hits_total = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'arcus_lop_misses_total':
                            self.arcus_lop_misses_total = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'arcus_sop_hits_total':
                            self.arcus_sop_hits_total = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'arcus_sop_misses_total':
                            self.arcus_sop_misses_total = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'arcus_bop_hits_total':
                            self.arcus_bop_hits_total = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'arcus_bop_misses_total':
                            self.arcus_bop_misses_total = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'arcus_mop_hits_total':
                            self.arcus_mop_hits_total = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'arcus_mop_misses_total':
                            self.arcus_mop_misses_total = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'arcus_heartbeat_count_total':
                            self.arcus_heartbeat_count = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'arcus_heartbeat_latency_microseconds':
                            self.arcus_heartbeat_latency = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'arcus_prefix_count':
                            self.arcus_prefix_count = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'arcus_sticky_items':
                            self.arcus_sticky_items = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'arcus_sticky_bytes':
                            self.arcus_sticky_bytes = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'arcus_outofmemorys_total':
                            self.arcus_outofmemorys = GaugeWrapper(collector, self.metric_factory.default_labels)
            else:
                raise
    
    async def _collect_single_instance(self, host_port: str, client: MemcachedClient):
        """Override to collect both base memcached and Arcus metrics"""
        # Collect base memcached metrics
        await super()._collect_single_instance(host_port, client)
        
        # Collect Arcus-specific metrics
        try:
            await self._collect_arcus_metrics(client, host_port)
        except Exception as e:
            self.logger.warning(f"Failed to collect Arcus metrics from {host_port}: {e}")
    
    async def _collect_arcus_metrics(self, client: MemcachedClient, instance: str):
        """Collect Arcus-specific metrics from the given client instance"""
        try:
            # Check if server is up
            if not await client.is_alive():
                self.logger.warning(f"Arcus server {instance} is not responding")
                return
            
            # Get cloud name for this instance
            with self._cloud_map_lock:
                cloud = self.cloud_instance_map.get(instance, 'unknown')
            
            # Get zookeeper address
            zookeeper = self.zookeeper_addr or 'none'
            
            # Get stats from the Arcus server
            stats = await client.get_stats()
            
            # Update Arcus collection command metrics
            lop_commands = {
                'create': stats.get('cmd_lop_create', 0),
                'insert': stats.get('cmd_lop_insert', 0),
                'delete': stats.get('cmd_lop_delete', 0),
                'get': stats.get('cmd_lop_get', 0)
            }
            
            for operation, count in lop_commands.items():
                self.arcus_lop_commands_total.labels(operation=operation, address=instance, cloud=cloud, zookeeper=zookeeper).set(count)
            
            sop_commands = {
                'create': stats.get('cmd_sop_create', 0),
                'insert': stats.get('cmd_sop_insert', 0),
                'delete': stats.get('cmd_sop_delete', 0),
                'get': stats.get('cmd_sop_get', 0),
                'exist': stats.get('cmd_sop_exist', 0)
            }
            
            for operation, count in sop_commands.items():
                self.arcus_sop_commands_total.labels(operation=operation, address=instance, cloud=cloud, zookeeper=zookeeper).set(count)
            
            bop_commands = {
                'create': stats.get('cmd_bop_create', 0),
                'insert': stats.get('cmd_bop_insert', 0),
                'update': stats.get('cmd_bop_update', 0),
                'delete': stats.get('cmd_bop_delete', 0),
                'get': stats.get('cmd_bop_get', 0),
                'count': stats.get('cmd_bop_count', 0),
                'position': stats.get('cmd_bop_position', 0),
                'pwg': stats.get('cmd_bop_pwg', 0),
                'gbp': stats.get('cmd_bop_gbp', 0),
                'mget': stats.get('cmd_bop_mget', 0),
                'smget': stats.get('cmd_bop_smget', 0),
                'incr': stats.get('cmd_bop_incr', 0),
                'decr': stats.get('cmd_bop_decr', 0)
            }
            
            for operation, count in bop_commands.items():
                self.arcus_bop_commands_total.labels(operation=operation, address=instance, cloud=cloud, zookeeper=zookeeper).set(count)
            
            mop_commands = {
                'create': stats.get('cmd_mop_create', 0),
                'insert': stats.get('cmd_mop_insert', 0),
                'update': stats.get('cmd_mop_update', 0),
                'delete': stats.get('cmd_mop_delete', 0),
                'get': stats.get('cmd_mop_get', 0)
            }
            
            for operation, count in mop_commands.items():
                self.arcus_mop_commands_total.labels(operation=operation, address=instance, cloud=cloud, zookeeper=zookeeper).set(count)
            
            # Update LOP hit/miss metrics
            lop_hits = {
                'insert': stats.get('lop_insert_hits', 0),
                'delete_elem': stats.get('lop_delete_elem_hits', 0),
                'delete_none': stats.get('lop_delete_none_hits', 0),
                'get_elem': stats.get('lop_get_elem_hits', 0),
                'get_none': stats.get('lop_get_none_hits', 0)
            }
            
            for operation, count in lop_hits.items():
                self.arcus_lop_hits_total.labels(operation=operation, address=instance, cloud=cloud, zookeeper=zookeeper).set(count)
            
            lop_misses = {
                'insert': stats.get('lop_insert_misses', 0),
                'delete': stats.get('lop_delete_misses', 0),
                'get': stats.get('lop_get_misses', 0)
            }
            
            for operation, count in lop_misses.items():
                self.arcus_lop_misses_total.labels(operation=operation, address=instance, cloud=cloud, zookeeper=zookeeper).set(count)
            
            # Update SOP hit/miss metrics
            sop_hits = {
                'insert': stats.get('sop_insert_hits', 0),
                'delete_elem': stats.get('sop_delete_elem_hits', 0),
                'delete_none': stats.get('sop_delete_none_hits', 0),
                'get_elem': stats.get('sop_get_elem_hits', 0),
                'get_none': stats.get('sop_get_none_hits', 0),
                'exist': stats.get('sop_exist_hits', 0)
            }
            
            for operation, count in sop_hits.items():
                self.arcus_sop_hits_total.labels(operation=operation, address=instance, cloud=cloud, zookeeper=zookeeper).set(count)
            
            sop_misses = {
                'insert': stats.get('sop_insert_misses', 0),
                'delete': stats.get('sop_delete_misses', 0),
                'get': stats.get('sop_get_misses', 0),
                'exist': stats.get('sop_exist_misses', 0)
            }
            
            for operation, count in sop_misses.items():
                self.arcus_sop_misses_total.labels(operation=operation, address=instance, cloud=cloud, zookeeper=zookeeper).set(count)
            
            # Update BOP hit/miss metrics  
            bop_hits = {
                'insert': stats.get('bop_insert_hits', 0),
                'update_elem': stats.get('bop_update_elem_hits', 0),
                'update_none': stats.get('bop_update_none_hits', 0),
                'delete_elem': stats.get('bop_delete_elem_hits', 0),
                'delete_none': stats.get('bop_delete_none_hits', 0),
                'get_elem': stats.get('bop_get_elem_hits', 0),
                'get_none': stats.get('bop_get_none_hits', 0),
                'count': stats.get('bop_count_hits', 0),
                'position_elem': stats.get('bop_position_elem_hits', 0),
                'position_none': stats.get('bop_position_none_hits', 0),
                'pwg_elem': stats.get('bop_pwg_elem_hits', 0),
                'pwg_none': stats.get('bop_pwg_none_hits', 0),
                'gbp_elem': stats.get('bop_gbp_elem_hits', 0),
                'gbp_none': stats.get('bop_gbp_none_hits', 0),
                'incr_elem': stats.get('bop_incr_elem_hits', 0),
                'incr_none': stats.get('bop_incr_none_hits', 0),
                'decr_elem': stats.get('bop_decr_elem_hits', 0),
                'decr_none': stats.get('bop_decr_none_hits', 0)
            }
            
            for operation, count in bop_hits.items():
                self.arcus_bop_hits_total.labels(operation=operation, address=instance, cloud=cloud, zookeeper=zookeeper).set(count)
            
            bop_misses = {
                'insert': stats.get('bop_insert_misses', 0),
                'update': stats.get('bop_update_misses', 0),
                'delete': stats.get('bop_delete_misses', 0),
                'get': stats.get('bop_get_misses', 0),
                'count': stats.get('bop_count_misses', 0),
                'position': stats.get('bop_position_misses', 0),
                'pwg': stats.get('bop_pwg_misses', 0),
                'gbp': stats.get('bop_gbp_misses', 0),
                'incr': stats.get('bop_incr_misses', 0),
                'decr': stats.get('bop_decr_misses', 0)
            }
            
            for operation, count in bop_misses.items():
                self.arcus_bop_misses_total.labels(operation=operation, address=instance, cloud=cloud, zookeeper=zookeeper).set(count)
            
            # Update MOP hit/miss metrics
            mop_hits = {
                'insert': stats.get('mop_insert_hits', 0),
                'update_elem': stats.get('mop_update_elem_hits', 0),
                'update_none': stats.get('mop_update_none_hits', 0),
                'delete_elem': stats.get('mop_delete_elem_hits', 0),
                'delete_none': stats.get('mop_delete_none_hits', 0),
                'get_elem': stats.get('mop_get_elem_hits', 0),
                'get_none': stats.get('mop_get_none_hits', 0)
            }
            
            for operation, count in mop_hits.items():
                self.arcus_mop_hits_total.labels(operation=operation, address=instance, cloud=cloud, zookeeper=zookeeper).set(count)
            
            mop_misses = {
                'insert': stats.get('mop_insert_misses', 0),
                'update': stats.get('mop_update_misses', 0),
                'delete': stats.get('mop_delete_misses', 0),
                'get': stats.get('mop_get_misses', 0)
            }
            
            for operation, count in mop_misses.items():
                self.arcus_mop_misses_total.labels(operation=operation, address=instance, cloud=cloud, zookeeper=zookeeper).set(count)
            
            # Update additional Arcus metrics
            self.arcus_heartbeat_count.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats.get('hb_count', 0))
            self.arcus_heartbeat_latency.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats.get('hb_latency', 0))
            self.arcus_prefix_count.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats.get('curr_prefixes', 0))
            self.arcus_sticky_items.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats.get('sticky_items', 0))
            self.arcus_sticky_bytes.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats.get('sticky_bytes', 0))
            self.arcus_outofmemorys.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats.get('outofmemorys', 0))
            
            # Additional metrics for collection oks
            if 'lop_create_oks' in stats:
                self.arcus_lop_commands_total.labels(operation='create_ok', address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['lop_create_oks'])
            if 'sop_create_oks' in stats:
                self.arcus_sop_commands_total.labels(operation='create_ok', address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['sop_create_oks'])
            if 'bop_create_oks' in stats:
                self.arcus_bop_commands_total.labels(operation='create_ok', address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['bop_create_oks'])
            if 'mop_create_oks' in stats:
                self.arcus_mop_commands_total.labels(operation='create_ok', address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['mop_create_oks'])
            
            if 'bop_mget_oks' in stats:
                self.arcus_bop_commands_total.labels(operation='mget_ok', address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['bop_mget_oks'])
            if 'bop_smget_oks' in stats:
                self.arcus_bop_commands_total.labels(operation='smget_ok', address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['bop_smget_oks'])
            
            self.logger.debug(f"Collected Arcus metrics from {instance}")
            
        except Exception as e:
            self.logger.error(f"Failed to collect Arcus metrics: {e}")
    
    def stop(self):
        """Stop the exporter and close ZooKeeper connection"""
        super().stop()
        
        # Close ZooKeeper connection
        if self.zk_client:
            try:
                self.zk_client.stop()
                self.zk_client.close()
                self.logger.info("ZooKeeper connection closed")
            except Exception as e:
                self.logger.warning(f"Error closing ZooKeeper connection: {e}")
    
    def _setup_zookeeper_watcher(self):
        """Setup ZooKeeper watcher for cache_list changes"""
        if not ZOOKEEPER_AVAILABLE:
            self.logger.warning("ZooKeeper not available, cannot setup watcher")
            return
            
        try:
            self.zk_client = KazooClient(hosts=self.zookeeper_addr, timeout=10.0)
            self.zk_client.start(timeout=10)
            
            # Compile cloud name regex pattern
            try:
                cloud_pattern = re.compile(self.cloud_name)
            except re.error as e:
                self.logger.error(f"Invalid cloud name regex pattern '{self.cloud_name}': {e}")
                return
            
            # Get all clouds from /arcus/cache_list/
            cache_list_base = "/arcus/cache_list"
            if not self.zk_client.exists(cache_list_base):
                self.logger.error(f"Cache list base path does not exist: {cache_list_base}")
                return
            
            # Find all clouds matching the pattern
            all_clouds = self.zk_client.get_children(cache_list_base)
            matched_clouds = [cloud for cloud in all_clouds if cloud_pattern.match(cloud)]
            
            if not matched_clouds:
                self.logger.warning(f"No clouds matched pattern '{self.cloud_name}' in {all_clouds}")
                return
            
            self.logger.info(f"Matched clouds: {matched_clouds} (pattern: '{self.cloud_name}')")
            
            # Collect all instances from all matched clouds first
            all_instances = []
            cloud_map = {}  # Maps instance to cloud name
            for cloud in matched_clouds:
                cache_list_path = f"{cache_list_base}/{cloud}"
                try:
                    children = self.zk_client.get_children(cache_list_path)
                    for child in children:
                        try:
                            addr, hosts = child.split('-', 1)
                            all_instances.append(addr)
                            cloud_map[addr] = cloud  # Store cloud mapping
                        except Exception as e:
                            self.logger.warning(f"Failed to process node {child}: {e}")
                            continue
                except Exception as e:
                    self.logger.error(f"Failed to get instances from {cache_list_path}: {e}")
            
            # Update cloud instance mapping
            with self._cloud_map_lock:
                self.cloud_instance_map = cloud_map
            
            # Update clients with all instances at once
            if all_instances:
                self.logger.info(f"Total instances from all clouds: {len(all_instances)}")
                self._update_clients(all_instances)
            
            # Now set up watchers for all matched clouds
            for cloud in matched_clouds:
                cache_list_path = f"{cache_list_base}/{cloud}"
                self._watch_cache_list(cache_list_path)
                self.logger.info(f"ZooKeeper watcher setup for {cache_list_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to setup ZooKeeper watcher: {e}")

    def _watch_cache_list(self, path: str):
        """Watch cache_list for changes"""
        def watcher(event):
            self.logger.info(f"ZooKeeper event: {event}")
            if event.type == 'CHILD':
                try:
                    # Re-collect instances from ALL watched clouds
                    cache_list_base = "/arcus/cache_list"
                    cloud_pattern = re.compile(self.cloud_name)
                    all_clouds = self.zk_client.get_children(cache_list_base)
                    matched_clouds = [cloud for cloud in all_clouds if cloud_pattern.match(cloud)]
                    
                    all_instances = []
                    cloud_map = {}  # Maps instance to cloud name
                    for cloud in matched_clouds:
                        cloud_path = f"{cache_list_base}/{cloud}"
                        try:
                            children = self.zk_client.get_children(cloud_path, watch=watcher if cloud_path == path else None)
                            for child in children:
                                try:
                                    addr, hosts = child.split('-', 1)
                                    all_instances.append(addr)
                                    cloud_map[addr] = cloud  # Store cloud mapping
                                except Exception as e:
                                    self.logger.warning(f"Failed to process node {child}: {e}")
                                    continue
                        except Exception as e:
                            self.logger.error(f"Failed to get instances from {cloud_path}: {e}")
                    
                    # Update cloud instance mapping
                    with self._cloud_map_lock:
                        self.cloud_instance_map = cloud_map
                    
                    self.logger.info(f"Updated instances from ZooKeeper: {len(all_instances)} total")
                    self._update_clients(all_instances)
                    
                except Exception as e:
                    self.logger.error(f"Failed to handle ZooKeeper event: {e}")
        
        try:
            # Set initial watcher (instances already collected in _setup_zookeeper_watcher)
            self.zk_client.get_children(path, watch=watcher)
            
        except Exception as e:
            self.logger.error(f"Failed to setup watcher for {path}: {e}")


def run_arcus_exporter(args=None):
    """Run arcus exporter"""
    parser = argparse.ArgumentParser(description='Arcus Prometheus Exporter')
    parser.add_argument('--zookeeper-addr', required=True,
                       help='ZooKeeper address')
    parser.add_argument('--cloud-name', default='.*',
                       help='Cloud name pattern (regex, default: .* for all clouds)')
    parser.add_argument('--exporter-port', type=int, default=9150,
                       help='Exporter HTTP port (default: 9150)')
    parser.add_argument('--collect-interval', type=float, default=3.0,
                       help='Metrics collection interval in seconds (default: 3.0)')
    parser.add_argument('--max-concurrent', type=int, default=10,
                       help='Maximum concurrent instance collections (default: 10)')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Log level (default: INFO)')
    parser.add_argument('--region', 
                       help='Region label for metrics')
    
    # Node metrics via SSH
    parser.add_argument('--enable-node-metrics', action='store_true',
                       help='Enable node metrics collection via SSH (default: False)')
    parser.add_argument('--include-k8s-node', action='store_true',
                       help='Include Kubernetes nodes in node metrics collection (default: False)')
    parser.add_argument('--ssh-username', default='root',
                       help='SSH username for remote node metrics (default: root)')
    parser.add_argument('--ssh-key-file',
                       help='SSH private key file path')
    parser.add_argument('--ssh-port', type=int, default=22,
                       help='SSH port (default: 22)')
    
    args = parser.parse_args(args)
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    # Create and start exporter
    try:
        # Default labels - don't add cloud_name here, it will be added per-instance
        default_labels = {}
        if args.region:
            default_labels['region'] = args.region
        
        # Determine flags
        enable_node_metrics = args.enable_node_metrics
        exclude_k8s_node = not args.include_k8s_node
        
        # Create Arcus exporter
        exporter = ArcusPrometheusExporter(
            zookeeper_addr=args.zookeeper_addr,
            cloud_name=args.cloud_name,
            exporter_port=args.exporter_port,
            collect_interval=args.collect_interval,
            default_labels=default_labels,
            max_concurrent=args.max_concurrent,
            enable_node_metrics=enable_node_metrics,
            exclude_k8s_node=exclude_k8s_node,
            ssh_username=args.ssh_username,
            ssh_key_file=args.ssh_key_file,
            ssh_port=args.ssh_port
        )
        exporter.start()
        logger.info(f"Exporter is running. Press Ctrl+C to stop.")
        
        # Keep the main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        finally:
            exporter.stop()
            logger.info("Exporter stopped")
            
    except Exception as e:
        logger.error(f"Failed to start exporter: {e}")
        sys.exit(1)


if __name__ == '__main__':
    run_arcus_exporter()
