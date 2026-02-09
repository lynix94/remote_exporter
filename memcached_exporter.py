#!/usr/bin/env python3
"""
Memcached Prometheus Exporter

A Python3 implementation of a Prometheus exporter for Memcached servers.
Collects various statistics from Memcached and exposes them as Prometheus metrics.
"""

import time
import socket
import logging
import threading
import asyncio
import argparse
from typing import Dict, Any, Optional, List
from prometheus_client import start_http_server
from prometheus_client.core import CollectorRegistry

# Prometheus wrapper import
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from prometheus_wrapper import MetricFactory, GaugeWrapper, CounterWrapper, InfoWrapper, HistogramWrapper

# Remote node exporter import
try:
    from node_exporter import RemoteNodeCollector
    REMOTE_NODE_AVAILABLE = True
except ImportError:
    REMOTE_NODE_AVAILABLE = False
    RemoteNodeCollector = None

# Import is_k8s_node from redis_exporter module
from redis_exporter import is_k8s_node

ONE_LINE_COMMANDS = {
    'version': True
}


class MemcachedClient:
    """Simple memcached client for collecting statistics with connection pooling"""
    
    def __init__(self, host: str = 'localhost', port: int = 11211, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)
        self._reader = None
        self._writer = None
        self._connection_lock = asyncio.Lock()
    
    async def _ensure_connection(self):
        """Ensure connection is established and healthy"""
        async with self._connection_lock:
            # Check if connection exists and is open
            if self._writer and not self._writer.is_closing():
                return self._reader, self._writer
            
            # Close old connection if exists
            if self._writer:
                try:
                    self._writer.close()
                    await self._writer.wait_closed()
                except:
                    pass
            
            # Create new connection
            try:
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=self.timeout
                )
                # Send ping to verify
                self._writer.write(b'ping\r\n')
                await self._writer.drain()
                data = await asyncio.wait_for(self._reader.read(4096), timeout=self.timeout)
                return self._reader, self._writer
            except Exception as e:
                self.logger.error(f"Failed to connect to {self.host}:{self.port}: {e}")
                raise
    
    async def _send_command(self, command: str, retry: bool = True) -> str:
        """Send a command to memcached and return the response"""
        one_line_resp = command in ONE_LINE_COMMANDS

        try:
            reader, writer = await self._ensure_connection()
            
            writer.write(f"{command}\r\n".encode('utf-8'))
            await writer.drain()
            
            response = b""
            while True:
                data = await asyncio.wait_for(reader.read(4096), timeout=self.timeout)
                if not data:
                    break
                response += data
                if response.endswith(b"END\r\n"):
                    break
                if one_line_resp:
                    break
                if response.startswith(b"ERROR "):
                    break

            return response.decode('utf-8')

        except Exception as e:
            # On error, invalidate connection and retry once
            if retry:
                self.logger.debug(f"Command failed, reconnecting: {e}")
                async with self._connection_lock:
                    if self._writer:
                        try:
                            self._writer.close()
                            await self._writer.wait_closed()
                        except:
                            pass
                    self._writer = None
                    self._reader = None
                return await self._send_command(command, retry=False)
            else:
                self.logger.error(f"Failed to send command '{command}': {e}")
                raise
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get general statistics from memcached"""
        response = await self._send_command("stats")
        return self._parse_stats_response(response)
    
    async def get_stats_items(self) -> Dict[str, Any]:
        """Get item statistics from memcached"""
        response = await self._send_command("stats items")
        return self._parse_stats_response(response)
    
    async def get_stats_slabs(self) -> Dict[str, Any]:
        """Get slab statistics from memcached"""
        response = await self._send_command("stats slabs")
        return self._parse_stats_response(response)
    
    async def get_stats_sizes(self) -> Dict[str, Any]:
        """Get size statistics from memcached"""
        response = await self._send_command("stats sizes")
        return self._parse_stats_response(response)
    
    def _parse_stats_response(self, response: str) -> Dict[str, Any]:
        """Parse memcached stats response"""
        stats = {}
        for line in response.strip().split('\n'):
            if line.startswith('STAT'):
                parts = line.split(' ', 2)
                if len(parts) >= 3:
                    key = parts[1]
                    value = parts[2]
                    # Try to convert to appropriate type
                    try:
                        if '.' in value:
                            stats[key] = float(value)
                        else:
                            stats[key] = int(value)
                    except ValueError:
                        stats[key] = value
        return stats
    
    async def is_alive(self) -> bool:
        """Check if memcached server is alive"""
        try:
            response = await self._send_command("version")
            return "VERSION" in response
        except:
            return False
    
    async def close(self):
        """Close the connection"""
        async with self._connection_lock:
            if self._writer:
                try:
                    self._writer.close()
                    await self._writer.wait_closed()
                except:
                    pass
                self._writer = None
                self._reader = None


class MemcachedPrometheusExporter:
    """Prometheus exporter for Memcached - supports multiple hosts"""
    
    def __init__(self, memcached_addrs: List[str] = None, 
                 exporter_port: int = 9150, collect_interval: float = 30.0,
                 default_labels: Dict[str, str] = None,
                 max_concurrent: int = 10,
                 enable_node_metrics: bool = True,
                 exclude_k8s_node: bool = True,
                 ssh_username: str = None,
                 ssh_key_file: str = None,
                 ssh_port: int = 22,
                 use_cloud_label: bool = False,
                 zookeeper_addr: str = None):
        
        self.memcached_addrs = memcached_addrs or []
        self.memcached_clients = {}
        self._clients_lock = threading.Lock()
        self.max_concurrent = max_concurrent
        self.use_cloud_label = use_cloud_label
        self.zookeeper_addr = zookeeper_addr
        
        # Node exporter settings
        self.enable_node_metrics = enable_node_metrics
        self.exclude_k8s_node = exclude_k8s_node
        self.ssh_username = ssh_username
        self.ssh_key_file = ssh_key_file
        self.ssh_port = ssh_port
        self.node_collector = None
        
        self.exporter_port = exporter_port
        self.collect_interval = collect_interval
        self.logger = logging.getLogger(__name__)
        
        # Create clients for each host
        self._update_clients(self.memcached_addrs)
        
        # Default labels
        self.default_labels = default_labels or {}
        
        # Create custom registry and metric factory
        self.registry = CollectorRegistry()
        self.metric_factory = MetricFactory(default_labels=self.default_labels, registry=self.registry)
        
        # Initialize metrics
        self._init_metrics()
        
        # Collection thread control
        self._running = False
        self._stop_event = threading.Event()
        self._collection_thread = None
        
        # Initialize node collector if enabled
        if self.enable_node_metrics and REMOTE_NODE_AVAILABLE:
            self.logger.info("Node metrics collection enabled (exclude_k8s_node: %s)", self.exclude_k8s_node)
            if not self.ssh_username:
                self.logger.warning("Node metrics enabled but no SSH username provided")
            else:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(self._init_node_collector())
                finally:
                    loop.close()
        elif self.enable_node_metrics:
            self.logger.warning("Node metrics enabled but node_exporter not available")
    
    def _update_clients(self, addrs: List[str]):
        """Update memcached clients based on addresses"""
        if not addrs:
            return
        
        new_clients = {}
        for addr in addrs:
            if ':' in addr:
                host, port = addr.rsplit(':', 1)
                port = int(port)
            else:
                host = addr
                port = 11211
            
            host_port = f"{host}:{port}"
            new_clients[host_port] = MemcachedClient(host, port)
        
        with self._clients_lock:
            self.memcached_clients = new_clients
    
    def _init_metrics(self):
        """Initialize Prometheus metrics using MetricFactory"""
        
        # Determine labelnames based on whether cloud label is needed
        base_labels = ['address', 'cloud', 'zookeeper'] if self.use_cloud_label else ['address']
        command_labels = ['address', 'command', 'cloud', 'zookeeper'] if self.use_cloud_label else ['address', 'command']
        
        try:
            # Server info
            self.memcached_up = self.metric_factory.gauge(
                'memcached_up', 
                'Whether the memcached server is up',
                labelnames=base_labels
            )
            
            self.memcached_version_info = self.metric_factory.info(
                'memcached_version',
                'Memcached version information',
                labelnames=base_labels
            )
            
            # Connection metrics
            self.memcached_current_connections = self.metric_factory.gauge(
                'memcached_current_connections',
                'Current number of open connections',
                labelnames=base_labels
            )
            
            self.memcached_total_connections = self.metric_factory.gauge(
                'memcached_total_connections_total',
                'Total number of connections opened since server start',
                labelnames=base_labels
            )
            
            # Memory metrics
            self.memcached_limit_bytes = self.metric_factory.gauge(
                'memcached_limit_bytes',
                'Number of bytes this server is allowed to use for storage',
                labelnames=base_labels
            )
            
            self.memcached_bytes = self.metric_factory.gauge(
                'memcached_bytes',
                'Current number of bytes used to store items',
                labelnames=base_labels
            )
            
            # Item metrics
            self.memcached_current_items = self.metric_factory.gauge(
                'memcached_current_items',
                'Current number of items stored',
                labelnames=base_labels
            )
            
            self.memcached_total_items = self.metric_factory.gauge(
                'memcached_total_items_total',
                'Total number of items stored since server start',
                labelnames=base_labels
            )
            
            # Operation metrics
            self.memcached_commands_total = self.metric_factory.gauge(
                'memcached_commands_total',
                'Total number of commands processed',
                labelnames=command_labels
            )
            
            self.memcached_get_hits_total = self.metric_factory.gauge(
                'memcached_get_hits_total',
                'Total number of successful GET requests',
                labelnames=base_labels
            )
            
            self.memcached_get_misses_total = self.metric_factory.gauge(
                'memcached_get_misses_total',
                'Total number of failed GET requests',
                labelnames=base_labels
            )
            
            # Cache metrics
            self.memcached_evictions_total = self.metric_factory.gauge(
                'memcached_evictions_total',
                'Total number of valid items removed from cache to free memory',
                labelnames=base_labels
            )
            
            self.memcached_reclaimed_total = self.metric_factory.gauge(
                'memcached_reclaimed_total',
                'Total number of times an entry was stored using memory from an expired entry',
                labelnames=base_labels
            )
            
            # Network metrics
            self.memcached_bytes_read_total = self.metric_factory.gauge(
                'memcached_bytes_read_total',
                'Total number of bytes read by this server',
                labelnames=base_labels
            )
            
            self.memcached_bytes_written_total = self.metric_factory.gauge(
                'memcached_bytes_written_total',
                'Total number of bytes sent by this server',
                labelnames=base_labels
            )
            
            # CPU metrics
            self.memcached_rusage_user_seconds = self.metric_factory.gauge(
                'memcached_rusage_user_seconds',
                'Total user time for this process',
                labelnames=base_labels
            )
            
            self.memcached_rusage_system_seconds = self.metric_factory.gauge(
                'memcached_rusage_system_seconds',
                'Total system time for this process',
                labelnames=base_labels
            )
            
            # Item size histogram
            self.memcached_item_size_bytes = self.metric_factory.histogram(
                'memcached_item_size_bytes',
                'Size distribution of stored items',
                buckets=[64, 256, 1024, 4096, 16384, 65536, 262144, 1048576],
                labelnames=['instance']
            )
            
            # Slab metrics
            self.memcached_slab_chunk_size_bytes = self.metric_factory.gauge(
                'memcached_slab_chunk_size_bytes',
                'Size of chunks in slab',
                labelnames=['address', 'slab']
            )
            
            self.memcached_slab_chunks_per_page = self.metric_factory.gauge(
                'memcached_slab_chunks_per_page',
                'Number of chunks per page in slab',
                labelnames=['address', 'slab']
            )
            
            self.memcached_slab_current_pages = self.metric_factory.gauge(
                'memcached_slab_current_pages',
                'Current number of pages in slab',
                labelnames=['address', 'slab']
            )
            
            self.memcached_slab_current_chunks = self.metric_factory.gauge(
                'memcached_slab_current_chunks',
                'Current number of chunks in slab',
                labelnames=['address', 'slab']
            )
            
            self.memcached_slab_chunks_used = self.metric_factory.gauge(
                'memcached_slab_chunks_used',
                'Number of chunks currently in use in slab',
                labelnames=['address', 'slab']
            )
        
        except ValueError as e:
            if 'Duplicated timeseries' in str(e):
                # Metrics already registered, get them from the registry
                self.logger.info("Metrics already registered in shared registry, reusing them")
                # Get existing metrics from registry
                for collector in self.registry._collector_to_names:
                    if hasattr(collector, '_name'):
                        metric_name = collector._name
                        if metric_name == 'memcached_up':
                            self.memcached_up = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_version':
                            self.memcached_version_info = InfoWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_current_connections':
                            self.memcached_current_connections = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_total_connections_total':
                            self.memcached_total_connections = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_limit_bytes':
                            self.memcached_limit_bytes = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_bytes':
                            self.memcached_bytes = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_current_items':
                            self.memcached_current_items = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_total_items':
                            self.memcached_total_items = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_evictions_total':
                            self.memcached_evictions = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_commands_total':
                            self.memcached_commands = CounterWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_items_evicted_time_seconds':
                            self.memcached_evicted_time = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_items_evicted_nonzero_total':
                            self.memcached_evicted_nonzero = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_items_evicted_unfetched_total':
                            self.memcached_evicted_unfetched = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_items_outofmemory_total':
                            self.memcached_outofmemory = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_items_reclaimed_total':
                            self.memcached_reclaimed = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_slab_chunk_size_bytes':
                            self.memcached_slab_chunk_size = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_slab_chunks_per_page':
                            self.memcached_slab_chunks_per_page = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_slab_current_pages':
                            self.memcached_slab_current_pages = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_slab_current_chunks':
                            self.memcached_slab_current_chunks = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'memcached_slab_chunks_used':
                            self.memcached_slab_chunks_used = GaugeWrapper(collector, self.metric_factory.default_labels)
            else:
                raise
    
    async def _init_node_collector(self):
        """Initialize node metrics collector"""
        unique_hosts = set()
        for addr in self.memcached_addrs:
            if ':' in addr:
                host = addr.split(':')[0]
            else:
                host = addr
            unique_hosts.add(host)
        
        # Filter out K8s nodes if exclude_k8s_node is enabled
        if self.exclude_k8s_node and unique_hosts:
            self.logger.info(f"Checking {len(unique_hosts)} hosts for K8s nodes...")
            filtered_hosts = set()
            
            async def check_host(host):
                is_k8s = await is_k8s_node(
                    host,
                    self.ssh_username,
                    self.ssh_key_file,
                    None,
                    self.ssh_port
                )
                if is_k8s:
                    self.logger.info(f"Excluding K8s node: {host}")
                    return None
                else:
                    return host
            
            results = await asyncio.gather(
                *[check_host(host) for host in unique_hosts],
                return_exceptions=True
            )
            
            for result in results:
                if result and not isinstance(result, Exception):
                    filtered_hosts.add(result)
            
            unique_hosts = filtered_hosts
            self.logger.info(f"After K8s filtering: {len(unique_hosts)} hosts")
        
        if unique_hosts:
            self.logger.info(f"Initializing node collector for {len(unique_hosts)} hosts")
            self.node_collector = RemoteNodeCollector(
                hosts=list(unique_hosts),
                username=self.ssh_username,
                key_file=self.ssh_key_file,
                port=self.ssh_port,
                timeout=10
            )
            self.node_collector.registry = self.registry
        else:
            self.logger.info("No hosts to monitor after filtering")
            self.node_collector = None
    
    async def _collect_node_metrics(self):
        """Collect node metrics from all unique hosts"""
        if self.node_collector:
            try:
                await self.node_collector.collect_all()
            except Exception as e:
                self.logger.error(f"Failed to collect node metrics: {e}")
    
    async def _collect_single_instance(self, host_port: str, client: MemcachedClient):
        """Collect metrics from a single memcached instance"""
        try:
            self.logger.debug(f"Collecting metrics from {host_port}")
            
            # Get cloud name if using cloud labels (for Arcus)
            cloud = None
            zookeeper = self.zookeeper_addr or 'none'
            if self.use_cloud_label and hasattr(self, 'cloud_instance_map'):
                with self._cloud_map_lock:
                    cloud = self.cloud_instance_map.get(host_port, 'unknown')
            
            # Check if server is up
            is_up = await client.is_alive()
            if self.use_cloud_label:
                self.memcached_up.labels(address=host_port, cloud=cloud, zookeeper=zookeeper).set(1 if is_up else 0)
            else:
                self.memcached_up.labels(address=host_port).set(1 if is_up else 0)
            
            if not is_up:
                self.logger.warning(f"Memcached server {host_port} is not responding")
                return
            
            # Collect stats sequentially
            try:
                stats = await client.get_stats()
                self._update_general_metrics(stats, host_port, cloud, zookeeper)
            except Exception as e:
                self.logger.warning(f"Failed to collect general stats from {host_port}: {e}")
            
            try:
                slab_stats = await client.get_stats_slabs()
                self._update_slab_metrics(slab_stats, host_port)
            except Exception as e:
                self.logger.warning(f"Failed to collect slab stats from {host_port}: {e}")
            
            try:
                item_stats = await client.get_stats_items()
                self._update_item_metrics(item_stats, host_port)
            except Exception as e:
                self.logger.warning(f"Failed to collect item stats from {host_port}: {e}")
                
        except Exception as e:
            self.logger.error(f"Failed to collect metrics from {host_port}: {e}")
            if self.use_cloud_label:
                cloud = 'unknown'
                zookeeper = self.zookeeper_addr or 'none'
                if hasattr(self, 'cloud_instance_map'):
                    with self._cloud_map_lock:
                        cloud = self.cloud_instance_map.get(host_port, 'unknown')
                self.memcached_up.labels(address=host_port, cloud=cloud, zookeeper=zookeeper).set(0)
            else:
                self.memcached_up.labels(address=host_port).set(0)
    
    async def _collect_metrics(self):
        """Collect metrics from all memcached hosts in parallel"""
        with self._clients_lock:
            clients_copy = self.memcached_clients.copy()
        
        if not clients_copy:
            return
        
        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def collect_with_semaphore(host_port, client):
            async with semaphore:
                await self._collect_single_instance(host_port, client)
        
        # Collect from all instances in parallel
        tasks = [
            collect_with_semaphore(host_port, client)
            for host_port, client in clients_copy.items()
        ]
        
        # Add node metrics collection if enabled
        if self.enable_node_metrics and self.node_collector:
            tasks.append(self._collect_node_metrics())
        
        start_time = time.time()
        await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = time.time() - start_time
        
        metrics_info = f"Collected metrics from {len(clients_copy)} memcached instances"
        if self.enable_node_metrics and self.node_collector:
            metrics_info += f" and {len(self.node_collector.hosts)} node hosts"
        metrics_info += f" in {elapsed:.2f}s"
        self.logger.info(metrics_info)
    
    def _update_general_metrics(self, stats: Dict[str, Any], instance: str, cloud: str = None, zookeeper: str = None):
        """Update general memcached metrics"""
        # Version info
        if 'version' in stats:
            if self.use_cloud_label:
                self.memcached_version_info.labels(address=instance, cloud=cloud, zookeeper=zookeeper).info({'version': str(stats['version'])})
            else:
                self.memcached_version_info.labels(address=instance).info({'version': str(stats['version'])})
        
        # Connection metrics
        if 'curr_connections' in stats:
            if self.use_cloud_label:
                self.memcached_current_connections.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['curr_connections'])
            else:
                self.memcached_current_connections.labels(address=instance).set(stats['curr_connections'])
        if 'total_connections' in stats:
            if self.use_cloud_label:
                self.memcached_total_connections.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['total_connections'])
            else:
                self.memcached_total_connections.labels(address=instance).set(stats['total_connections'])
        
        # Memory metrics
        if 'limit_maxbytes' in stats:
            if self.use_cloud_label:
                self.memcached_limit_bytes.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['limit_maxbytes'])
            else:
                self.memcached_limit_bytes.labels(address=instance).set(stats['limit_maxbytes'])
        if 'bytes' in stats:
            if self.use_cloud_label:
                self.memcached_bytes.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['bytes'])
            else:
                self.memcached_bytes.labels(address=instance).set(stats['bytes'])
        
        # Item metrics
        if 'curr_items' in stats:
            if self.use_cloud_label:
                self.memcached_current_items.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['curr_items'])
            else:
                self.memcached_current_items.labels(address=instance).set(stats['curr_items'])
        if 'total_items' in stats:
            if self.use_cloud_label:
                self.memcached_total_items.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['total_items'])
            else:
                self.memcached_total_items.labels(address=instance).set(stats['total_items'])
        
        # Command metrics
        commands = ['get', 'set', 'delete', 'incr', 'decr', 'cas', 'touch', 'flush']
        for cmd in commands:
            cmd_key = f'cmd_{cmd}'
            if cmd_key in stats:
                if self.use_cloud_label:
                    self.memcached_commands_total.labels(address=instance, command=cmd, cloud=cloud, zookeeper=zookeeper).set(stats[cmd_key])
                else:
                    self.memcached_commands_total.labels(address=instance, command=cmd).set(stats[cmd_key])
        
        # Hit/miss metrics
        if 'get_hits' in stats:
            if self.use_cloud_label:
                self.memcached_get_hits_total.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['get_hits'])
            else:
                self.memcached_get_hits_total.labels(address=instance).set(stats['get_hits'])
        if 'get_misses' in stats:
            if self.use_cloud_label:
                self.memcached_get_misses_total.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['get_misses'])
            else:
                self.memcached_get_misses_total.labels(address=instance).set(stats['get_misses'])
        
        # Cache metrics
        if 'evictions' in stats:
            if self.use_cloud_label:
                self.memcached_evictions_total.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['evictions'])
            else:
                self.memcached_evictions_total.labels(address=instance).set(stats['evictions'])
        if 'reclaimed' in stats:
            if self.use_cloud_label:
                self.memcached_reclaimed_total.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['reclaimed'])
            else:
                self.memcached_reclaimed_total.labels(address=instance).set(stats['reclaimed'])
        
        # Network metrics
        if 'bytes_read' in stats:
            if self.use_cloud_label:
                self.memcached_bytes_read_total.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['bytes_read'])
            else:
                self.memcached_bytes_read_total.labels(address=instance).set(stats['bytes_read'])
        if 'bytes_written' in stats:
            if self.use_cloud_label:
                self.memcached_bytes_written_total.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['bytes_written'])
            else:
                self.memcached_bytes_written_total.labels(address=instance).set(stats['bytes_written'])
        
        # CPU metrics
        if 'rusage_user' in stats:
            if self.use_cloud_label:
                self.memcached_rusage_user_seconds.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['rusage_user'])
            else:
                self.memcached_rusage_user_seconds.labels(address=instance).set(stats['rusage_user'])
        if 'rusage_system' in stats:
            if self.use_cloud_label:
                self.memcached_rusage_system_seconds.labels(address=instance, cloud=cloud, zookeeper=zookeeper).set(stats['rusage_system'])
            else:
                self.memcached_rusage_system_seconds.labels(address=instance).set(stats['rusage_system'])
    
    def _update_slab_metrics(self, stats: Dict[str, Any], instance: str):
        """Update slab-related metrics"""
        slabs = {}
        for key, value in stats.items():
            parts = key.split(':')
            if len(parts) == 2 and parts[0].isdigit():
                slab_id = parts[0]
                metric_name = parts[1]
                if slab_id not in slabs:
                    slabs[slab_id] = {}
                slabs[slab_id][metric_name] = value
        
        for slab_id, slab_stats in slabs.items():
            if 'chunk_size' in slab_stats:
                self.memcached_slab_chunk_size_bytes.labels(address=instance, slab=slab_id).set(slab_stats['chunk_size'])
            if 'chunks_per_page' in slab_stats:
                self.memcached_slab_chunks_per_page.labels(address=instance, slab=slab_id).set(slab_stats['chunks_per_page'])
            if 'total_pages' in slab_stats:
                self.memcached_slab_current_pages.labels(address=instance, slab=slab_id).set(slab_stats['total_pages'])
            if 'total_chunks' in slab_stats:
                self.memcached_slab_current_chunks.labels(address=instance, slab=slab_id).set(slab_stats['total_chunks'])
            if 'used_chunks' in slab_stats:
                self.memcached_slab_chunks_used.labels(address=instance, slab=slab_id).set(slab_stats['used_chunks'])
    
    def _update_item_metrics(self, stats: Dict[str, Any], instance: str):
        """Update item-related metrics"""
        # Parse item stats if needed
        pass
    
    def _start_collection_threads(self):
        """Start background collection threads"""
        self._collection_thread = threading.Thread(target=self._collection_loop, daemon=True)
        self._collection_thread.start()
    
    def _collection_loop(self):
        """Main collection loop running in a thread"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            while self._running:
                try:
                    loop.run_until_complete(self._collect_metrics())
                except Exception as e:
                    self.logger.error(f"Collection error: {e}")
                
                # Wait for next collection
                if self._stop_event.wait(self.collect_interval):
                    break
        finally:
            loop.close()
    
    def start(self):
        """Start the exporter"""
        self.logger.info(f"Starting Memcached exporter on port {self.exporter_port}")
        self.logger.info(f"Monitoring {len(self.memcached_clients)} memcached instances")
        
        self._running = True
        self._start_collection_threads()
        
        # Start HTTP server
        start_http_server(self.exporter_port, registry=self.registry)
        
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal, shutting down...")
            self.stop()
    
    def stop(self):
        """Stop the exporter"""
        self.logger.info("Stopping exporter...")
        self._running = False
        self._stop_event.set()
        
        if self._collection_thread:
            self._collection_thread.join(timeout=5)


def run_memcached_exporter(args_list=None):
    """Main entry point for memcached exporter"""
    parser = argparse.ArgumentParser(description='Memcached Prometheus Exporter')
    parser.add_argument('--memcached-addrs', nargs='+', required=True,
                       help='Memcached server addresses in format host:port')
    parser.add_argument('--exporter-port', type=int, default=9150,
                       help='Exporter HTTP port (default: 9150)')
    parser.add_argument('--collect-interval', type=float, default=30.0,
                       help='Metrics collection interval in seconds (default: 30)')
    parser.add_argument('--max-concurrent', type=int, default=10,
                       help='Maximum concurrent collections (default: 10)')
    parser.add_argument('--region', help='Region label for metrics')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Log level (default: INFO)')
    
    # Node metrics arguments
    parser.add_argument('--enable-node-metrics', action='store_true',
                       help='Enable node metrics collection via SSH (default: False)')
    parser.add_argument('--include-k8s-node', action='store_true',
                       help='Include Kubernetes nodes in node metrics collection (default: False)')
    parser.add_argument('--ssh-username', default='root',
                       help='SSH username for node metrics (default: root)')
    parser.add_argument('--ssh-key-file',
                       help='SSH private key file path')
    parser.add_argument('--ssh-port', type=int, default=22,
                       help='SSH port (default: 22)')
    
    args = parser.parse_args(args_list)
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create default labels
    default_labels = {}
    if args.region:
        default_labels['region'] = args.region
    
    # Create and start exporter
    exporter = MemcachedPrometheusExporter(
        memcached_addrs=args.memcached_addrs,
        exporter_port=args.exporter_port,
        collect_interval=args.collect_interval,
        default_labels=default_labels,
        max_concurrent=args.max_concurrent,
        enable_node_metrics=args.enable_node_metrics,
        exclude_k8s_node=not args.include_k8s_node,
        ssh_username=args.ssh_username,
        ssh_key_file=args.ssh_key_file,
        ssh_port=args.ssh_port
    )
    
    exporter.start()


if __name__ == '__main__':
    run_memcached_exporter()
