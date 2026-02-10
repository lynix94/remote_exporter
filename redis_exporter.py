#!/usr/bin/env python3
"""
Redis Exporter - Redis metrics collector

Collects metrics from Redis standalone or Redis Cluster.
Supports automatic cluster node discovery and periodic refresh.
"""

import asyncio
import argparse
import logging
import sys
import os
import time
import re
from typing import Dict, List, Optional, Set, Tuple
from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server, generate_latest, CONTENT_TYPE_LATEST
from prometheus_client.core import CollectorRegistry
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from prometheus_wrapper import MetricFactory, GaugeWrapper, CounterWrapper, InfoWrapper

# Remote node exporter import
try:
    from node_exporter import RemoteNodeCollector
    REMOTE_NODE_AVAILABLE = True
except ImportError:
    REMOTE_NODE_AVAILABLE = False
    RemoteNodeCollector = None


async def is_k8s_node(host: str, username: str, key_file: Optional[str] = None, 
                      port: int = 22) -> bool:
    """Check if a host is a Kubernetes node"""
    try:
        import asyncssh
        
        # SSH connection options
        connect_kwargs = {
            'username': username,
            'known_hosts': None,
        }
        if key_file:
            connect_kwargs['client_keys'] = [key_file]
        
        async with asyncssh.connect(host, port=port, **connect_kwargs) as conn:
            # Check 1: /var/lib/kubelet directory exists
            result = await conn.run('test -d /var/lib/kubelet && echo yes || echo no', check=False)
            if result.stdout.strip() == 'yes':
                return True
            
            # Check 2: kubelet process is running
            result = await conn.run('pgrep -x kubelet > /dev/null && echo yes || echo no', check=False)
            if result.stdout.strip() == 'yes':
                return True
            
            # Check 3: /etc/kubernetes directory exists
            result = await conn.run('test -d /etc/kubernetes && echo yes || echo no', check=False)
            if result.stdout.strip() == 'yes':
                return True
            
            return False
    except Exception:
        # If we can't check, assume it's not a K8s node
        return False


class RedisClient:
    """Async Redis client for metrics collection"""
    
    def __init__(self, host: str, port: int = 6379, password: Optional[str] = None, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.password = password
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)
        self._reader = None
        self._writer = None
        self._connection_lock = asyncio.Lock()
    
    async def _ensure_connection(self):
        """Ensure connection is established"""
        async with self._connection_lock:
            if self._writer and not self._writer.is_closing():
                return self._reader, self._writer
            
            if self._writer:
                try:
                    self._writer.close()
                    await self._writer.wait_closed()
                except:
                    pass
            
            try:
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=self.timeout
                )
                
                # Authenticate if password provided
                if self.password:
                    await self._send_command_raw(f'AUTH {self.password}\r\n')
                
                return self._reader, self._writer
            except Exception as e:
                self.logger.error(f"Failed to connect to {self.host}:{self.port}: {e}")
                raise
    
    async def _send_command_raw(self, command: str) -> str:
        """Send raw command and return response"""
        reader, writer = await self._ensure_connection()
        
        writer.write(command.encode())
        await writer.drain()
        
        # Read response (simplified Redis protocol parsing)
        response = await asyncio.wait_for(reader.read(65536), timeout=self.timeout)
        return response.decode('utf-8', errors='ignore')
    
    async def _send_command(self, *args) -> str:
        """Send Redis command"""
        # Simple RESP protocol implementation
        command = f"*{len(args)}\r\n"
        for arg in args:
            arg_str = str(arg)
            command += f"${len(arg_str)}\r\n{arg_str}\r\n"
        
        return await self._send_command_raw(command)
    
    async def info(self, section: Optional[str] = None) -> Dict[str, str]:
        """Get Redis INFO"""
        if section:
            response = await self._send_command('INFO', section)
        else:
            response = await self._send_command('INFO')
        
        # Parse INFO response
        info_dict = {}
        for line in response.split('\r\n'):
            line = line.strip()
            if line and not line.startswith('#') and ':' in line:
                key, value = line.split(':', 1)
                info_dict[key] = value
        
        return info_dict
    
    async def cluster_nodes(self) -> List[Dict[str, str]]:
        """Get cluster nodes"""
        try:
            response = await self._send_command('CLUSTER', 'NODES')
            nodes = []
            
            for line in response.split('\n'):
                line = line.strip()
                if not line or line.startswith('$') or line.startswith('*') or line.startswith('+'):
                    continue
                
                parts = line.split()
                if len(parts) >= 8:
                    # Parse node info: id ip:port flags master repl_offset ping_pong role slots
                    node_info = {
                        'id': parts[0],
                        'addr': parts[1],
                        'flags': parts[2],
                        'master': parts[3] if parts[3] != '-' else None,
                        'ping_sent': parts[4],
                        'pong_recv': parts[5],
                        'epoch': parts[6],
                        'state': parts[7],
                        'slots': ' '.join(parts[8:]) if len(parts) > 8 else ''
                    }
                    
                    # Extract host:port
                    addr = node_info['addr'].split('@')[0]  # Remove cluster bus port
                    if addr and ':' in addr:
                        host, port = addr.rsplit(':', 1)
                        node_info['host'] = host
                        node_info['port'] = int(port)
                        nodes.append(node_info)
            
            return nodes
        except Exception as e:
            self.logger.debug(f"Not a cluster or error getting nodes: {e}")
            return []
    
    async def close(self):
        """Close connection"""
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except:
                pass


class RedisPrometheusExporter:
    """Redis Prometheus Exporter"""
    
    # Class-level metrics (shared across instances with same registry)
    _metrics_initialized = {}  # registry_id -> bool
    _metrics_lock = threading.Lock()
    
    def __init__(self,
                 redis_addrs: List[str] = None,
                 redis_password: Optional[str] = None,
                 cluster_name: Optional[str] = None,
                 cluster_refresh_interval: float = 60.0,
                 exporter_port: int = 9121,
                 collect_interval: float = 5.0,
                 default_labels: Dict[str, str] = None,
                 enable_node_metrics: bool = True,
                 exclude_k8s_node: bool = True,
                 ssh_username: str = 'root',
                 ssh_key_file: Optional[str] = None,
                 ssh_port: int = 22):
        
        self.redis_addrs = redis_addrs or []
        self.redis_password = redis_password
        self.cluster_name = cluster_name
        self.cluster_mode = bool(cluster_name)  # cluster_name이 있으면 cluster mode
        self.cluster_refresh_interval = cluster_refresh_interval
        self.exporter_port = exporter_port
        self.collect_interval = collect_interval
        self.default_labels = default_labels or {}
        self.enable_node_metrics = enable_node_metrics
        self.exclude_k8s_node = exclude_k8s_node
        self.ssh_username = ssh_username
        self.ssh_key_file = ssh_key_file
        self.ssh_port = ssh_port
        
        self.logger = logging.getLogger(__name__)
        self.registry = CollectorRegistry()
        self.metric_factory = MetricFactory(registry=self.registry, default_labels=self.default_labels)
        
        self.redis_clients: Dict[str, RedisClient] = {}
        self._clients_lock = threading.Lock()
        self._running = False
        self._collection_thread = None
        self._cluster_refresh_thread = None
        self.node_collector = None
        
        self._init_metrics()
        
        # Initialize node collector if enabled
        if self.enable_node_metrics and REMOTE_NODE_AVAILABLE:
            self.logger.info("Node metrics collection enabled (exclude_k8s_node: %s)", self.exclude_k8s_node)
            # Will be initialized after redis addresses are discovered
        elif self.enable_node_metrics:
            self.logger.warning("Node metrics requested but node_exporter not available")
    
    def _init_metrics(self):
        """Initialize Prometheus metrics"""
        # Always use consistent labelnames for shared registry compatibility
        base_labels = ['address', 'cluster']
        
        try:
            # Server info
            self.redis_up = self.metric_factory.gauge('redis_up', 'Redis instance is up', base_labels)
            self.redis_info = self.metric_factory.info('redis_info', 'Redis server info', base_labels)
            
            # Memory metrics
            self.redis_memory_used_bytes = self.metric_factory.gauge('redis_memory_used_bytes', 'Used memory in bytes', base_labels)
            self.redis_memory_max_bytes = self.metric_factory.gauge('redis_memory_max_bytes', 'Max memory in bytes', base_labels)
            self.redis_memory_rss_bytes = self.metric_factory.gauge('redis_memory_rss_bytes', 'RSS memory in bytes', base_labels)
            self.redis_memory_fragmentation_ratio = self.metric_factory.gauge('redis_memory_fragmentation_ratio', 'Memory fragmentation ratio', base_labels)
            
            # Client metrics
            self.redis_connected_clients = self.metric_factory.gauge('redis_connected_clients', 'Connected clients', base_labels)
            self.redis_blocked_clients = self.metric_factory.gauge('redis_blocked_clients', 'Blocked clients', base_labels)
            
            # Stats metrics
            self.redis_commands_processed_total = self.metric_factory.counter('redis_commands_processed_total', 'Total commands processed', base_labels)
            self.redis_connections_received_total = self.metric_factory.counter('redis_connections_received_total', 'Total connections received', base_labels)
            self.redis_keyspace_hits_total = self.metric_factory.counter('redis_keyspace_hits_total', 'Keyspace hits', base_labels)
            self.redis_keyspace_misses_total = self.metric_factory.counter('redis_keyspace_misses_total', 'Keyspace misses', base_labels)
            self.redis_evicted_keys_total = self.metric_factory.counter('redis_evicted_keys_total', 'Evicted keys', base_labels)
            self.redis_expired_keys_total = self.metric_factory.counter('redis_expired_keys_total', 'Expired keys', base_labels)
            
            # Persistence metrics
            self.redis_rdb_last_save_time = self.metric_factory.gauge('redis_rdb_last_save_time', 'Last RDB save timestamp', base_labels)
            self.redis_rdb_changes_since_last_save = self.metric_factory.gauge('redis_rdb_changes_since_last_save', 'Changes since last save', base_labels)
            
            # Replication metrics
            self.redis_connected_slaves = self.metric_factory.gauge('redis_connected_slaves', 'Connected slaves', base_labels)
            self.redis_master_repl_offset = self.metric_factory.gauge('redis_master_repl_offset', 'Master replication offset', base_labels)
            
            # Cluster metrics
            self.redis_cluster_enabled = self.metric_factory.gauge('redis_cluster_enabled', 'Cluster enabled', base_labels)
            self.redis_cluster_state = self.metric_factory.gauge('redis_cluster_state', 'Cluster state (1=ok, 0=fail)', base_labels)
            
            # Database metrics
            db_labels = ['address', 'db', 'cluster']
            self.redis_db_keys = self.metric_factory.gauge('redis_db_keys', 'Keys in database', db_labels)
            self.redis_db_expires = self.metric_factory.gauge('redis_db_expires', 'Keys with expiration', db_labels)
        
        except ValueError as e:
            if 'Duplicated timeseries' in str(e):
                # Metrics already registered, get them from the registry
                self.logger.info("Metrics already registered in shared registry, reusing them")
                # Get existing metrics from registry
                for collector in self.registry._collector_to_names:
                    if hasattr(collector, '_name'):
                        metric_name = collector._name
                        if metric_name == 'redis_up':
                            self.redis_up = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_info':
                            self.redis_info = InfoWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_memory_used_bytes':
                            self.redis_memory_used_bytes = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_memory_max_bytes':
                            self.redis_memory_max_bytes = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_memory_rss_bytes':
                            self.redis_memory_rss_bytes = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_memory_fragmentation_ratio':
                            self.redis_memory_fragmentation_ratio = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_connected_clients':
                            self.redis_connected_clients = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_blocked_clients':
                            self.redis_blocked_clients = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_commands_processed_total':
                            self.redis_commands_processed_total = CounterWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_connections_received_total':
                            self.redis_connections_received_total = CounterWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_keyspace_hits_total':
                            self.redis_keyspace_hits_total = CounterWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_keyspace_misses_total':
                            self.redis_keyspace_misses_total = CounterWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_evicted_keys_total':
                            self.redis_evicted_keys_total = CounterWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_expired_keys_total':
                            self.redis_expired_keys_total = CounterWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_rdb_last_save_time':
                            self.redis_rdb_last_save_time = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_rdb_changes_since_last_save':
                            self.redis_rdb_changes_since_last_save = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_connected_slaves':
                            self.redis_connected_slaves = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_master_repl_offset':
                            self.redis_master_repl_offset = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_cluster_enabled':
                            self.redis_cluster_enabled = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_cluster_state':
                            self.redis_cluster_state = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_db_keys':
                            self.redis_db_keys = GaugeWrapper(collector, self.metric_factory.default_labels)
                        elif metric_name == 'redis_db_expires':
                            self.redis_db_expires = GaugeWrapper(collector, self.metric_factory.default_labels)
            else:
                raise
    
    def _parse_addr(self, addr: str) -> Tuple[str, int]:
        """Parse address to (host, port)"""
        if ':' in addr:
            host, port_str = addr.rsplit(':', 1)
            return host, int(port_str)
        return addr, 6379
    
    async def _update_clients(self, addresses: List[str]):
        """Update Redis clients"""
        with self._clients_lock:
            current_addrs = set(addresses)
            existing_addrs = set(self.redis_clients.keys())
            
            # Remove old clients
            for addr in existing_addrs - current_addrs:
                self.logger.info(f"Removing Redis client: {addr}")
                del self.redis_clients[addr]
            
            # Add new clients
            for addr in current_addrs - existing_addrs:
                host, port = self._parse_addr(addr)
                self.logger.info(f"Adding Redis client: {addr}")
                self.redis_clients[addr] = RedisClient(
                    host=host,
                    port=port,
                    password=self.redis_password
                )
        
        # Update node collector if enabled
        if self.enable_node_metrics and REMOTE_NODE_AVAILABLE:
            await self._init_node_collector(addresses)
    
    async def _discover_cluster_nodes(self):
        """Discover cluster nodes"""
        if not self.cluster_mode or not self.redis_addrs:
            return
        
        self.logger.info("Discovering cluster nodes...")
        all_nodes = set()
        
        # Query each seed node
        for seed_addr in self.redis_addrs:
            try:
                host, port = self._parse_addr(seed_addr)
                client = RedisClient(host=host, port=port, password=self.redis_password)
                
                nodes = await client.cluster_nodes()
                await client.close()
                
                for node in nodes:
                    if 'host' in node and 'port' in node:
                        addr = f"{node['host']}:{node['port']}"
                        all_nodes.add(addr)
                        self.logger.debug(f"Found cluster node: {addr} (flags: {node.get('flags', 'unknown')})")
                
            except Exception as e:
                self.logger.warning(f"Failed to query cluster from {seed_addr}: {e}")
        
        if all_nodes:
            self.logger.info(f"Discovered {len(all_nodes)} cluster nodes")
            await self._update_clients(list(all_nodes))
        else:
            self.logger.warning("No cluster nodes discovered, using seed addresses")
            await self._update_clients(self.redis_addrs)
    
    def _cluster_refresh_worker(self):
        """Worker thread for periodic cluster refresh"""
        self.logger.info(f"Starting cluster refresh worker (interval: {self.cluster_refresh_interval}s)")
        
        # Create and set event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            while self._running:
                try:
                    loop.run_until_complete(self._discover_cluster_nodes())
                except Exception as e:
                    self.logger.error(f"Cluster refresh error: {e}")
                
                # Sleep
                time.sleep(self.cluster_refresh_interval)
        finally:
            loop.close()
    
    async def _collect_single_instance(self, addr: str, client: RedisClient):
        """Collect metrics from single Redis instance"""
        try:
            # Get INFO
            info = await client.info()
            
            # Use cluster name if provided, otherwise use 'standalone'
            cluster_label = self.cluster_name if self.cluster_name else 'standalone'
            
            # Server is up
            self.redis_up.labels(address=addr, cluster=cluster_label).set(1)
            
            # Server info
            self.redis_info.labels(address=addr, cluster=cluster_label).info({
                'redis_version': info.get('redis_version', 'unknown'),
                'redis_mode': info.get('redis_mode', 'unknown'),
                'os': info.get('os', 'unknown'),
                'role': info.get('role', 'unknown')
            })
            
            # Memory metrics
            if 'used_memory' in info:
                self.redis_memory_used_bytes.labels(address=addr, cluster=cluster_label).set(int(info['used_memory']))
            if 'maxmemory' in info:
                self.redis_memory_max_bytes.labels(address=addr, cluster=cluster_label).set(int(info['maxmemory']))
            if 'used_memory_rss' in info:
                self.redis_memory_rss_bytes.labels(address=addr, cluster=cluster_label).set(int(info['used_memory_rss']))
            if 'mem_fragmentation_ratio' in info:
                self.redis_memory_fragmentation_ratio.labels(address=addr, cluster=cluster_label).set(float(info['mem_fragmentation_ratio']))
            
            # Client metrics
            if 'connected_clients' in info:
                self.redis_connected_clients.labels(address=addr, cluster=cluster_label).set(int(info['connected_clients']))
            if 'blocked_clients' in info:
                self.redis_blocked_clients.labels(address=addr, cluster=cluster_label).set(int(info['blocked_clients']))
            
            # Stats metrics
            if 'total_commands_processed' in info:
                self.redis_commands_processed_total.labels(address=addr, cluster=cluster_label)._value._value = int(info['total_commands_processed'])
            if 'total_connections_received' in info:
                self.redis_connections_received_total.labels(address=addr, cluster=cluster_label)._value._value = int(info['total_connections_received'])
            if 'keyspace_hits' in info:
                self.redis_keyspace_hits_total.labels(address=addr, cluster=cluster_label)._value._value = int(info['keyspace_hits'])
            if 'keyspace_misses' in info:
                self.redis_keyspace_misses_total.labels(address=addr, cluster=cluster_label)._value._value = int(info['keyspace_misses'])
            if 'evicted_keys' in info:
                self.redis_evicted_keys_total.labels(address=addr, cluster=cluster_label)._value._value = int(info['evicted_keys'])
            if 'expired_keys' in info:
                self.redis_expired_keys_total.labels(address=addr, cluster=cluster_label)._value._value = int(info['expired_keys'])
            
            # Persistence metrics
            if 'rdb_last_save_time' in info:
                self.redis_rdb_last_save_time.labels(address=addr, cluster=cluster_label).set(int(info['rdb_last_save_time']))
            if 'rdb_changes_since_last_save' in info:
                self.redis_rdb_changes_since_last_save.labels(address=addr, cluster=cluster_label).set(int(info['rdb_changes_since_last_save']))
            
            # Replication metrics
            if 'connected_slaves' in info:
                self.redis_connected_slaves.labels(address=addr, cluster=cluster_label).set(int(info['connected_slaves']))
            if 'master_repl_offset' in info:
                self.redis_master_repl_offset.labels(address=addr, cluster=cluster_label).set(int(info['master_repl_offset']))
            
            # Cluster metrics
            if 'cluster_enabled' in info:
                self.redis_cluster_enabled.labels(address=addr, cluster=cluster_label).set(int(info['cluster_enabled']))
            
            # Database metrics (db0, db1, etc.)
            for key, value in info.items():
                if key.startswith('db'):
                    # Parse: db0:keys=123,expires=45
                    db_name = key
                    parts = value.split(',')
                    db_info = {}
                    for part in parts:
                        if '=' in part:
                            k, v = part.split('=', 1)
                            db_info[k] = v
                    
                    if 'keys' in db_info:
                        self.redis_db_keys.labels(address=addr, db=db_name, cluster=cluster_label).set(int(db_info['keys']))
                    if 'expires' in db_info:
                        self.redis_db_expires.labels(address=addr, db=db_name, cluster=cluster_label).set(int(db_info['expires']))
            
        except Exception as e:
            self.logger.error(f"Failed to collect from {addr}: {e}")
            self.redis_up.labels(address=addr, cluster=cluster_label).set(0)
    
    async def _init_node_collector(self, addresses: List[str]):
        """Initialize node metrics collector"""
        # Get unique hosts from redis addresses
        unique_hosts = set()
        for addr in addresses:
            host, _ = self._parse_addr(addr)
            unique_hosts.add(host)
        
        # Filter out K8s nodes if exclude_k8s_node is enabled
        if self.exclude_k8s_node and unique_hosts:
            self.logger.info(f"Checking {len(unique_hosts)} hosts for K8s nodes...")
            filtered_hosts = set()
            
            # Check each host concurrently
            async def check_host(host):
                is_k8s = await is_k8s_node(
                    host, 
                    self.ssh_username,
                    self.ssh_key_file,
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
            # Use the same registry
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
    
    async def _collect_metrics(self):
        """Collect metrics from all instances"""
        with self._clients_lock:
            clients_copy = self.redis_clients.copy()
        
        if not clients_copy:
            return
        
        tasks = [
            self._collect_single_instance(addr, client)
            for addr, client in clients_copy.items()
        ]
        
        # Add node metrics collection if enabled
        if self.enable_node_metrics and self.node_collector:
            tasks.append(self._collect_node_metrics())
        
        start_time = time.time()
        await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = time.time() - start_time
        
        metrics_info = f"Collected metrics from {len(clients_copy)} Redis instances"
        if self.enable_node_metrics and self.node_collector:
            metrics_info += f" and {len(self.node_collector.hosts)} node hosts"
        metrics_info += f" in {elapsed:.2f}s"
        self.logger.info(metrics_info)
    
    def _collection_worker(self):
        """Worker thread for periodic collection"""
        self.logger.info(f"Starting collection worker (interval: {self.collect_interval}s)")
        
        # Create and set event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            while self._running:
                try:
                    loop.run_until_complete(self._collect_metrics())
                except Exception as e:
                    self.logger.error(f"Collection error: {e}")
                
                time.sleep(self.collect_interval)
        finally:
            loop.close()
    
    def start(self):
        """Start the exporter"""
        self._running = True
        
        # Initial setup
        if self.cluster_mode:
            # Discover cluster nodes initially
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._discover_cluster_nodes())
            loop.close()
            
            # Start cluster refresh thread
            self._cluster_refresh_thread = threading.Thread(target=self._cluster_refresh_worker, daemon=True)
            self._cluster_refresh_thread.start()
        else:
            # Use provided addresses
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._update_clients(self.redis_addrs))
            loop.close()
        
        # Start collection thread
        self._collection_thread = threading.Thread(target=self._collection_worker, daemon=True)
        self._collection_thread.start()
        
        # Start HTTP server
        start_http_server(self.exporter_port, registry=self.registry)
        self.logger.info(f"Redis exporter started on port {self.exporter_port}")
    
    def stop(self):
        """Stop the exporter"""
        self._running = False
        
        # Close all clients
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        for client in self.redis_clients.values():
            loop.run_until_complete(client.close())
        loop.close()


def run_redis_exporter(args=None):
    """Run Redis exporter"""
    parser = argparse.ArgumentParser(description='Redis Prometheus Exporter')
    parser.add_argument('--redis-addrs', nargs='+', required=True,
                       help='Redis server addresses in format host:port')
    parser.add_argument('--redis-password',
                       help='Redis password')
    parser.add_argument('--cluster-name',
                       help='Redis Cluster name (enables cluster mode with automatic node discovery)')
    parser.add_argument('--cluster-refresh-interval', type=float, default=60.0,
                       help='Cluster node refresh interval in seconds (default: 60)')
    parser.add_argument('--exporter-port', type=int, default=9121,
                       help='Exporter HTTP port (default: 9121)')
    parser.add_argument('--collect-interval', type=float, default=5.0,
                       help='Metrics collection interval in seconds (default: 5.0)')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Log level (default: INFO)')
    parser.add_argument('--environment',
                       help='Environment label for metrics')
    parser.add_argument('--region',
                       help='Region label for metrics')
    
    # Node metrics via SSH
    parser.add_argument('--enable-node-metrics', action='store_true',
                       help='Enable node metrics collection via SSH (default: False)')
    parser.add_argument('--include-k8s-node', action='store_true',
                       help='Include Kubernetes nodes in node metrics collection (default: False, excludes K8s nodes)')
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
    # Reduce asyncssh verbose logging (connection/channel events)
    logging.getLogger('asyncssh').setLevel(logging.WARNING)    
    logger = logging.getLogger(__name__)
    
    # Create and start exporter
    try:
        # Default labels
        base_default_labels = {}
        if args.environment:
            base_default_labels['environment'] = args.environment
        if args.region:
            base_default_labels['region'] = args.region
        
        # Determine flags
        enable_node_metrics = args.enable_node_metrics
        exclude_k8s_node = not args.include_k8s_node  # By default exclude K8s nodes unless --include-k8s-node is set
        
        exporter = RedisPrometheusExporter(
            redis_addrs=args.redis_addrs,
            redis_password=args.redis_password,
            cluster_name=args.cluster_name,
            cluster_refresh_interval=args.cluster_refresh_interval,
            exporter_port=args.exporter_port,
            collect_interval=args.collect_interval,
            default_labels=base_default_labels,
            enable_node_metrics=enable_node_metrics,
            exclude_k8s_node=exclude_k8s_node,
            ssh_username=args.ssh_username,
            ssh_key_file=args.ssh_key_file,
            ssh_port=args.ssh_port
        )
        exporter.start()
        logger.info(f"Exporter is running. Press Ctrl+C to stop.")
        
        # Keep main thread alive
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
    run_redis_exporter()
