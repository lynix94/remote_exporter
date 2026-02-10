#!/usr/bin/env python3
"""
Remote Exporter CLI - Main entry point with subcommands

Unified exporter for collecting metrics from:
- node: System metrics from remote hosts via SSH
- arcus: Memcached/Arcus metrics with optional node metrics
- redis: Redis/Redis Cluster metrics with automatic node discovery
- conf: Run multiple exporters from YAML configuration file
"""

import argparse
import sys
import threading
import time
import logging
import gzip
from io import BytesIO
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


def main():
    parser = argparse.ArgumentParser(
        description='Remote Prometheus Exporter for Redis, Memcached, Arcus, and Node metrics',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run exporters from YAML configuration file
  %(prog)s -c config.yaml
  
  # Run with custom log level
  %(prog)s -c config.yaml --log-level DEBUG
        """
    )
    
    # Only keep essential CLI arguments
    parser.add_argument('-c', '--config', required=True,
                       help='Path to YAML configuration file')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Log level (default: INFO)')
    
    args = parser.parse_args()
    
    # Check PyYAML availability
    if not YAML_AVAILABLE:
        print("Error: PyYAML is not installed. Install it with: pip install pyyaml", file=sys.stderr)
        sys.exit(1)
    
    # Run from config file
    run_from_config(args.config, args.log_level)


def run_from_config(config_file: str, log_level: str = 'INFO'):
    """Run multiple exporters from YAML configuration file with shared registry"""
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_file}")
        sys.exit(1)
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML configuration: {e}")
        sys.exit(1)
    
    if not config or 'exporters' not in config:
        logger.error("Configuration file must contain 'exporters' section")
        sys.exit(1)
    
    exporters = config['exporters']
    if not isinstance(exporters, list):
        logger.error("'exporters' must be a list")
        sys.exit(1)
    
    # Get global settings (CLI arguments override config file)
    # Get global settings from config file
    exporter_port = config.get('exporter_port', 9150)
    collect_interval = config.get('collect_interval', 15.0)
    max_concurrent = config.get('max_concurrent', 10)
    
    # Get SSH settings from config
    ssh_username = config.get('ssh_username', 'root')
    ssh_key_file = config.get('ssh_key_file')
    ssh_port = config.get('ssh_port', 22)
    
    # Get region from config
    region = config.get('region')
    
    logger.info(f"Starting {len(exporters)} exporter(s) from configuration")
    logger.info(f"Shared settings - port: {exporter_port}, interval: {collect_interval}s, max_concurrent: {max_concurrent}, region: {region}")
    logger.info(f"SSH settings - username: {ssh_username}, key_file: {ssh_key_file}, port: {ssh_port}")
    
    # Create shared registry
    from prometheus_client.core import CollectorRegistry
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    shared_registry = CollectorRegistry()
    
    # Metrics cache for performance
    metrics_cache = {'data': None, 'compressed': None, 'timestamp': 0, 'lock': threading.Lock()}
    cache_ttl = 10  # Regenerate every 10 seconds in background
    
    def update_metrics_cache():
        """Background task to update metrics cache"""
        logger.info("Starting background metrics cache updater")
        
        # Do initial update immediately
        try:
            logger.info("Performing initial metrics cache update...")
            metrics_data = generate_latest(shared_registry)
            
            # Compress data
            compressed = BytesIO()
            with gzip.GzipFile(fileobj=compressed, mode='wb', compresslevel=6) as f:
                f.write(metrics_data)
            compressed_data = compressed.getvalue()
            
            # Update cache
            with metrics_cache['lock']:
                metrics_cache['data'] = metrics_data
                metrics_cache['compressed'] = compressed_data
                metrics_cache['timestamp'] = time.time()
            
            if len(metrics_data) > 0:
                ratio = 100 * len(compressed_data) / len(metrics_data)
                logger.info(f"Initial metrics cache ready: {len(metrics_data)} bytes raw, {len(compressed_data)} bytes compressed ({ratio:.1f}% ratio)")
            else:
                logger.warning(f"Initial metrics cache is empty - no metrics registered yet")
        except Exception as e:
            logger.error(f"Error in initial metrics cache update: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        # Continue with periodic updates
        while True:
            try:
                time.sleep(cache_ttl)
                
                # Generate metrics
                metrics_data = generate_latest(shared_registry)
                
                # Compress data
                compressed = BytesIO()
                with gzip.GzipFile(fileobj=compressed, mode='wb', compresslevel=6) as f:
                    f.write(metrics_data)
                compressed_data = compressed.getvalue()
                
                # Update cache
                with metrics_cache['lock']:
                    metrics_cache['data'] = metrics_data
                    metrics_cache['compressed'] = compressed_data
                    metrics_cache['timestamp'] = time.time()
                
                # Calculate compression ratio, handle empty metrics
                if len(metrics_data) > 0:
                    ratio = 100 * len(compressed_data) / len(metrics_data)
                    logger.debug(f"Metrics cache updated: {len(metrics_data)} bytes raw, {len(compressed_data)} bytes compressed ({ratio:.1f}% ratio)")
                else:
                    logger.debug(f"Metrics cache updated: empty metrics")
                
            except Exception as e:
                logger.error(f"Error updating metrics cache: {e}")
                import traceback
                logger.error(traceback.format_exc())
    
    class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
        """HTTP Server with threading support for concurrent requests"""
        daemon_threads = True
        allow_reuse_address = True
    
    class CachedMetricsHandler(BaseHTTPRequestHandler):
        """HTTP handler with pre-cached and compressed metrics"""
        
        def log_message(self, format, *args):
            """Suppress default logging"""
            pass
        
        def do_GET(self):
            if self.path == '/metrics':
                try:
                    accept_encoding = self.headers.get('Accept-Encoding', '')
                    use_gzip = 'gzip' in accept_encoding
                    
                    # Use cached data (always ready from background thread)
                    with metrics_cache['lock']:
                        if use_gzip and metrics_cache['compressed']:
                            response_data = metrics_cache['compressed']
                            self.send_response(200)
                            self.send_header('Content-Type', CONTENT_TYPE_LATEST)
                            self.send_header('Content-Encoding', 'gzip')
                            self.send_header('Content-Length', str(len(response_data)))
                            self.end_headers()
                            self.wfile.write(response_data)
                        elif metrics_cache['data']:
                            response_data = metrics_cache['data']
                            self.send_response(200)
                            self.send_header('Content-Type', CONTENT_TYPE_LATEST)
                            self.send_header('Content-Length', str(len(response_data)))
                            self.end_headers()
                            self.wfile.write(response_data)
                        else:
                            # Cache not ready yet (startup)
                            self.send_response(503)
                            self.send_header('Content-Type', 'text/plain')
                            self.end_headers()
                            self.wfile.write(b'Metrics cache initializing, please retry in a few seconds\n')
                
                except (BrokenPipeError, ConnectionResetError):
                    # Client disconnected, ignore silently
                    pass
                except Exception as e:
                    try:
                        logger.error(f"Error serving metrics: {e}")
                        self.send_error(500, f"Internal Server Error: {e}")
                    except (BrokenPipeError, ConnectionResetError):
                        pass
            elif self.path == '/':
                # Root path - show simple info page
                try:
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/html')
                    self.end_headers()
                    html = b"""<!DOCTYPE html>
<html>
<head><title>Remote Exporter</title></head>
<body>
<h1>Remote Exporter</h1>
<p>Prometheus exporter for Redis, Memcached, Arcus, and Node metrics.</p>
<p><a href="/metrics">View Metrics</a></p>
</body>
</html>"""
                    self.wfile.write(html)
                except (BrokenPipeError, ConnectionResetError):
                    pass
            else:
                try:
                    self.send_error(404, "Not Found")
                except (BrokenPipeError, ConnectionResetError):
                    pass
    
    # Collect all unique hosts for node metrics across all exporters
    all_hosts = set()
    has_node_metrics_enabled = False  # Track if any exporter has node metrics enabled
    
    # Parse exporters and collect host information
    exporter_instances = []
    for idx, exporter_config in enumerate(exporters):
        if 'type' not in exporter_config:
            logger.warning(f"Skipping exporter {idx}: missing 'type' field")
            continue
        
        exporter_type = exporter_config['type']
        
        # Node exporter always collects node metrics
        if exporter_type == 'node':
            has_node_metrics_enabled = True
            if 'hosts' in exporter_config:
                all_hosts.update(exporter_config['hosts'])
                logger.info(f"Node exporter #{idx}: added {len(exporter_config['hosts'])} hosts")
            else:
                logger.warning(f"Node exporter #{idx} has no 'hosts' field")
        
        # Collect hosts if node metrics enabled for other exporter types
        elif exporter_config.get('enable_node_metrics', False):
            has_node_metrics_enabled = True
            logger.info(f"Exporter {exporter_type} #{idx} has enable_node_metrics=True")
            if exporter_type == 'arcus':
                # For Arcus, check memcached_addrs or hosts field
                if 'memcached_addrs' in exporter_config:
                    for addr in exporter_config['memcached_addrs']:
                        host = addr.split(':')[0]
                        all_hosts.add(host)
                        logger.debug(f"Added host from arcus memcached_addrs: {host}")
                elif 'hosts' in exporter_config:
                    all_hosts.update(exporter_config['hosts'])
                    logger.debug(f"Added hosts from arcus hosts field: {exporter_config['hosts']}")
                else:
                    logger.warning(f"Arcus exporter has enable_node_metrics=True but no 'memcached_addrs' or 'hosts' field - hosts will be discovered from ZooKeeper at runtime")
            elif exporter_type == 'memcached' and 'memcached_addrs' in exporter_config:
                for addr in exporter_config['memcached_addrs']:
                    host = addr.split(':')[0]
                    all_hosts.add(host)
                    logger.debug(f"Added host from memcached: {host}")
            elif exporter_type == 'redis' and 'redis_addrs' in exporter_config:
                for addr in exporter_config['redis_addrs']:
                    host = addr.split(':')[0]
                    all_hosts.add(host)
                    logger.debug(f"Added host from redis: {host}")
        
        exporter_instances.append((exporter_type, exporter_config, idx))
    
    logger.info(f"Collected {len(all_hosts)} unique hosts for node metrics: {all_hosts}")
    
    # Create shared node collector if node metrics are enabled
    # Even if all_hosts is empty initially (e.g., Arcus with ZooKeeper discovery),
    # we still need to create the collector so it can be updated dynamically
    # If ssh_key_file is None, asyncssh will use default keys (~/.ssh/id_rsa, etc.)
    shared_node_collector = None
    if has_node_metrics_enabled:
        logger.info(f"Creating shared node collector (initial hosts: {len(all_hosts)})")
        from node_exporter import RemoteNodeCollector
        
        # Create collector with all hosts (no K8s filtering)
        shared_node_collector = RemoteNodeCollector(
            hosts=list(all_hosts),
            username=ssh_username,
            key_file=ssh_key_file,
            port=ssh_port,
            registry=shared_registry  # Use shared registry from the start
        )
        if ssh_key_file:
            logger.warning(f"[SHARED NODE] Shared node collector created (initial hosts: {len(all_hosts)}, username: {ssh_username}, ssh_key: {ssh_key_file})")
        else:
            logger.warning(f"[SHARED NODE] Shared node collector created (initial hosts: {len(all_hosts)}, username: {ssh_username}, using default SSH keys from ~/.ssh/)")
    else:
        logger.info(f"[SHARED NODE] Node metrics not enabled (has_node_metrics_enabled={has_node_metrics_enabled})")
    
    # Track which exporter types have already initialized metrics
    initialized_types = set()
    initialized_types_lock = threading.Lock()
    
    # Start all exporters with shared registry and node collector
    threads = []
    for exporter_type, exporter_config, idx in exporter_instances:
        thread = threading.Thread(
            target=run_exporter_from_config,
            args=(exporter_type, exporter_config, log_level, shared_registry, shared_node_collector, exporter_port, collect_interval, max_concurrent, region, ssh_username, ssh_key_file, ssh_port, initialized_types, initialized_types_lock),
            daemon=True,
            name=f"{exporter_type}-exporter-{idx}"
        )
        threads.append(thread)
        thread.start()
        logger.info(f"Started {exporter_type} exporter (thread: {thread.name})")
    
    if not threads:
        logger.error("No valid exporters configured")
        sys.exit(1)
    
    # Start shared node collector background task if it exists
    if shared_node_collector:
        def node_collection_worker():
            """Background task to collect node metrics"""
            logger.info("Starting shared node collector background task")
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            async def collection_loop():
                while True:
                    try:
                        await shared_node_collector.collect_all()
                    except Exception as e:
                        logger.error(f"Node collection error: {e}")
                    await asyncio.sleep(collect_interval)
            
            try:
                loop.run_until_complete(collection_loop())
            finally:
                loop.close()
        
        node_thread = threading.Thread(target=node_collection_worker, daemon=True, name='shared-node-collector')
        node_thread.start()
        logger.info(f"Started shared node collector (interval: {collect_interval}s, hosts: {len(all_hosts)})")
    
    # Start background metrics cache updater
    cache_updater_thread = threading.Thread(target=update_metrics_cache, daemon=True, name='cache-updater')
    cache_updater_thread.start()
    logger.info(f"Started background metrics cache updater (interval: {cache_ttl}s)")
    
    # Wait a moment for initial cache to be ready
    time.sleep(2)
    
    # Start shared HTTP server with threading support for concurrent requests
    server = ThreadedHTTPServer(('', exporter_port), CachedMetricsHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True, name='http-server')
    server_thread.start()
    logger.info(f"Shared exporter HTTP server started on port {exporter_port} (threaded, pre-cached with gzip compression)")
    logger.info(f"All {len(threads)} exporter(s) running. Press Ctrl+C to stop.")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
        sys.exit(0)


def run_exporter_from_config(exporter_type: str, config: dict, log_level: str, 
                              shared_registry, shared_node_collector, 
                              exporter_port: int, collect_interval: float, max_concurrent: int, region: str = None,
                              ssh_username: str = 'root', ssh_key_file: str = None, ssh_port: int = 22,
                              initialized_types: set = None, initialized_types_lock = None):
    """Run a single exporter with shared registry and node collector"""
    logger = logging.getLogger(f"{exporter_type}_exporter")
    
    # For tracking if this is the first instance of this type
    is_first_instance = False
    if initialized_types is not None and initialized_types_lock is not None:
        with initialized_types_lock:
            if exporter_type not in initialized_types:
                initialized_types.add(exporter_type)
                is_first_instance = True
    
    try:
        if exporter_type == 'node':
            # Node exporter doesn't need to run separately if using shared collector
            if shared_node_collector:
                logger.info("Using shared node collector, no separate node exporter needed")
                # Just keep the thread alive for collection
                import asyncio
                async def node_collection_loop():
                    while True:
                        try:
                            await shared_node_collector.collect_all()
                        except Exception as e:
                            logger.error(f"Node collection error: {e}")
                        await asyncio.sleep(collect_interval)
                
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(node_collection_loop())
            else:
                logger.warning("No node collector configured")
        
        elif exporter_type == 'arcus':
            from arcus_exporter import ArcusPrometheusExporter
            from prometheus_wrapper import MetricFactory
            
            # Create event loop for this thread (needed by Kazoo)
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            default_labels = {}
            if region:
                default_labels['region'] = region
            # Don't add cloud_name to default_labels - it will be added per-instance
            
            # Create metric factory with shared registry
            metric_factory = MetricFactory(registry=shared_registry, default_labels=default_labels)
            
            logger.warning(f"[MAIN] Creating Arcus exporter with shared_node_collector = {shared_node_collector}, type = {type(shared_node_collector)}")
            
            exporter = ArcusPrometheusExporter(
                zookeeper_addr=config.get('zookeeper_addr'),
                cloud_name=config.get('cloud_name', '.*'),
                exporter_port=exporter_port,
                collect_interval=collect_interval,
                default_labels=default_labels,
                max_concurrent=max_concurrent,
                enable_node_metrics=False,  # Disabled, using shared collector
                exclude_k8s_node=True,
                ssh_username=ssh_username,  # Use global SSH username
                ssh_key_file=ssh_key_file,  # Use global SSH key file
                ssh_port=ssh_port,  # Use global SSH port
                shared_node_collector=shared_node_collector  # Pass shared node collector
            )
            
            logger.warning(f"[MAIN] After creation, exporter.shared_node_collector = {exporter.shared_node_collector}")
            
            # Replace registry with shared one
            exporter.registry = shared_registry
            exporter.metric_factory = metric_factory
            exporter.node_collector = shared_node_collector
            exporter.shared_node_collector = shared_node_collector  # Ensure it's set after registry replacement
            
            # Initialize metrics with shared registry
            exporter._init_metrics()
            
            # Don't start HTTP server (already started)
            exporter._running = True
            exporter._start_collection_threads()
            
            # Keep thread alive
            while exporter._running:
                time.sleep(1)
        
        elif exporter_type == 'memcached':
            from memcached_exporter import MemcachedPrometheusExporter
            from prometheus_wrapper import MetricFactory
            
            default_labels = {}
            if region:
                default_labels['region'] = region
            
            # Create metric factory with shared registry
            metric_factory = MetricFactory(registry=shared_registry, default_labels=default_labels)
            
            exporter = MemcachedPrometheusExporter(
                memcached_addrs=config.get('memcached_addrs', []),
                exporter_port=exporter_port,
                collect_interval=collect_interval,
                default_labels=default_labels,
                max_concurrent=max_concurrent,
                enable_node_metrics=False,  # Disabled, using shared collector
                exclude_k8s_node=True,
                ssh_username=ssh_username,  # Use global SSH username
                ssh_key_file=ssh_key_file,  # Use global SSH key file
                ssh_port=ssh_port  # Use global SSH port
            )
            
            # Replace registry with shared one
            exporter.registry = shared_registry
            exporter.metric_factory = metric_factory
            exporter.node_collector = shared_node_collector
            
            # Initialize metrics with shared registry
            exporter._init_metrics()
            
            # Don't start HTTP server (already started)
            exporter._running = True
            exporter._start_collection_threads()
            
            # Keep thread alive
            while exporter._running:
                time.sleep(1)
        
        elif exporter_type == 'redis':
            from redis_exporter import RedisPrometheusExporter
            from prometheus_wrapper import MetricFactory
            
            default_labels = {}
            if region:
                default_labels['region'] = region
            
            # Create metric factory with shared registry
            metric_factory = MetricFactory(registry=shared_registry, default_labels=default_labels)
            
            exporter = RedisPrometheusExporter(
                redis_addrs=config.get('redis_addrs', []),
                redis_password=config.get('redis_password'),
                cluster_name=config.get('cluster_name'),
                cluster_refresh_interval=config.get('cluster_refresh_interval', 60.0),
                exporter_port=exporter_port,
                collect_interval=collect_interval,
                default_labels=default_labels,
                enable_node_metrics=False,  # Disabled, using shared collector
                exclude_k8s_node=True,
                ssh_username=ssh_username,  # Use global SSH username
                ssh_key_file=ssh_key_file,  # Use global SSH key file
                ssh_port=ssh_port  # Use global SSH port
            )
            
            # Replace registry with shared one
            exporter.registry = shared_registry
            exporter.metric_factory = metric_factory
            exporter.node_collector = shared_node_collector
            
            # Initialize metrics with shared registry
            # The _init_metrics() method has try-except to handle duplicate registration
            exporter._init_metrics()
            
            # Don't start HTTP server (already started)
            exporter._running = True
            if exporter.cluster_mode:
                import asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(exporter._discover_cluster_nodes())
                loop.close()
                
                exporter._cluster_refresh_thread = threading.Thread(
                    target=exporter._cluster_refresh_worker, daemon=True
                )
                exporter._cluster_refresh_thread.start()
            else:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(exporter._update_clients(exporter.redis_addrs))
                loop.close()
            
            exporter._collection_thread = threading.Thread(
                target=exporter._collection_worker, daemon=True
            )
            exporter._collection_thread.start()
            
            # Keep thread alive
            while exporter._running:
                time.sleep(1)
    
    except Exception as e:
        logger.error(f"Exporter error: {e}", exc_info=True)


def run_node_exporter_from_config(config: dict, log_level: str):
    """Run node exporter from config dict"""
    from node_exporter import run_node_exporter
    
    args = []
    
    if 'hosts' in config:
        args.extend(['--hosts'] + config['hosts'])
    if 'username' in config:
        args.extend(['--username', config['username']])
    if 'key_file' in config:
        args.extend(['--key-file', config['key_file']])
    if 'password' in config:
        args.extend(['--password', config['password']])
    if 'port' in config:
        args.extend(['--port', str(config['port'])])
    if 'collect_interval' in config:
        args.extend(['--collect-interval', str(config['collect_interval'])])
    
    args.extend(['--log-level', log_level])
    
    run_node_exporter(args)


def run_arcus_exporter_from_config(config: dict, log_level: str):
    """Run arcus exporter from config dict"""
    from arcus_exporter import run_arcus_exporter
    
    args = []
    
    if 'zookeeper_addr' in config:
        args.extend(['--zookeeper-addr', config['zookeeper_addr']])
    if 'cloud_name' in config:
        args.extend(['--cloud-name', config['cloud_name']])
    if 'memcached_addrs' in config:
        args.extend(['--memcached-addrs'] + config['memcached_addrs'])
    if 'collect_interval' in config:
        args.extend(['--collect-interval', str(config['collect_interval'])])
    if 'max_concurrent' in config:
        args.extend(['--max-concurrent', str(config['max_concurrent'])])
    if 'environment' in config:
        args.extend(['--environment', config['environment']])
    if 'region' in config:
        args.extend(['--region', config['region']])
    
    # Node metrics settings
    if config.get('enable_node_metrics', False):
        args.append('--enable-node-metrics')
    
    if config.get('include_k8s_node', False):
        args.append('--include-k8s-node')
    
    if 'ssh_username' in config:
        args.extend(['--ssh-username', config['ssh_username']])
    if 'ssh_key_file' in config:
        args.extend(['--ssh-key-file', config['ssh_key_file']])
    if 'ssh_password' in config:
        args.extend(['--ssh-password', config['ssh_password']])
    if 'ssh_port' in config:
        args.extend(['--ssh-port', str(config['ssh_port'])])
    
    args.extend(['--log-level', log_level])
    
    run_arcus_exporter(args)


def run_redis_exporter_from_config(config: dict, log_level: str):
    """Run redis exporter from config dict"""
    from redis_exporter import run_redis_exporter
    
    args = []
    
    if 'redis_addrs' in config:
        args.extend(['--redis-addrs'] + config['redis_addrs'])
    if 'redis_password' in config:
        args.extend(['--redis-password', config['redis_password']])
    if 'cluster_name' in config:
        args.extend(['--cluster-name', config['cluster_name']])
    if 'cluster_refresh_interval' in config:
        args.extend(['--cluster-refresh-interval', str(config['cluster_refresh_interval'])])
    if 'collect_interval' in config:
        args.extend(['--collect-interval', str(config['collect_interval'])])
    if 'environment' in config:
        args.extend(['--environment', config['environment']])
    if 'region' in config:
        args.extend(['--region', config['region']])
    
    # Node metrics settings
    if config.get('enable_node_metrics', False):
        args.append('--enable-node-metrics')
    
    if config.get('include_k8s_node', False):
        args.append('--include-k8s-node')
    
    if 'ssh_username' in config:
        args.extend(['--ssh-username', config['ssh_username']])
    if 'ssh_key_file' in config:
        args.extend(['--ssh-key-file', config['ssh_key_file']])
    if 'ssh_password' in config:
        args.extend(['--ssh-password', config['ssh_password']])
    if 'ssh_port' in config:
        args.extend(['--ssh-port', str(config['ssh_port'])])
    
    args.extend(['--log-level', log_level])
    
    run_redis_exporter(args)


if __name__ == '__main__':
    main()
