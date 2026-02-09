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
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


def main():
    """Main entry point with subcommands"""
    parser = argparse.ArgumentParser(
        description='Remote Exporter - Unified metrics collection tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect node metrics via SSH
  %(prog)s node --hosts 192.168.1.10 192.168.1.11 --username root --key-file ~/.ssh/id_rsa
  
  # Collect arcus/memcached metrics from ZooKeeper
  %(prog)s arcus --zookeeper-addr localhost:2181 --cloud-name "prod.*"
  
  # Collect arcus metrics with node metrics enabled
  %(prog)s arcus --zookeeper-addr localhost:2181 --enable-node-metrics --ssh-username root
  
  # Collect Redis metrics
  %(prog)s redis --redis-addrs localhost:6379
  
  # Collect Redis Cluster metrics with auto-discovery
  %(prog)s redis --redis-addrs seed1:6379 seed2:6379 --cluster-mode
  
  # Run multiple exporters from config file
  %(prog)s conf --config config.yaml
        """
    )
    
    # Common arguments
    parser.add_argument('--exporter-port', type=int, default=9150,
                       help='Exporter HTTP port (default: 9150)')
    parser.add_argument('--collect-interval', type=float, default=15.0,
                       help='Metrics collection interval in seconds (default: 15.0)')
    parser.add_argument('--max-concurrent', type=int, default=10,
                       help='Maximum concurrent instance collections (default: 10)')
    parser.add_argument('--ssh-username', default='root',
                       help='SSH username for node metrics collection (default: root)')
    parser.add_argument('--ssh-key-file',
                       help='SSH private key file path for node metrics collection')
    parser.add_argument('--ssh-port', type=int, default=22,
                       help='SSH port for node metrics collection (default: 22)')
    parser.add_argument('--region',
                       help='Region label for metrics')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Log level (default: INFO)')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    subparsers.required = True
    
    # Node exporter subcommand
    node_parser = subparsers.add_parser('node', 
                                         help='Remote node metrics collector via SSH')
    node_parser.add_argument('--hosts', nargs='+', required=True,
                            help='Remote hosts to monitor')
    node_parser.add_argument('--username', required=True,
                            help='SSH username')
    node_parser.add_argument('--key-file', 
                            help='SSH private key file')
    node_parser.add_argument('--ssh-port', type=int, default=22,
                            help='SSH port (default: 22)')
    
    # Arcus exporter subcommand
    arcus_parser = subparsers.add_parser('arcus',
                                          help='Arcus cache metrics collector via ZooKeeper')
    arcus_parser.add_argument('--zookeeper-addr', required=True,
                             help='ZooKeeper address')
    arcus_parser.add_argument('--cloud-name', default='.*',
                             help='Cloud name pattern (regex, default: .* for all clouds)')
    
    # Node metrics via SSH for arcus
    arcus_parser.add_argument('--enable-node-metrics', action='store_true',
                             help='Enable node metrics collection via SSH (default: False)')
    arcus_parser.add_argument('--include-k8s-node', action='store_true',
                             help='Include Kubernetes nodes in node metrics collection (default: False)')
    
    # Memcached exporter subcommand
    memcached_parser = subparsers.add_parser('memcached',
                                              help='Memcached metrics collector')
    memcached_parser.add_argument('--memcached-addrs', nargs='+', required=True,
                                  help='Memcached server addresses in format host:port')
    
    # Node metrics via SSH for memcached
    memcached_parser.add_argument('--enable-node-metrics', action='store_true',
                                  help='Enable node metrics collection via SSH (default: False)')
    memcached_parser.add_argument('--include-k8s-node', action='store_true',
                                  help='Include Kubernetes nodes in node metrics collection (default: False)')
    
    # Redis exporter subcommand
    redis_parser = subparsers.add_parser('redis',
                                          help='Redis/Redis Cluster metrics collector')
    redis_parser.add_argument('--redis-addrs', nargs='+', required=True,
                             help='Redis server addresses in format host:port')
    redis_parser.add_argument('--redis-password',
                             help='Redis password')
    redis_parser.add_argument('--cluster-name',
                             help='Redis Cluster name (enables cluster mode with automatic node discovery)')
    redis_parser.add_argument('--cluster-refresh-interval', type=float, default=60.0,
                             help='Cluster node refresh interval in seconds (default: 60)')
    
    # Node metrics via SSH for redis
    redis_parser.add_argument('--enable-node-metrics', action='store_true',
                             help='Enable node metrics collection via SSH (default: False)')
    redis_parser.add_argument('--include-k8s-node', action='store_true',
                             help='Include Kubernetes nodes in node metrics collection (default: False)')
    
    # Conf subcommand - run multiple exporters from YAML config
    conf_parser = subparsers.add_parser('conf',
                                         help='Run multiple exporters from YAML configuration file')
    conf_parser.add_argument('--config', '-c', required=True,
                            help='Path to YAML configuration file')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Route to appropriate handler
    if args.command == 'node':
        from node_exporter import run_node_exporter
        # Convert args to list for node parser
        node_args = []
        node_args.extend(['--hosts'] + args.hosts)
        node_args.extend(['--username', args.username])
        if args.key_file:
            node_args.extend(['--key-file', args.key_file])
        node_args.extend(['--ssh-port', str(args.ssh_port)])
        node_args.extend(['--exporter-port', str(args.exporter_port)])
        node_args.extend(['--collect-interval', str(args.collect_interval)])
        node_args.extend(['--log-level', args.log_level])
        
        run_node_exporter(node_args)
        
    elif args.command == 'arcus':
        from arcus_exporter import run_arcus_exporter
        # Convert args to list for arcus parser
        arcus_args = []
        arcus_args.extend(['--zookeeper-addr', args.zookeeper_addr])
        arcus_args.extend(['--cloud-name', args.cloud_name])
        arcus_args.extend(['--exporter-port', str(args.exporter_port)])
        arcus_args.extend(['--collect-interval', str(args.collect_interval)])
        arcus_args.extend(['--max-concurrent', str(args.max_concurrent)])
        arcus_args.extend(['--log-level', args.log_level])
        if args.region:
            arcus_args.extend(['--region', args.region])
        # Handle node metrics flags
        if args.enable_node_metrics:
            arcus_args.append('--enable-node-metrics')
        if args.include_k8s_node:
            arcus_args.append('--include-k8s-node')
        arcus_args.extend(['--ssh-username', args.ssh_username])
        if args.ssh_key_file:
            arcus_args.extend(['--ssh-key-file', args.ssh_key_file])
        arcus_args.extend(['--ssh-port', str(args.ssh_port)])
        
        run_arcus_exporter(arcus_args)
    
    elif args.command == 'memcached':
        from memcached_exporter import run_memcached_exporter
        # Convert args to list for memcached parser
        memcached_args = []
        memcached_args.extend(['--memcached-addrs'] + args.memcached_addrs)
        memcached_args.extend(['--exporter-port', str(args.exporter_port)])
        memcached_args.extend(['--collect-interval', str(args.collect_interval)])
        memcached_args.extend(['--max-concurrent', str(args.max_concurrent)])
        memcached_args.extend(['--log-level', args.log_level])
        if args.region:
            memcached_args.extend(['--region', args.region])
        # Handle node metrics flags
        if args.enable_node_metrics:
            memcached_args.append('--enable-node-metrics')
        if args.include_k8s_node:
            memcached_args.append('--include-k8s-node')
        memcached_args.extend(['--ssh-username', args.ssh_username])
        if args.ssh_key_file:
            memcached_args.extend(['--ssh-key-file', args.ssh_key_file])
        memcached_args.extend(['--ssh-port', str(args.ssh_port)])
        
        run_memcached_exporter(memcached_args)
    
    elif args.command == 'redis':
        from redis_exporter import run_redis_exporter
        # Convert args to list for redis parser
        redis_args = []
        redis_args.extend(['--redis-addrs'] + args.redis_addrs)
        if args.redis_password:
            redis_args.extend(['--redis-password', args.redis_password])
        if args.cluster_name:
            redis_args.extend(['--cluster-name', args.cluster_name])
        redis_args.extend(['--cluster-refresh-interval', str(args.cluster_refresh_interval)])
        redis_args.extend(['--exporter-port', str(args.exporter_port)])
        redis_args.extend(['--collect-interval', str(args.collect_interval)])
        redis_args.extend(['--log-level', args.log_level])
        if args.region:
            redis_args.extend(['--region', args.region])
        # Handle node metrics flags
        if args.enable_node_metrics:
            redis_args.append('--enable-node-metrics')
        if args.include_k8s_node:
            redis_args.append('--include-k8s-node')
        redis_args.extend(['--ssh-username', args.ssh_username])
        if args.ssh_key_file:
            redis_args.extend(['--ssh-key-file', args.ssh_key_file])
        redis_args.extend(['--ssh-port', str(args.ssh_port)])
        
        run_redis_exporter(redis_args)
    
    elif args.command == 'conf':
        if not YAML_AVAILABLE:
            print("Error: PyYAML is not installed. Install it with: pip install pyyaml", file=sys.stderr)
            sys.exit(1)
        
        run_from_config(args.config, args.exporter_port, args.collect_interval, args.max_concurrent,
                       args.ssh_username, args.ssh_key_file, args.ssh_port, args.region, args.log_level)
    
    else:
        parser.print_help()
        sys.exit(1)


def run_from_config(config_file: str, cli_exporter_port: int = None, cli_collect_interval: float = None, 
                     cli_max_concurrent: int = None, cli_ssh_username: str = None, 
                     cli_ssh_key_file: str = None, cli_ssh_port: int = None, cli_region: str = None, 
                     log_level: str = 'INFO'):
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
    exporter_port = cli_exporter_port if cli_exporter_port is not None else config.get('exporter_port', 9150)
    collect_interval = cli_collect_interval if cli_collect_interval is not None else config.get('collect_interval', 15.0)
    max_concurrent = cli_max_concurrent if cli_max_concurrent is not None else config.get('max_concurrent', 10)
    
    # Get SSH settings from CLI or config (CLI takes precedence)
    ssh_username = cli_ssh_username if cli_ssh_username is not None else config.get('ssh_username', 'root')
    ssh_key_file = cli_ssh_key_file if cli_ssh_key_file is not None else config.get('ssh_key_file')
    ssh_port = cli_ssh_port if cli_ssh_port is not None else config.get('ssh_port', 22)
    
    # Get region from CLI or config
    region = cli_region if cli_region is not None else config.get('region')
    
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
        while True:
            try:
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
                
                logger.debug(f"Metrics cache updated: {len(metrics_data)} bytes raw, {len(compressed_data)} bytes compressed ({100*len(compressed_data)/len(metrics_data):.1f}% ratio)")
                
            except Exception as e:
                logger.error(f"Error updating metrics cache: {e}")
            
            time.sleep(cache_ttl)
    
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
                
                except Exception as e:
                    logger.error(f"Error serving metrics: {e}")
                    self.send_error(500, f"Internal Server Error: {e}")
            else:
                self.send_error(404, "Not Found")
    
    # Collect all unique hosts for node metrics across all exporters
    all_hosts = set()
    include_k8s_node = False
    
    # Parse exporters and collect host information
    exporter_instances = []
    for idx, exporter_config in enumerate(exporters):
        if 'type' not in exporter_config:
            logger.warning(f"Skipping exporter {idx}: missing 'type' field")
            continue
        
        exporter_type = exporter_config['type']
        
        # Collect hosts if node metrics enabled
        if exporter_config.get('enable_node_metrics', False):
            if exporter_type == 'arcus' and 'memcached_addrs' in exporter_config:
                for addr in exporter_config['memcached_addrs']:
                    host = addr.split(':')[0]
                    all_hosts.add(host)
            elif exporter_type == 'redis' and 'redis_addrs' in exporter_config:
                for addr in exporter_config['redis_addrs']:
                    host = addr.split(':')[0]
                    all_hosts.add(host)
            elif exporter_type == 'node' and 'hosts' in exporter_config:
                all_hosts.update(exporter_config['hosts'])
            
            # Check if any exporter wants to include K8s nodes
            if exporter_config.get('include_k8s_node', False):
                include_k8s_node = True
        
        exporter_instances.append((exporter_type, exporter_config, idx))
    
    # Create shared node collector if we have hosts
    shared_node_collector = None
    if all_hosts and ssh_key_file:
        logger.info(f"Creating shared node collector for {len(all_hosts)} unique hosts")
        from node_exporter import RemoteNodeCollector
        from redis_exporter import is_k8s_node
        import asyncio
        
        # Filter K8s nodes if needed (exclude by default unless include_k8s_node is True)
        if not include_k8s_node:
            async def filter_k8s_nodes():
                filtered = set()
                for host in all_hosts:
                    is_k8s = await is_k8s_node(
                        host,
                        ssh_username,
                        ssh_key_file,
                        ssh_port
                    )
                    if not is_k8s:
                        filtered.add(host)
                    else:
                        logger.info(f"Excluding K8s node: {host}")
                return filtered
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            all_hosts = loop.run_until_complete(filter_k8s_nodes())
            loop.close()
        
        if all_hosts:
            shared_node_collector = RemoteNodeCollector(
                hosts=list(all_hosts),
                username=ssh_username,
                key_file=ssh_key_file,
                port=ssh_port
            )
            shared_node_collector.registry = shared_registry
            logger.info(f"Shared node collector created for {len(all_hosts)} hosts")
    elif all_hosts and not ssh_key_file:
        logger.warning(f"Found {len(all_hosts)} hosts but no SSH key file specified - node metrics disabled")
    
    # Track which exporter types have already initialized metrics
    initialized_types = set()
    initialized_types_lock = threading.Lock()
    
    # Start all exporters with shared registry and node collector
    threads = []
    for exporter_type, exporter_config, idx in exporter_instances:
        thread = threading.Thread(
            target=run_exporter_from_config,
            args=(exporter_type, exporter_config, log_level, shared_registry, shared_node_collector, exporter_port, collect_interval, max_concurrent, region, initialized_types, initialized_types_lock),
            daemon=True,
            name=f"{exporter_type}-exporter-{idx}"
        )
        threads.append(thread)
        thread.start()
        logger.info(f"Started {exporter_type} exporter (thread: {thread.name})")
    
    if not threads:
        logger.error("No valid exporters configured")
        sys.exit(1)
    
    # Start background metrics cache updater
    cache_updater_thread = threading.Thread(target=update_metrics_cache, daemon=True, name='cache-updater')
    cache_updater_thread.start()
    logger.info(f"Started background metrics cache updater (interval: {cache_ttl}s)")
    
    # Wait a moment for initial cache to be ready
    time.sleep(2)
    
    # Start shared HTTP server with caching and compression
    server = HTTPServer(('', exporter_port), CachedMetricsHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True, name='http-server')
    server_thread.start()
    logger.info(f"Shared exporter HTTP server started on port {exporter_port} (pre-cached with gzip compression)")
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
            
            exporter = ArcusPrometheusExporter(
                zookeeper_addr=config.get('zookeeper_addr'),
                cloud_name=config.get('cloud_name', '.*'),
                exporter_port=exporter_port,
                collect_interval=collect_interval,
                default_labels=default_labels,
                max_concurrent=max_concurrent,
                enable_node_metrics=False,  # Disabled, using shared collector
                exclude_k8s_node=True,
                ssh_username=config.get('ssh_username', 'root'),
                ssh_key_file=config.get('ssh_key_file'),
                ssh_port=config.get('ssh_port', 22)
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
                ssh_username=config.get('ssh_username', 'root'),
                ssh_key_file=config.get('ssh_key_file'),
                ssh_port=config.get('ssh_port', 22)
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
                ssh_username=config.get('ssh_username', 'root'),
                ssh_key_file=config.get('ssh_key_file'),
                ssh_port=config.get('ssh_port', 22)
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
