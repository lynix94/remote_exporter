# Remote Exporter

A unified Prometheus exporter for collecting metrics from remote nodes, Memcached/Arcus, and Redis servers.

## Features

### 1. Node Mode
Collect system metrics from remote servers via SSH without requiring node_exporter installation on target servers.

- 40+ metrics collected (CPU, memory, disk, network, etc.)
- Directly reads `/proc/*` files to generate node_exporter-compatible metrics
- Fast performance with async parallel collection

### 2. Memcached Mode
Collect metrics from Memcached servers with static address configuration.

- Static server address configuration
- Standard memcached metrics (connections, cache, memory, operations)
- Optional node metrics collection via SSH
- Async parallel collection

### 3. Arcus Mode
Collect metrics from Arcus cache servers with ZooKeeper-based service discovery.

- Automatic server discovery via ZooKeeper integration
- Cloud filtering with regex patterns
- Arcus-specific collection metrics (LOP, SOP, BOP, MOP)
- Extends Memcached exporter with Arcus-specific metrics
- Optional node metrics collection via SSH

### 4. Redis Mode
Collect metrics from Redis and Redis Cluster.

- Single Redis instance or Redis Cluster support
- Automatic cluster node discovery with periodic refresh
- Optional node metrics collection via SSH (default: disabled)
- 30+ Redis metrics (memory, clients, stats, replication, etc.)
- Async parallel collection

### 5. Conf Mode
Run multiple exporters simultaneously from a YAML configuration file.

- Manage multiple Node, Memcached, Arcus, and Redis exporters from a single config file
- Each exporter runs in an independent thread
- **Shared registry**: All exporters expose metrics on a single HTTP port
- **Unique node collection**: Node metrics are collected once for all unique hosts
- Centralized configuration for operational convenience
- Global common parameters (port, interval, SSH credentials, region)

## Installation

```bash
# Local installation
pip install -e .

# Or install requirements only
pip install -r requirements.txt
```

## Usage

### Node Mode

```bash
# Basic usage
remote_exporter node \
    --hosts 192.168.1.10 192.168.1.11 \
    --username root \
    --key-file ~/.ssh/id_rsa

# With port and interval settings
remote_exporter node \
    --hosts 192.168.1.10 \
    --username myuser \
    --key-file ~/.ssh/id_rsa \
    --exporter-port 9100 \
    --collect-interval 15 \
    --log-level DEBUG
```

**Parameters:**
- `--hosts`: Remote hosts to monitor (required, multiple allowed)
- `--username`: SSH username (required)
- `--key-file`: SSH private key file path
- `--ssh-port`: SSH port (default: 22)
- `--exporter-port`: Exporter HTTP port (default: 9100)
- `--collect-interval`: Collection interval in seconds (default: 15)
- `--log-level`: Log level (DEBUG|INFO|WARNING|ERROR, default: INFO)

### Memcached Mode

```bash
# Static memcached addresses
remote_exporter memcached \
    --memcached-addrs 192.168.1.10:11211 192.168.1.11:11211 \
    --exporter-port 9150

# With node metrics
remote_exporter memcached \
    --memcached-addrs 10.0.1.10:11211 10.0.1.11:11211 \
    --enable-node-metrics \
    --ssh-username root \
    --ssh-key-file ~/.ssh/id_rsa
```

**Parameters:**
- `--memcached-addrs`: Memcached server addresses (required, host:port format)
- `--exporter-port`: Exporter HTTP port (default: 9150)
- `--collect-interval`: Collection interval in seconds (default: 30.0)
- `--max-concurrent`: Maximum concurrent instance collections (default: 10)
- `--region`: Region label for metrics
- `--enable-node-metrics`: Enable node metrics collection via SSH (default: False)
- `--include-k8s-node`: Include Kubernetes nodes (default: False, excludes K8s nodes)
- `--ssh-username`: SSH username (default: root)
- `--ssh-key-file`: SSH private key file path
- `--ssh-port`: SSH port (default: 22)
- `--log-level`: Log level

### Arcus Mode

```bash
# Using ZooKeeper
remote_exporter arcus \
    --zookeeper-addr localhost:2181 \
    --cloud-name "prod.*"

# With node metrics
remote_exporter arcus \
    --zookeeper-addr localhost:2181 \
    --cloud-name ".*" \
    --enable-node-metrics \
    --ssh-username root \
    --ssh-key-file ~/.ssh/id_rsa

# Including K8s nodes
remote_exporter arcus \
    --zookeeper-addr localhost:2181 \
    --cloud-name "test-cloud" \
    --include-k8s-node
```

**Parameters:**
- `--zookeeper-addr`: ZooKeeper address (required)
- `--cloud-name`: Cloud name pattern (regex, default: .*)
- `--exporter-port`: Exporter HTTP port (default: 9150)
- `--collect-interval`: Collection interval in seconds (default: 3.0)
- `--max-concurrent`: Maximum concurrent instance collections (default: 10)
- `--region`: Region label for metrics
- `--enable-node-metrics`: Enable node metrics collection via SSH (default: False)
- `--include-k8s-node`: Include Kubernetes nodes (default: False, excludes K8s nodes)
- `--ssh-username`: SSH username (default: root)
- `--ssh-key-file`: SSH private key file path
- `--ssh-port`: SSH port (default: 22)
- `--log-level`: Log level

### Redis Mode

```bash
# Single Redis
remote_exporter redis \
    --redis-addrs localhost:6379

# Redis Cluster (automatic node discovery)
remote_exporter redis \
    --redis-addrs seed1:6379 seed2:6379 \
    --cluster-mode

# With node metrics
remote_exporter redis \
    --redis-addrs localhost:6379 \
    --enable-node-metrics \
    --ssh-username root \
    --ssh-key-file ~/.ssh/id_rsa

# Including K8s nodes (default excludes them)
remote_exporter redis \
    --redis-addrs localhost:6379 \
    --enable-node-metrics \
    --include-k8s-node
```

**Parameters:**
- `--redis-addrs`: Redis server addresses (required, host:port format)
- `--redis-password`: Redis password
- `--cluster-mode`: Enable Redis Cluster mode
- `--cluster-refresh-interval`: Cluster node refresh interval in seconds (default: 60)
- `--exporter-port`: Exporter HTTP port (default: 9121)
- `--collect-interval`: Collection interval in seconds (default: 5.0)
- `--region`: Region label for metrics
- `--enable-node-metrics`: Enable node metrics collection via SSH (default: False)
- `--include-k8s-node`: Include Kubernetes nodes (default: False, excludes K8s nodes)
- `--ssh-username`: SSH username (default: root)
- `--ssh-key-file`: SSH private key file path
- `--ssh-port`: SSH port (default: 22)
- `--log-level`: Log level

### Conf Mode (YAML Configuration)

```bash
# Run multiple exporters from YAML config
remote_exporter conf --config config.yaml

# With CLI overrides
remote_exporter conf --config config.yaml \
    --exporter-port 9200 \
    --region us-west-2 \
    --ssh-username ubuntu \
    --ssh-key-file ~/.ssh/aws_key.pem
```

**Configuration Example (`config.yaml`):**

```yaml
# Global settings (shared by all exporters)
exporter_port: 9150        # Single HTTP port for all metrics
collect_interval: 15.0     # Default collection interval in seconds
max_concurrent: 10         # Maximum concurrent collections

# Global SSH settings (used for all node metrics collection)
ssh_username: root         # SSH username for node metrics
ssh_key_file: /home/user/.ssh/id_rsa  # SSH private key file
ssh_port: 22               # SSH port

# Global region label (applied to all metrics)
region: us-west-2          # Region label for all exporters

exporters:
  # Example 1: Node exporter
  - type: node
    hosts:
      - 192.168.1.10
      - 192.168.1.11
    username: root
    key_file: /home/user/.ssh/id_rsa
    ssh_port: 22

  # Example 2: Arcus exporter - ZooKeeper discovery
  - type: arcus
    zookeeper_addr: localhost:2181
    cloud_name: "prod.*"
    enable_node_metrics: true
    include_k8s_node: false

  # Example 3: Memcached exporter - static addresses
  - type: memcached
    memcached_addrs:
      - 10.0.1.10:11211
      - 10.0.1.11:11211
    enable_node_metrics: true

  # Example 4: Redis standalone
  - type: redis
    redis_addrs:
      - localhost:6379
    redis_password: myredispassword
    enable_node_metrics: true

  # Example 5: Redis Cluster with auto-discovery
  - type: redis
    redis_addrs:
      - redis-cluster-seed1:6379
      - redis-cluster-seed2:6379
    cluster_mode: true
    cluster_refresh_interval: 60.0
    enable_node_metrics: true
    include_k8s_node: true
```

See `config.sample.yaml` for detailed configuration examples.

**Parameters:**
- `--config, -c`: YAML configuration file path (required)
- `--exporter-port`: Override global exporter port
- `--collect-interval`: Override global collect interval
- `--max-concurrent`: Override global max concurrent
- `--ssh-username`: Override global SSH username
- `--ssh-key-file`: Override global SSH key file
- `--ssh-port`: Override global SSH port
- `--region`: Override global region label
- `--log-level`: Log level

**Conf Mode Features:**
1. **Shared Registry Architecture**:
   - All exporters share a single HTTP port (exporter_port)
   - All metrics from all exporters are served on one endpoint
   - Node metrics are collected once for all unique hosts

2. **Global vs Per-Exporter Settings**:
   - `exporter_port`: Global only (shared by all)
   - `collect_interval`: Global default, can be overridden per exporter
   - `max_concurrent`: Global only
   - `ssh_username`, `ssh_key_file`, `ssh_port`: Global (used for all unique nodes)
   - `region`: Global (applied to all metrics as label)
   - CLI arguments override config file settings

3. **Unique Node Collection**:
   - Hosts are automatically discovered from memcached/arcus/redis addresses
   - Duplicates are removed - each host is monitored only once
   - All unique nodes use the same SSH credentials

## Checking Metrics

Once the exporter is running, metrics can be accessed at:

- Node mode: http://localhost:9100/metrics
- Memcached mode: http://localhost:9150/metrics
- Arcus mode: http://localhost:9150/metrics
- Redis mode: http://localhost:9121/metrics
- Conf mode: http://localhost:9150/metrics (or configured port)

## Architecture

```
remote_exporter/
├── __init__.py              # Package initialization
├── __main__.py              # CLI entry point (subcommand handling)
├── node_exporter.py         # Node exporter logic with RemoteNodeCollector
├── memcached_exporter.py    # Base Memcached exporter
├── arcus_exporter.py        # Arcus exporter (extends Memcached)
├── redis_exporter.py        # Redis exporter logic
├── prometheus_wrapper.py    # Prometheus metrics wrapper
├── setup.py                 # pip package configuration
├── requirements.txt         # Dependencies
├── config.sample.yaml       # Sample configuration for conf mode
└── README.md                # Documentation
```

## Using as a Library

```python
from node_exporter import run_node_exporter
from memcached_exporter import run_memcached_exporter
from arcus_exporter import run_arcus_exporter
from redis_exporter import run_redis_exporter

# Run Node exporter
run_node_exporter(['--hosts', '192.168.1.10', '--username', 'root'])

# Run Memcached exporter
run_memcached_exporter(['--memcached-addrs', '10.0.1.10:11211'])

# Run Arcus exporter
run_arcus_exporter(['--zookeeper-addr', 'localhost:2181'])

# Run Redis exporter
run_redis_exporter(['--redis-addrs', 'localhost:6379', '--cluster-mode'])
```

## Node Metrics Collection

Memcached, Arcus, and Redis exporters can collect system metrics from target hosts via SSH.

### Default Behavior
- **Disabled by default**: Node metrics are not collected unless explicitly enabled
- **K8s nodes excluded**: Kubernetes nodes are automatically excluded to prevent duplicate collection

### K8s Node Detection
K8s nodes are detected using three methods:
1. Check for `/var/lib/kubelet` directory
2. Check for `kubelet` process
3. Check for `/etc/kubernetes` directory

### Usage Examples

```bash
# Enable node metrics (K8s nodes excluded by default)
remote_exporter arcus --zookeeper-addr localhost:2181 --enable-node-metrics

# Include K8s nodes
remote_exporter arcus --zookeeper-addr localhost:2181 \
    --enable-node-metrics --include-k8s-node

# Disable node metrics (default)
remote_exporter redis --redis-addrs localhost:6379
```

## Common Parameters

The following parameters are available for all exporters:

- `--exporter-port`: Exporter HTTP port (default varies by mode)
- `--collect-interval`: Collection interval in seconds (default varies by mode)
- `--max-concurrent`: Maximum concurrent collections (default: 10)
- `--ssh-username`: SSH username for node metrics (default: root)
- `--ssh-key-file`: SSH private key file path
- `--ssh-port`: SSH port (default: 22)
- `--region`: Region label for metrics
- `--log-level`: Log level (DEBUG|INFO|WARNING|ERROR, default: INFO)

## Dependencies

- `prometheus-client>=0.14.0` - Prometheus metrics generation
- `asyncssh>=2.13.0` - Async SSH client
- `kazoo>=2.8.0` - ZooKeeper client (for Arcus mode)
- `pyyaml>=6.0` - YAML config file support (for conf mode)

## Security Notes

- **SSH authentication**: Only key-based authentication is supported (password authentication has been removed for security)
- Ensure SSH keys have proper permissions (chmod 600)
- Use dedicated monitoring user with minimal permissions

## License

MIT License
