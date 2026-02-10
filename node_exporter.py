#!/usr/bin/env python3
"""
Node Exporter - SSH-based remote node metrics collector

Collects system metrics from remote hosts via SSH without requiring
node_exporter installation.
SSH를 통해 원격 서버의 시스템 메트릭을 수집합니다.
node_exporter 수준의 다양한 메트릭을 지원합니다.
"""

import asyncio
import asyncssh
import re
import argparse
import logging
import sys
import time
from typing import Dict, List, Optional, Tuple
from prometheus_client import CollectorRegistry, Counter, Gauge, Info, start_http_server, generate_latest


class RemoteNodeCollector:
    """을 통한 원격 노드 메트릭 수집"""
    
    def __init__(self, hosts: List[str], username: str, key_file: str = None, 
                 port: int = 22, timeout: int = 10, registry: CollectorRegistry = None):
        self.hosts = hosts
        self.username = username
        self.key_file = key_file
        self.port = port
        self.timeout = timeout
        self.registry = registry if registry is not None else CollectorRegistry()
        self.logger = logging.getLogger(__name__)
        self.logger.warning(f"[NODE COLLECTOR INIT] username={username}, key_file={key_file}, port={port}, hosts={len(hosts)}")
        
        # Track failed hosts to avoid repeated connection attempts
        # Format: {host: last_attempt_timestamp}
        self.bad_hosts = {}
        self.bad_host_retry_interval = 3600  # 1 hour in seconds
        
        # SSH connection pool: {host: connection}
        self.connections = {}
        # Lock will be created lazily per event loop
        self._connection_lock = None
        
        self._init_metrics()
    
    def _get_lock(self):
        """Get or create lock for current event loop"""
        # Create lock lazily to avoid event loop binding issues
        if self._connection_lock is None:
            self._connection_lock = asyncio.Lock()
        return self._connection_lock
    
    def _init_metrics(self):
        """메트릭 초기화 - node_exporter 호환"""
        
        # Node info
        self.node_uname_info = Info('node_uname', 'System information',
                                    ['host'], registry=self.registry)
        
        # CPU metrics
        self.node_cpu_seconds_total = Counter('node_cpu_seconds_total',
                                              'CPU time in seconds',
                                              ['host', 'cpu', 'mode'],
                                              registry=self.registry)
        
        # Load average
        self.node_load1 = Gauge('node_load1', 'Load average 1m',
                               ['host'], registry=self.registry)
        self.node_load5 = Gauge('node_load5', 'Load average 5m',
                               ['host'], registry=self.registry)
        self.node_load15 = Gauge('node_load15', 'Load average 15m',
                                ['host'], registry=self.registry)
        
        # Memory metrics
        self.node_memory_MemTotal_bytes = Gauge('node_memory_MemTotal_bytes',
                                                'Total memory',
                                                ['host'], registry=self.registry)
        self.node_memory_MemFree_bytes = Gauge('node_memory_MemFree_bytes',
                                               'Free memory',
                                               ['host'], registry=self.registry)
        self.node_memory_MemAvailable_bytes = Gauge('node_memory_MemAvailable_bytes',
                                                    'Available memory',
                                                    ['host'], registry=self.registry)
        self.node_memory_Buffers_bytes = Gauge('node_memory_Buffers_bytes',
                                               'Buffers memory',
                                               ['host'], registry=self.registry)
        self.node_memory_Cached_bytes = Gauge('node_memory_Cached_bytes',
                                              'Cached memory',
                                              ['host'], registry=self.registry)
        self.node_memory_SwapTotal_bytes = Gauge('node_memory_SwapTotal_bytes',
                                                 'Total swap',
                                                 ['host'], registry=self.registry)
        self.node_memory_SwapFree_bytes = Gauge('node_memory_SwapFree_bytes',
                                                'Free swap',
                                                ['host'], registry=self.registry)
        
        # Filesystem metrics
        self.node_filesystem_size_bytes = Gauge('node_filesystem_size_bytes',
                                                'Filesystem size',
                                                ['host', 'device', 'mountpoint', 'fstype'],
                                                registry=self.registry)
        self.node_filesystem_avail_bytes = Gauge('node_filesystem_avail_bytes',
                                                 'Filesystem available',
                                                 ['host', 'device', 'mountpoint', 'fstype'],
                                                 registry=self.registry)
        self.node_filesystem_files = Gauge('node_filesystem_files',
                                           'Filesystem inodes total',
                                           ['host', 'device', 'mountpoint', 'fstype'],
                                           registry=self.registry)
        self.node_filesystem_files_free = Gauge('node_filesystem_files_free',
                                                'Filesystem inodes free',
                                                ['host', 'device', 'mountpoint', 'fstype'],
                                                registry=self.registry)
        
        # Disk I/O metrics
        self.node_disk_reads_completed_total = Counter('node_disk_reads_completed_total',
                                                       'Disk reads completed',
                                                       ['host', 'device'],
                                                       registry=self.registry)
        self.node_disk_writes_completed_total = Counter('node_disk_writes_completed_total',
                                                        'Disk writes completed',
                                                        ['host', 'device'],
                                                        registry=self.registry)
        self.node_disk_read_bytes_total = Counter('node_disk_read_bytes_total',
                                                  'Disk bytes read',
                                                  ['host', 'device'],
                                                  registry=self.registry)
        self.node_disk_written_bytes_total = Counter('node_disk_written_bytes_total',
                                                     'Disk bytes written',
                                                     ['host', 'device'],
                                                     registry=self.registry)
        
        # Network metrics
        self.node_network_receive_bytes_total = Counter('node_network_receive_bytes_total',
                                                        'Network bytes received',
                                                        ['host', 'device'],
                                                        registry=self.registry)
        self.node_network_transmit_bytes_total = Counter('node_network_transmit_bytes_total',
                                                         'Network bytes transmitted',
                                                         ['host', 'device'],
                                                         registry=self.registry)
        self.node_network_receive_packets_total = Counter('node_network_receive_packets_total',
                                                          'Network packets received',
                                                          ['host', 'device'],
                                                          registry=self.registry)
        self.node_network_transmit_packets_total = Counter('node_network_transmit_packets_total',
                                                           'Network packets transmitted',
                                                           ['host', 'device'],
                                                           registry=self.registry)
        self.node_network_receive_errs_total = Counter('node_network_receive_errs_total',
                                                       'Network receive errors',
                                                       ['host', 'device'],
                                                       registry=self.registry)
        self.node_network_transmit_errs_total = Counter('node_network_transmit_errs_total',
                                                        'Network transmit errors',
                                                        ['host', 'device'],
                                                        registry=self.registry)
        
        # System metrics
        self.node_boot_time_seconds = Gauge('node_boot_time_seconds',
                                           'System boot time',
                                           ['host'], registry=self.registry)
        self.node_context_switches_total = Counter('node_context_switches_total',
                                                   'Context switches',
                                                   ['host'], registry=self.registry)
        self.node_forks_total = Counter('node_forks_total',
                                        'Process forks',
                                        ['host'], registry=self.registry)
        self.node_procs_running = Gauge('node_procs_running',
                                       'Running processes',
                                       ['host'], registry=self.registry)
        self.node_procs_blocked = Gauge('node_procs_blocked',
                                       'Blocked processes',
                                       ['host'], registry=self.registry)
        
        # File descriptor metrics
        self.node_filefd_allocated = Gauge('node_filefd_allocated',
                                          'Allocated file descriptors',
                                          ['host'], registry=self.registry)
        self.node_filefd_maximum = Gauge('node_filefd_maximum',
                                        'Maximum file descriptors',
                                        ['host'], registry=self.registry)
    
    async def _run_command(self, conn, command: str, timeout: int = None) -> Optional[str]:
        """원격 명령 실행"""
        try:
            result = await asyncio.wait_for(
                conn.run(command, check=False),
                timeout=timeout or self.timeout
            )
            if result.exit_status == 0:
                return result.stdout
            return None
        except Exception as e:
            self.logger.debug(f"Command failed: {command}: {e}")
            return None    
    async def _collect_all_metrics(self, conn, host: str):
        """모든 메트릭을 한 번의 SSH 호출로 수집"""
        # Combine all commands with delimiters
        command = """
echo "===UNAME==="; uname -a
echo "===LOADAVG==="; cat /proc/loadavg
echo "===MEMINFO==="; cat /proc/meminfo
echo "===STAT==="; cat /proc/stat
echo "===DISKSTATS==="; cat /proc/diskstats
echo "===NETDEV==="; cat /proc/net/dev
echo "===FILEFD==="; cat /proc/sys/fs/file-nr
echo "===DF==="; df -B1 -T
echo "===DFI==="; df -i
"""
        output = await self._run_command(conn, command.strip(), timeout=10)
        if not output:
            return
        
        # Split output by delimiters
        sections = {}
        current_section = None
        current_data = []
        
        for line in output.split('\n'):
            if line.startswith('===') and line.endswith('==='):
                if current_section:
                    sections[current_section] = '\n'.join(current_data)
                current_section = line.strip('=')
                current_data = []
            else:
                current_data.append(line)
        
        if current_section:
            sections[current_section] = '\n'.join(current_data)
        
        # Parse each section
        if 'UNAME' in sections:
            self._parse_uname(sections['UNAME'], host)
        if 'LOADAVG' in sections:
            self._parse_loadavg(sections['LOADAVG'], host)
        if 'MEMINFO' in sections:
            self._parse_meminfo(sections['MEMINFO'], host)
        if 'STAT' in sections:
            self._parse_stat(sections['STAT'], host)
            # STAT 섹션에서 vmstat 데이터도 파싱 (ctxt, processes, procs_running 등)
            self._parse_vmstat(sections['STAT'], host)
        if 'DISKSTATS' in sections:
            self._parse_diskstats(sections['DISKSTATS'], host)
        if 'NETDEV' in sections:
            self._parse_netdev(sections['NETDEV'], host)
        if 'FILEFD' in sections:
            self._parse_filefd(sections['FILEFD'], host)
        if 'DF' in sections and 'DFI' in sections:
            self._parse_filesystem(sections['DF'], sections['DFI'], host)    
    def _parse_uname(self, output: str, host: str):
        """시스템 정보 파싱"""
        if output:
            parts = output.strip().split()
            self.node_uname_info.labels(host=host).info({
                'sysname': parts[0] if len(parts) > 0 else '',
                'release': parts[2] if len(parts) > 2 else '',
                'version': parts[3] if len(parts) > 3 else '',
                'machine': parts[-1] if len(parts) > 0 else '',
                'nodename': parts[1] if len(parts) > 1 else host
            })
    
    def _parse_stat(self, output: str, host: str):
        """CPU 및 시스템 통계 파싱"""
        if not output:
            return
        
        for line in output.strip().split('\n'):
            if line.startswith('cpu'):
                parts = line.split()
                cpu_name = parts[0]
                if cpu_name == 'cpu':
                    continue  # Skip aggregate
                
                values = [int(x) for x in parts[1:8]]
                modes = ['user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq']
                
                for mode, value in zip(modes, values):
                    # Convert from jiffies to seconds (assuming 100 HZ)
                    self.node_cpu_seconds_total.labels(
                        host=host, cpu=cpu_name, mode=mode
                    )._value._value = value / 100.0
    
    def _parse_loadavg(self, output: str, host: str):
        """Load average 파싱"""
        if output:
            loads = output.strip().split()[:3]
            self.node_load1.labels(host=host).set(float(loads[0]))
            self.node_load5.labels(host=host).set(float(loads[1]))
            self.node_load15.labels(host=host).set(float(loads[2]))
    
    def _parse_meminfo(self, output: str, host: str):
        """메모리 메트릭 파싱"""
        if not output:
            return
        
        mem_metrics = {
            'MemTotal': self.node_memory_MemTotal_bytes,
            'MemFree': self.node_memory_MemFree_bytes,
            'MemAvailable': self.node_memory_MemAvailable_bytes,
            'Buffers': self.node_memory_Buffers_bytes,
            'Cached': self.node_memory_Cached_bytes,
            'SwapTotal': self.node_memory_SwapTotal_bytes,
            'SwapFree': self.node_memory_SwapFree_bytes,
        }
        
        for line in output.strip().split('\n'):
            match = re.match(r'(\w+):\s+(\d+)', line)
            if match:
                key, value_kb = match.groups()
                if key in mem_metrics:
                    mem_metrics[key].labels(host=host).set(int(value_kb) * 1024)
    
    def _parse_filesystem(self, df_output: str, dfi_output: str, host: str):
        """파일시스템 메트릭 파싱"""
        # df with all information
        if df_output:
            for line in df_output.strip().split('\n')[1:]:
                parts = line.split()
                if len(parts) >= 7:
                    device = parts[0]
                    fstype = parts[1]
                    size = int(parts[2])
                    used = int(parts[3])
                    avail = int(parts[4])
                    mountpoint = parts[6]
                    
                    # Skip special filesystems
                    if fstype in ['tmpfs', 'devtmpfs', 'devfs', 'proc', 'sysfs']:
                        continue
                    
                    self.node_filesystem_size_bytes.labels(
                        host=host, device=device, mountpoint=mountpoint, fstype=fstype
                    ).set(size)
                    self.node_filesystem_avail_bytes.labels(
                        host=host, device=device, mountpoint=mountpoint, fstype=fstype
                    ).set(avail)
        
        # Inode information
        if dfi_output:
            for line in dfi_output.strip().split('\n')[1:]:
                parts = line.split()
                if len(parts) >= 6:
                    device = parts[0]
                    inodes = parts[1]
                    ifree = parts[3]
                    mountpoint = parts[5]
                    
                    if inodes != '-' and ifree != '-':
                        self.node_filesystem_files.labels(
                            host=host, device=device, mountpoint=mountpoint, fstype=''
                        ).set(int(inodes))
                        self.node_filesystem_files_free.labels(
                            host=host, device=device, mountpoint=mountpoint, fstype=''
                        ).set(int(ifree))
    
    def _parse_diskstats(self, output: str, host: str):
        """디스크 I/O 통계 파싱"""
        if not output:
            return
        
        for line in output.strip().split('\n'):
            parts = line.split()
            if len(parts) >= 14:
                device = parts[2]
                # Skip partitions and loop devices
                if re.match(r'(loop|ram|sr)\d+', device) or re.match(r'[a-z]+\d+$', device):
                    continue
                
                reads_completed = int(parts[3])
                sectors_read = int(parts[5])
                writes_completed = int(parts[7])
                sectors_written = int(parts[9])
                
                self.node_disk_reads_completed_total.labels(
                    host=host, device=device
                )._value._value = reads_completed
                
                self.node_disk_writes_completed_total.labels(
                    host=host, device=device
                )._value._value = writes_completed
                
                self.node_disk_read_bytes_total.labels(
                    host=host, device=device
                )._value._value = sectors_read * 512
                
                self.node_disk_written_bytes_total.labels(
                    host=host, device=device
                )._value._value = sectors_written * 512
    
    def _parse_netdev(self, output: str, host: str):
        """네트워크 통계 파싱"""
        if not output:
            return
        
        for line in output.strip().split('\n')[2:]:  # Skip headers
            if ':' not in line:
                continue
            
            parts = line.split(':')
            device = parts[0].strip()
            stats = parts[1].split()
            
            if len(stats) >= 16:
                rx_bytes = int(stats[0])
                rx_packets = int(stats[1])
                rx_errs = int(stats[2])
                tx_bytes = int(stats[8])
                tx_packets = int(stats[9])
                tx_errs = int(stats[10])
                
                self.node_network_receive_bytes_total.labels(
                    host=host, device=device
                )._value._value = rx_bytes
                
                self.node_network_receive_packets_total.labels(
                    host=host, device=device
                )._value._value = rx_packets
                
                self.node_network_receive_errs_total.labels(
                    host=host, device=device
                )._value._value = rx_errs
                
                self.node_network_transmit_bytes_total.labels(
                    host=host, device=device
                )._value._value = tx_bytes
                
                self.node_network_transmit_packets_total.labels(
                    host=host, device=device
                )._value._value = tx_packets
                
                self.node_network_transmit_errs_total.labels(
                    host=host, device=device
                )._value._value = tx_errs
    
    def _parse_vmstat(self, output: str, host: str):
        """시스템 통계 파싱 (실제로는 /proc/stat 데이터)"""
        if not output:
            return
        
        for line in output.strip().split('\n'):
            if line.startswith('ctxt '):
                ctxt = int(line.split()[1])
                self.node_context_switches_total.labels(host=host)._value._value = ctxt
            elif line.startswith('processes '):
                forks = int(line.split()[1])
                self.node_forks_total.labels(host=host)._value._value = forks
            elif line.startswith('procs_running '):
                running = int(line.split()[1])
                self.node_procs_running.labels(host=host).set(running)
            elif line.startswith('procs_blocked '):
                blocked = int(line.split()[1])
                self.node_procs_blocked.labels(host=host).set(blocked)
            elif line.startswith('btime '):
                btime = int(line.split()[1])
                self.node_boot_time_seconds.labels(host=host).set(btime)
    
    def _parse_filefd(self, output: str, host: str):
        """파일 디스크립터 통계 파싱"""
        if output:
            parts = output.strip().split()
            if len(parts) >= 3:
                self.node_filefd_allocated.labels(host=host).set(int(parts[0]))
                self.node_filefd_maximum.labels(host=host).set(int(parts[2]))
    
    async def _get_connection(self, host: str):
        """Get or create SSH connection from pool"""
        # First, check if we have an existing connection (with lock)
        async with self._get_lock():
            if host in self.connections:
                conn = self.connections[host]
                # Test if connection is still alive
                try:
                    result = await asyncio.wait_for(
                        conn.run('echo test', check=False),
                        timeout=2
                    )
                    if result.exit_status == 0:
                        self.logger.debug(f"Reusing connection to {host}")
                        return conn
                    else:
                        # Connection dead, remove it
                        self.logger.debug(f"Connection to {host} is dead, reconnecting")
                        del self.connections[host]
                except:
                    # Connection failed, remove it
                    self.logger.debug(f"Connection to {host} failed test, reconnecting")
                    if host in self.connections:
                        del self.connections[host]
        
        # Create new connection OUTSIDE the lock (slow operation - allows parallel connections)
        self.logger.debug(f"Creating new SSH connection to {host} as user {self.username}...")
        conn = await asyncssh.connect(
            host,
            port=self.port,
            username=self.username,
            client_keys=[self.key_file] if self.key_file else None,
            known_hosts=None
        )
        
        # Add to pool (with lock)
        async with self._get_lock():
            # Double-check in case another task created connection while we were connecting
            if host in self.connections:
                # Another task already connected, close ours and use existing
                conn.close()
                return self.connections[host]
            self.connections[host] = conn
            self.logger.debug(f"New connection established to {host}")
            return conn
    
    async def collect_from_host(self, host: str):
        """하나의 호스트에서 모든 메트릭 수집"""
        try:
            return await self._collect_from_host_impl(host)
        except Exception as e:
            # Catch any unexpected exceptions and log with host info
            self.logger.error(f"Unexpected exception collecting from {host}: {type(e).__name__}: {e}", exc_info=True)
            return ('exception', host, e)
    
    async def _collect_from_host_impl(self, host: str):
        """Internal implementation of collect_from_host"""
        # Check if this host is in bad_hosts and if retry interval has passed
        current_time = time.time()
        if host in self.bad_hosts:
            last_attempt = self.bad_hosts[host]
            time_since_last_attempt = current_time - last_attempt
            if time_since_last_attempt < self.bad_host_retry_interval:
                # Skip this host, will retry after interval (silently skip, counted elsewhere)
                return 'skipped'
            else:
                # Retry interval has passed, remove from bad_hosts and try again
                self.logger.info(f"Retrying connection to previously failed host {host} (last attempt {time_since_last_attempt/60:.1f} minutes ago)")
                del self.bad_hosts[host]
        
        try:
            # Get or create connection from pool
            conn = await self._get_connection(host)
            
            # Collect all metrics in one SSH call for efficiency
            await self._collect_all_metrics(conn, host)
            
            # Connection successful, remove from bad_hosts if it was there
            if host in self.bad_hosts:
                del self.bad_hosts[host]
                self.logger.info(f"Host {host} recovered, removed from bad hosts list")
            
            self.logger.debug(f"Collected metrics from {host}")
            return 'success'
        
        except (asyncssh.Error, OSError, TimeoutError) as e:
            # SSH connection failed - add to bad_hosts (network/firewall issue)
            self.bad_hosts[host] = current_time
            async with self._get_lock():
                if host in self.connections:
                    del self.connections[host]
            # Only log first few failures to avoid log spam
            if len(self.bad_hosts) <= 10:
                self.logger.warning(f"SSH connection failed for {host}: {e}")
            return 'failed'
        except Exception as e:
            # Other errors during metric collection - DO NOT add to bad_hosts
            # These are usually transient errors, connection is still valid
            async with self._get_lock():
                if host in self.connections:
                    # Keep connection alive for next attempt
                    pass
            self.logger.warning(f"Metric collection error from {host}: {type(e).__name__}: {e}")
            return 'error'  # Different from 'failed' - not added to bad_hosts
    
    def update_hosts(self, new_hosts: List[str]):
        """호스트 목록 동적 업데이트"""
        if not new_hosts:
            self.logger.debug("update_hosts called with empty list")
            return
        
        # 중복 제거하며 새 호스트 추가
        current_hosts = set(self.hosts)
        new_host_set = set(new_hosts)
        added_hosts = new_host_set - current_hosts
        
        if added_hosts:
            self.hosts.extend(list(added_hosts))
            self.logger.info(f"Added {len(added_hosts)} new hosts (total: {len(self.hosts)})")
            self.logger.debug(f"New hosts: {sorted(list(added_hosts)[:5])}{'...' if len(added_hosts) > 5 else ''}")
        else:
            self.logger.debug(f"No new hosts to add ({len(new_host_set)} already tracked)")
    
    async def collect_all(self):
        """모든 호스트에서 메트릭 수집"""
        start_time = time.time()
        
        # Debug: Check how many hosts we have and how many are in bad_hosts
        total_hosts = len(self.hosts)
        bad_hosts_count = len(self.bad_hosts)
        active_hosts = total_hosts - bad_hosts_count
        
        if total_hosts == 0:
            self.logger.warning(f"[NODE COLLECTOR] No hosts to collect from! hosts list is empty")
            return
        
        tasks = [self.collect_from_host(host) for host in self.hosts]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = time.time() - start_time
        
        # Count results
        success_count = sum(1 for r in results if r == 'success')
        skipped_count = sum(1 for r in results if r == 'skipped')
        failed_count = sum(1 for r in results if r == 'failed')  # SSH connection failures only
        error_count = sum(1 for r in results if r == 'error')    # Metric collection errors (not in bad_hosts)
        
        # Extract exception tuples: ('exception', host, exception)
        exception_tuples = [r for r in results if isinstance(r, tuple) and len(r) == 3 and r[0] == 'exception']
        exception_count = len(exception_tuples)
        
        # Always log summary
        msg = f"Collected from {success_count}/{total_hosts} hosts in {elapsed:.2f}s"
        details = []
        if skipped_count > 0:
            details.append(f"skipped: {skipped_count}")
        if failed_count > 0:
            details.append(f"conn failed: {failed_count}")
        if error_count > 0:
            details.append(f"errors: {error_count}")
        if exception_count > 0:
            details.append(f"exceptions: {exception_count}")
        
        if details:
            msg += f" ({', '.join(details)})"
        
        self.logger.info(msg)
        
        # Log detailed exception information (already logged in collect_from_host, just show summary)
        if exception_count > 0:
            hosts_with_exceptions = [host for _, host, _ in exception_tuples[:5]]
            self.logger.error(f"Hosts with exceptions: {hosts_with_exceptions}")
    
    def get_metrics(self) -> bytes:
        """Prometheus 포맷으로 메트릭 반환"""
        return generate_latest(self.registry)


async def collection_loop(collector: RemoteNodeCollector, interval: int):
    """주기적으로 메트릭 수집"""
    logger = logging.getLogger(__name__)
    logger.info("Starting collection loop")
    
    iteration = 0
    while True:
        try:
            iteration += 1
            host_count = len(collector.hosts)
            if iteration % 10 == 1:  # Log every 10th iteration
                logger.info(f"[NODE COLLECTOR] Collecting from {host_count} hosts (iteration {iteration})")
            await collector.collect_all()
        except Exception as e:
            logger.error(f"Collection error: {e}")
        
        await asyncio.sleep(interval)


def run_node_exporter(args=None):
    """Run node exporter"""
    parser = argparse.ArgumentParser(description='Remote Node Exporter via SSH')
    parser.add_argument('--hosts', nargs='+', required=True,
                       help='Remote hosts to monitor')
    parser.add_argument('--username', required=True,
                       help='SSH username')
    parser.add_argument('--key-file', 
                       help='SSH private key file')
    parser.add_argument('--port', type=int, default=22,
                       help='SSH port (default: 22)')
    parser.add_argument('--exporter-port', type=int, default=9100,
                       help='Exporter HTTP port (default: 9100)')
    parser.add_argument('--collect-interval', type=int, default=15,
                       help='Collection interval in seconds (default: 15)')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Log level (default: INFO)')
    
    args = parser.parse_args(args)
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )    
    # Reduce asyncssh verbose logging (connection/channel events)
    logging.getLogger('asyncssh').setLevel(logging.WARNING)    
    logger = logging.getLogger(__name__)
    
    # Create collector
    collector = RemoteNodeCollector(
        hosts=args.hosts,
        username=args.username,
        key_file=args.key_file,
        port=args.port
    )
    
    # Start HTTP server
    start_http_server(args.exporter_port, registry=collector.registry)
    logger.info(f"Exporter listening on port {args.exporter_port}")
    
    # Start collection loop
    try:
        asyncio.run(collection_loop(collector, args.collect_interval))
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == '__main__':
    run_node_exporter()
