## Individual Experiment: Memcached

### Throughput (Figure 6)

**Commands:**  `just test-memcached-throughput`

**Execution Time:** ~20 min

**Test Results:** `results/memcached-throughput-report.txt[.json]`

If you don't have 48+ CPU cores available, you can run a scaled-down comparison with:

`python3 scripts/memcached/run-compare.py --mode throughput`

If you want to generate load from another host (client runs remotely via SSH), use:

`python3 scripts/memcached/run-compare.py --mode throughput --server-ip <SERVER_IPV4> --client-ssh <USER@LOADHOST> --client-workdir <REMOTE_ORTHRUS_DIR>`

If your environment has strict firewall rules, also set a fixed port range:

`--port-start <START> --port-end <END>`

For a production/remote-load checklist (including how to build libsei-gcc variants), see `docs/exp-memcached-production.md`.

**Example:**

```text
vanilla running
throughput: 373808.5
sei running
throughput: 370000.0
orthrus running
throughput: 361216.5
rbv running
throughput: 235036.5
```

--------------

### Throughput vs Latency(p95) (Figure 7)

**Commands:** `just test-memcached-latency_vs_pXX`

**Execution Time:** ~4 hour 30 min

**Test Results:** `results/memcached-latency_vs_pXX_{vanilla|orthrus|rbv}.json`

**Example:** N/A

--------------

### Validation Latency CDF (Figure 8)

**Commands:**  `just test-memcached-validation_latency_cdf`

**Execution Time:** ~20 min

**Test Results:** `results/memcached-validation_latency-{vanilla|orthrus|rbv}.cdf`

**Example:** N/A

--------------

## Memory (Discussed in paper)

**Commands:**  `just test-memcached-memory`

**Execution Time:** ~25 min

**Test Results:** `results/memcached-mem-report.txt`

**Example:** 

```
=== Memory Stats ===
Processing raw
max mem run :  17391092
Processing sei
max mem run :  17500000
Processing scee
max mem run :  21958432
Processing rbv
max mem run :  17717556
max mem run :  17521548
----------  results(peak)  ----------
ratio (Orthrus vs Vanilla):  1.2626252566543839
ratio (SEI vs Vanilla):      1.01
ratio (RBV vs Vanilla):      2.026273220796026
----------  results(avg)  ----------
ratio (Orthrus vs Vanilla):  1.2616075431953984
ratio (SEI vs Vanilla):      1.01
ratio (RBV vs Vanilla):      2.042777587432064
```
