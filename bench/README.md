# Natch Benchmark Suite

Comprehensive benchmarks comparing Natch (native TCP) vs Pillar (HTTP) ClickHouse clients.

## Prerequisites

1. **ClickHouse Server Running**
   ```bash
   # Start ClickHouse via Docker
   cd /Users/brendon/work/natch
   docker-compose up -d

   # Verify it's running
   curl http://localhost:8123/ping
   # Should return: Ok.
   ```

2. **Dependencies Installed**
   ```bash
   mix deps.get
   ```

## Benchmarks

### Natch-Only Benchmark

Tests Natch performance in isolation:

```bash
mix run bench/natch_only_bench.exs
```

**What it tests:**
- INSERT performance: 10k, 100k, 1M rows
- SELECT performance: full table scan, filtered queries, aggregations
- Memory usage during operations

**Results:**
- Console output with statistics
- HTML reports: `bench/results_natch_insert.html` and `bench/results_natch_select.html`

### Natch vs Pillar Comparison

Head-to-head comparison:

```bash
mix run bench/natch_vs_pillar_bench.exs
```

**What it tests:**
- INSERT benchmarks at multiple scales
- SELECT benchmarks with various query patterns
- Memory consumption comparison
- Throughput comparison (rows/sec)

**Results:**
- Console output with comparison ratios
- HTML reports: `bench/results_insert.html` and `bench/results_select.html`

## Test Data

All benchmarks use realistic multi-column schema:

```elixir
schema = [
  id: :uint64,           # Primary key
  user_id: :uint32,      # Foreign key
  event_type: :string,   # Category (5 distinct values)
  timestamp: :datetime,  # Temporal data
  value: :float64,       # Numeric metric
  count: :int64,         # Signed integer
  metadata: :string      # Variable-length text
]
```

Data is generated deterministically with a fixed random seed for reproducibility.

## Understanding Results

### Console Output

```
Name                           ips        average  deviation         median
Natch INSERT 100k rows         5.50       181.82 ms     ±8.23%      178.45 ms
Pillar INSERT 100k rows       2.20       454.55 ms    ±12.45%      445.32 ms

Comparison:
Natch INSERT 100k rows         5.50
Pillar INSERT 100k rows       2.20 - 2.50x slower
```

**Key metrics:**
- **ips (iterations per second)**: Higher is better
- **average**: Mean execution time
- **deviation**: Consistency (lower is better)
- **median**: Middle value (less affected by outliers)

### Throughput Calculation

```
Throughput (rows/sec) = rows × ips
Example: 100,000 rows × 5.50 ips = 550,000 rows/sec
```

### HTML Reports

Open the generated HTML files in a browser for:
- Interactive charts
- Detailed statistics
- Memory profiling data
- Export options (JSON, CSV)

## Troubleshooting

### "Connection refused" error

ClickHouse isn't running. Start it with:
```bash
docker-compose up -d
```

### "decode failed" error

This may indicate:
- ClickHouse version incompatibility
- Pillar configuration issue
- Try running `natch_only_bench.exs` first to isolate the problem

### Benchmarks take too long

You can edit the benchmark files to:
- Reduce `time:` parameter (default: 5 seconds per scenario)
- Reduce `warmup:` parameter (default: 1 second)
- Comment out the 1M row benchmarks

### Memory issues with 1M rows

If you run out of memory:
- Close other applications
- Reduce to 100k row benchmarks only
- Increase Docker memory limits

## Customization

### Add More Scenarios

Edit `natch_vs_pillar_bench.exs` and add to the Benchee.run map:

```elixir
"My custom benchmark" => fn ->
  # Your benchmark code here
end
```

### Change Data Generation

Edit `bench/helpers.ex` to modify:
- Schema (add/remove columns)
- Data distribution (change random patterns)
- Row counts

### Adjust Benchmark Duration

In the benchmark files, modify:

```elixir
Benchee.run(
  %{...},
  warmup: 1,      # Warmup duration (seconds)
  time: 5,        # Benchmark duration (seconds)
  memory_time: 2  # Memory profiling duration (seconds)
)
```

## Results Archive

After running benchmarks, you may want to:

1. **Save Results**
   ```bash
   mkdir -p bench/results/$(date +%Y%m%d)
   mv bench/results_*.html bench/results/$(date +%Y%m%d)/
   ```

2. **Add to Git** (optional)
   ```bash
   git add bench/results/
   git commit -m "Add benchmark results for $(date +%Y-%m-%d)"
   ```

3. **Update README.md** with verified numbers

## CI Integration

To run benchmarks in CI:

```yaml
- name: Run Benchmarks
  run: |
    docker-compose up -d
    sleep 5  # Wait for ClickHouse to be ready
    mix run bench/natch_only_bench.exs
```

Consider:
- Running on a schedule (nightly)
- Tracking performance over time
- Alerting on regressions

## Contributing

When adding new benchmarks:

1. Follow the existing naming convention
2. Use `Helpers` module for common operations
3. Clean up tables after benchmarking
4. Document what the benchmark tests
5. Update this README

## References

- [Benchee Documentation](https://hexdocs.pm/benchee/)
- [ClickHouse Performance Guide](https://clickhouse.com/docs/en/operations/performance/)
- [Natch Performance Tips](../README.md#performance-tips)
