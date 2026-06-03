# fs2-nats benchmarks

Harness for measuring the Tier 1–3 performance work. Two layers:

1. **JMH micro-benchmarks** — isolate the CPU/allocation cost of the serialization
   and parser hot paths, in memory, with low variance (no network).
2. **`ThroughputBench`** — an end-to-end app that drives a real NATS server to
   measure publish throughput and round-trip latency (the only way to see the
   write-coalescing win, which depends on real socket writes).

## Prerequisites

A NATS server for the end-to-end app (not needed for JMH):

```bash
docker compose up -d        # from the repo root; serves nats on :4222
```

## 1. JMH micro-benchmarks

```bash
# Serialization (Tier 1 single-array frame build)
sbt "benchmarks/Jmh/run -i 5 -wi 5 -f 1 .*SerializationBenchmark.*"

# Parser (Tier 1 in-place findCrlf + Tier 2.1 byte tokenizer)
sbt "benchmarks/Jmh/run -i 5 -wi 5 -f 1 .*ProtocolParserBenchmark.*"

# Allocation rate (bytes/op) — the metric the rewrites target:
sbt "benchmarks/Jmh/run -prof gc .*SerializationBenchmark.*"
```

`-prof gc` prints `·gc.alloc.rate.norm` = **bytes allocated per op**. The Tier 1
serialization rewrite and Tier 2.1 tokenizer should show a clear drop here.

## 2. End-to-end throughput + latency

```bash
sbt "benchmarks/runMain fs2.nats.benchmarks.ThroughputBench"           # 200000 x 16B
sbt "benchmarks/runMain fs2.nats.benchmarks.ThroughputBench 1000000 16"
sbt "benchmarks/runMain fs2.nats.benchmarks.ThroughputBench 200000 256 localhost 4222"
```

Args: `[numMessages] [payloadBytes] [host] [port]` (defaults `200000 16 localhost 4222`).
Reports `msgs/s`, `MB/s`, and round-trip latency percentiles (p50/p99/p99.9/max).

## Before/after comparison

The numbers are only meaningful relative to the baseline. To compare the
optimization branch against `main`:

```bash
git stash                       # if needed
git checkout main
sbt "benchmarks/runMain fs2.nats.benchmarks.ThroughputBench 500000 16"   # baseline
git checkout perf/write-coalescing
sbt "benchmarks/runMain fs2.nats.benchmarks.ThroughputBench 500000 16"   # optimized
```

(The `benchmarks` project lives only on this branch, so for the baseline run
either cherry-pick this directory onto `main` or copy it across worktrees.)

## Example results (main vs `perf/write-coalescing`)

Back-to-back on one dev machine (200,000 × 16 B, loopback echo). Absolute numbers
vary by machine and load — the **ratio** is the point.

| Metric | Baseline (`main`) | Optimized | Change |
|---|---|---|---|
| Throughput | 74,161 msgs/s | 287,686 msgs/s | **~3.9× faster** |
| `buildPub` alloc (16 B) | 464 B/op | 136 B/op | **~3.4× less** |
| `buildPub` speed (16 B) | 22.9 ops/µs | 30.7 ops/µs | ~1.34× |
| Round-trip latency p50 | 217 µs | 217 µs | neutral |

Throughput is the headline (write coalescing — fewer syscalls under load).
Round-trip latency is ~neutral: the latency test sends one message at a time, so
there is nothing to coalesce; the win shows up only under sustained load.

## Syscall count (proves write coalescing)

The headline Tier 1 win is **one `write` per drain cycle instead of per message**.
Count `write`/`writev` syscalls while the throughput app runs:

```bash
# Linux
strace -f -e trace=write,writev -c -- \
  sbt "benchmarks/runMain fs2.nats.benchmarks.ThroughputBench 200000 16"

# macOS (needs sudo; SIP must allow dtrace on your own java process)
sudo dtruss -c -t write -f \
  sbt "benchmarks/runMain fs2.nats.benchmarks.ThroughputBench 200000 16" 2>&1 \
  | grep -c write
```

Pre-Tier-1 you should see ≈ N writes for N messages; after, the count drops by
1–2 orders of magnitude (one write per coalesced flush).

## Allocation / flight-recorder profile

```bash
sbt -J-XX:StartFlightRecording=filename=bench.jfr,settings=profile \
  "benchmarks/runMain fs2.nats.benchmarks.ThroughputBench 200000 16"
# open bench.jfr in JDK Mission Control, or use async-profiler in alloc mode
```

Top allocators that the Tier 1–3 work removes from the hot paths:
`Chunk$Queue`, `String`/`char[]` (was `s"PUB…"` + `new String(toArray)`),
`Pattern`/`Matcher` (was the regex split), and per-message `Some`.

## Caveat — TCP_NODELAY

The client connects with default socket options, so **Nagle's algorithm is on**.
That OS-level coalescing can mask or distort the app-level write-coalescing and
inflate small-message latency. Setting `TCP_NODELAY` on the client socket is a
worthwhile follow-up (the library does not currently expose socket options);
keep it in mind when interpreting latency numbers.
