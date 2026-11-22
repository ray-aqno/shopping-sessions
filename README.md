# Shopping Sessions

A C++20 implementation of the exact [Shopping Sessions rules](rules.md).
OpenMP supplies CPU parallelism; Apache Arrow C++ writes Parquet. The runtime
does not launch Python, a SQL engine, a JVM, or a subprocess.

## Optimization update

The submission target is **under 1.4 seconds** on the hidden practice dataset.
The current build takes **1.39 seconds median** locally on 20M events / 5M clicks.
Three runs took 1.42, 1.31, and 1.39 seconds, versus 1.60 seconds median for the
preceding parallel-graph build in the same alternating comparison. Peak RSS was
2.89 GiB. These are native start-to-exit measurements on eight pinned CPUs, not
official scorer results; the margin against 1.4 seconds is small. See the
[raw phase timings and memory measurements](benchmarks/compact-events-20m.json).
Docker verification and a current-build 60M benchmark are pending. The 39.40-second
result below is for the original build.

The changes are one CSV parse per event, tombstones for invalid dedup candidates,
a bounded exact-key cache for identity resolution, and a CPU-dispatched AVX2 CSV
parser with a scalar fallback, collision-free packed IDs with exact string
fallbacks, parallel identity-graph construction, and 16-byte events sorted by
radix. Windows longer than the compact timestamp representation supports use
full-width timestamps and comparison sorting. Dedup still considers invalid well-formed copies:
an invalid earliest copy still suppresses a later valid copy. Vector parsing
passed 450,000 scalar/vector comparisons under AddressSanitizer and UBSan.
The optimized pipeline passes all 36 fixtures and eight independent randomized
comparisons with and without Unicode/whitespace mutations, plus a long-window
fallback check. Its full practice output matches the original exactly: 2,249,563
user rows and 28,014 campaign rows, with no differences or duplicate keys.

## Verified results

All 36 official fixtures pass. The official seed-1 dev answer matches exactly:
117,038 user rows and 27,400 campaign rows, with no differences or duplicate keys.
An independent Python oracle also matches eight generated datasets with extra
malformed records, Unicode IDs/digits, carriage returns, long epoch values,
duplicate IDs, and invalid earliest event copies.

Original-build start-to-exit timings, including shared-library startup and process teardown:

| Input | Seed | Wall time | Peak RSS |
| --- | --- | ---: | ---: |
| 1M events / 250K clicks, official dev | 1 | 0.31 s | 205 MiB |
| 20M events / 5M clicks | 17 | 7.72 s before traversal change; 8.67 s after | 3.12 GiB |
| 60M events / 15M clicks | 2028 | 39.40 s | 9.02 GiB |

These are native executable measurements on an Intel Core Ultra 9 275HX under
WSL2. Practice and final runs were pinned to CPUs 0–7 with `NCPU=8`; the machine
has 15.3 GiB RAM. Inputs use the official generator. These are single runs, not
official scores or medians. File cache state, workstation activity, memory pressure,
and CPU scheduling differ from the scorer. A streaming output check overlapped
part of the final-scale measurement. Both practice runs and the final run reported
zero swaps in `/usr/bin/time`.

Docker Desktop was unavailable during the initial measurements. Container
verification is being performed separately from these historical native timings.
The 60M test validates runtime and cross-table invariants; no published exact
answer exists for that seed and size.
The final-scale output also passes streaming accounting checks: 6,663,688 user
rows, 28,014 campaign rows, and 10,081,816 sessions, with equal session and revenue
totals across both tables.

## Build and verify locally

Requirements: Linux amd64, a C++20 compiler, CMake, OpenMP, Python 3.12 or 3.13,
and Rust 1.85+ for the official generator. Arrow 21 has prebuilt wheels for these
Python versions; the wheel supplies native headers and shared libraries.

```sh
uv venv --python 3.13 .venv
uv pip install --python .venv/bin/python -r requirements-dev.txt
make fixtures
make parser-check
make differential
make dev
make run-dev
```

Download and extract the official `dev-answer.zip` from the track page, then:

```sh
make check-dev ANSWER=/path/to/dev-answer
```

The published scripts and fixtures are included unmodified; see [NOTICE.md](NOTICE.md).
Generated input, outputs, virtual environments, compiler artifacts, and archives
are excluded from Git and from the Docker build context.

## Container

```sh
make image
make fixtures-container
make run-dev-container
make check-dev ANSWER=/path/to/dev-answer
```

The root Dockerfile builds `linux/amd64`. Its final stage contains the compiled
executable, Arrow/Parquet shared libraries, and system runtime libraries. It runs
as uid/gid 1000. The official `scripts/docker_run.sh` applies the scorer's read-only
filesystem, network, memory, CPU, `/tmp`, and process limits.

Environment variables: `DATA_DIR=/data`, `OUTPUT_DIR=/output`, `NCPU=8`.
`HOME=/scratch`, `SCRATCH_DIR=/scratch`, and `MEM_GB=24` are accepted by the container
environment. The algorithm is in memory; it does not spill or use `/tmp` or
`/scratch`. `MEM_GB` does not dynamically resize the algorithm; the container must
enforce the memory limit. Final-size native peak RSS was 9.02 GiB for the measured
seed; different seeds and host cache pressure can change memory and runtime.

To time a generated input on eight pinned CPUs:

```sh
tools/generator/target/release/generate --seed 17 --events 20000000 --clicks 5000000 --threads 8 --output data/practice17
env DATA_DIR=data/practice17 OUTPUT_DIR=output/practice17 NCPU=8 /usr/bin/time -v taskset -c 0-7 build/shopping
.venv/bin/python scripts/check_invariants.py output/practice17
```

The invariant checker checks accounting and row constraints, not full correctness.
Use the exact dev comparator and independent oracle for correctness.

## Design

1. **Map files and resolve identity links first.** Strict CSV parsing keeps spaces
   and carriage returns. Eligible short IDs are encoded losslessly into integers;
   all other values retain exact-string handling. For packed IDs, parallel hash
   partitions intern nodes, then atomic unions hook larger roots below smaller
   roots. Parent IDs strictly decrease, preventing cycles. General linked strings
   use union by size with path compression. A separate component minimum names
   each person, including IDs that appear only in links. The finalized identity
   dictionary is immutable during event processing. A bounded per-thread cache
   accelerates repeated IDs with exact comparisons. Unlinked IDs use striped
   interning, giving every person a compact numeric key.
2. **Parse once and partition event copies by a full-string hash.** The parser
   checks separators and quotes in 32-byte blocks where AVX2 and POPCNT are
   available, with a scalar fallback. Every structurally valid row with a nonempty
   event ID is retained. Semantic validation is computed during this sequential
   pass and invalid copies become tombstones, rather than being discarded.
   Calendar and offset checks are explicit. Epoch milliseconds and prices accept
   only ASCII digits and use checked integer arithmetic, including leading zeros.
   Only purchase events need category lookups.
3. **Dedup all copies and compact surviving winners.** Each of 256 partitions
   deduplicates with exact string comparisons and keeps the earliest ingest
   timestamp, including tombstones. A tombstone winner emits no event. Hash
   collisions never establish equality. Malformed rows cannot suppress valid
   copies; invalid well-formed winners can. Compact numeric events are partitioned
   by person. Windows of at most 2^32 milliseconds use a 64-bit person/time key and
   16-byte event records, sorted with stable radix passes; longer windows retain
   full timestamps. Raw dedup partitions are freed as they finish. There is no second
   CSV parse and no scattered semantic-validation pass over the mapped CSVs.
4. **Parse and partition clicks.** Identical click duplicates are retained because
   their multiplicity is semantically invisible to both attribution and set-valued
   assists. This relies on the rules' guarantee that duplicate click IDs have
   identical values. Each partition sorts by person, timestamp, then descending
   click ID, making the smallest ID the final click at a tied timestamp.
5. **Scan each person's sorted events and clicks.** Adjacent event gaps greater
   than 30 minutes start sessions. Every event in a session uses its start day.
   Two monotonic click cursors maintain the inclusive seven-day window. Per-campaign
   reference counts and a dense active set produce distinct campaign assists;
   they avoid a session-by-click Cartesian/range join. The final active click wins
   attribution. Campaign totals include assist-only rows.
6. **Write Parquet directly.** Each worker owns its user rows and campaign totals,
   preventing locks in session processing. Campaign totals merge after the scan.
   User files are written in parallel; dates use Parquet `DATE`, and no-purchase
   top categories are null. Only visible Parquet files in the two output folders
   are replaced on a repeated run.

The memory cost is linear in input rows and distinct IDs. Sorting costs the sum
of per-partition event/click sorts. Attribution scans clicks once per person;
assist work is proportional to the number of distinct campaigns assisting each
session, rather than the number of qualifying clicks.

## Submission

Publish this repository and submit a commit hash after building and testing the
container. The repository contains no generated dataset. A public repository and
an official submission have not been created by these local validation steps.
