# Input throughput diagnostic

`src/throughput.cpp` is a separate measurement utility. It does not change or
participate in the submission pipeline. Build it with:

```sh
g++ -std=c++20 -O3 -fopenmp -I src src/throughput.cpp -o throughput
```

Run with a v5 data directory and one of `sum`, `fnv`, `scan`, or `parse`:

```sh
taskset -c 0-7 ./throughput /data sum 8
taskset -c 0-7 ./throughput /data scan 8
taskset -c 0-7 ./throughput /data parse 8
```

`sum` traverses every byte of every event CSV and accumulates unsigned byte
values. `fnv` is a checksum with a serial dependency chain, so it is
compute-heavy and should not be interpreted as a read-bandwidth floor.
`scan` uses strict `scan_csv<7>` structural parsing and counts valid records.
`parse` additionally parses both event timestamps and checks whether event
and user IDs fit the packed-ID representation. It does not perform link joins,
semantic validation, event deduplication, click processing, or output writing.

## Measurement

Input: `practice17-v5`, event CSV files only (50 files, 1,782,599,239 bytes;
clicks, links, products, campaigns, and manifest are excluded from this byte
count). File bytes include headers; `scan` and `parse` process chunk views after
the header, so their processed-byte counts are slightly smaller. Runs used
eight pinned CPUs (`taskset -c 0-7`) and a warm Linux page cache. The timer
starts after opening/mapping files and planning chunks; it measures the
processing loop only, not start-to-exit time. Three rounds produced these
medians:

| Mode | Median seconds | Throughput | Count |
| --- | ---: | ---: | ---: |
| Byte sum | 0.134652 | 12,625 MiB/s | sum `114869606286` |
| FNV checksum | 0.204517 | 8,312 MiB/s | checksum `4765380072424787636` |
| strict CSV structure scan | 0.105864 | 16,058 MiB/s | 21,588,714 rows |
| CSV + timestamp + packed-ID checks | 0.152944 | 11,115 MiB/s | 21,506,768 timestamp-valid rows; 16,661,359 rows with both IDs packable |

The scan and parse modes read all input records. Their lower times than byte
sum reflect compiler-vectorized delimiter processing and fewer operations per
byte; these are diagnostic lower-level workloads, not independent physical-I/O
measurements.

The current production control used `shopping-sessions:v5-cache` on the same
input, Docker-limited to eight CPUs and 24 GB. Its three container wall times
were 1.524853, 1.535611, and 1.532803 seconds (median 1.532803 seconds). The
container timings are local comparisons, not official scorer measurements.
Raw run detail is in `diagnostics/current-control.json`.
