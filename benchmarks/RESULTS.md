Benchmark: 42 tests, median of 3 runs, one pytest process per engine.
Machine: Linux x86_64, 16 CPUs (x86_64), Python 3.11.13, pyspark 4.2.0, pysail 0.7.2.

| Engine | Suite wall time | Median per test | p90 per test | Slowest test | Status |
|---|---|---|---|---|---|
| sail | 2.5 s | 27 ms | 38 ms | 101 ms (test_totals_per_country) | green, 2 known gaps (xfail) |
| spark | 29.1 s | 375 ms | 676 ms | 10143 ms (test_totals_per_country) | all green |

Speedup: suite 11.8x, median test 14x.
