Benchmark: 42 tests, median of 3 runs, one pytest process per engine.
Machine: Linux x86_64, 16 CPUs (x86_64), Python 3.11.13, pyspark 4.2.0, pysail 0.7.2.

| Engine | Suite wall time | Median per test | p90 per test | Slowest test | Status |
|---|---|---|---|---|---|
| sail | 2.8 s | 25 ms | 34 ms | 146 ms (test_totals_per_country) | green, 2 known gaps (xfail) |
| spark | 31.7 s | 364 ms | 686 ms | 11918 ms (test_totals_per_country) | all green |

Speedup: suite 11.2x, median test 15x.
