# Contributing to sparkwarp

Thank you for helping. sparkwarp is small: a pytest plugin, a few hundred lines of glue,
and a corpus that proves how closely Sail follows Spark. Most of the value is in that
corpus, so new cases are the best contribution.

## Ways to help

- **Add a corpus case** for a function, method or behaviour your suite uses. See below.
- **Report a difference** you hit in a real suite. Include the smallest DataFrame and
  expression that shows it, and what real Spark returns.
- **Report a bug** in sparkwarp itself: the plugin, the fixture, teardown or the docs.
- **Report a Sail gap upstream.** Drafts with verified repros are in
  [`docs/upstream-issues.md`](docs/upstream-issues.md).

## Development setup

You need [uv](https://docs.astral.sh/uv/), Python 3.10+, and Java 17+ for the real-Spark
side of the corpus and the benchmark.

```bash
uv sync --extra test --group dev
uv run pytest -q                       # package tests, on Sail
SPARKWARP=1 uv run pytest corpus -q    # corpus on Sail
SPARKWARP=0 uv run pytest corpus -q    # corpus on real Spark (needs Java 17+)
```

If `SPARK_HOME` points at a separate Spark install, unset it for these commands, so that
PySpark uses its own jars.

## Add a corpus case

1. Add one `Case(...)` to the right list in `corpus/test_*.py`. Functions go in
   `test_functions.py`, DataFrame methods in `test_methods.py`, semantic rules in
   `test_traps.py`, row-order behaviour in `test_ordering.py`.

   ```python
   row("initcap", lambda: F.initcap(F.col("s"))),
   ```

   Set `expects_error=True` when the case is meant to raise on Spark, and `ordered=True`
   when Spark guarantees the row order.

2. Record its expected result on real Spark:

   ```bash
   SPARKWARP=0 SPARKWARP_RECORD=1 uv run pytest corpus -q -k initcap
   ```

   Recording refuses a case whose outcome contradicts `expects_error`, and a case whose
   error comes from the Python client rather than the engine.

3. Run it on Sail:

   ```bash
   SPARKWARP=1 uv run pytest corpus -q -k initcap
   ```

4. If Sail differs, that is a finding. Add a row to [`KNOWN_GAPS.md`](KNOWN_GAPS.md) with
   an id, a severity and a one-line description. Then pass `gap=KnownGap("GAP-0NN")` to
   the case. The id must exist in `KNOWN_GAPS.md`, or the import fails.

5. Regenerate the support page:

   ```bash
   uv run python -m corpus.support_page
   ```

Commit the case, its golden in `corpus/goldens/`, and `docs/SUPPORT.md` together.

## Checks every pull request passes

CI runs all of these. Run them locally before you push:

```bash
uv run ruff check .
uv run black --check .
uv run mypy
uv run pytest -q -n auto
uv run pytest -q -m timing
SPARKWARP=1 uv run pytest corpus -q
uv run python -m corpus.support_page --check
```

## The speed guard

`tests/test_speed_guard.py` keeps sparkwarp's fast paths fast. Two kinds of checks live
there:

- Counters run with every `pytest` run: a small action sends the same requests as plain
  PySpark on Sail, a too-deep column without Java never reaches Sail, and a shallow run
  never opens the JVM.
- Checks marked `timing` compare times measured in one process: an action against plain
  Sail, the regex fallback against a native pattern, the lineage cut against a growing
  loop, and how fast a too-deep column fails and a session starts. A plain `pytest` skips
  them; run them alone, with nothing else busy, as `uv run pytest -q -m timing`.

When a timing check fails, run it once more. If it fails again, treat it as a regression,
not as noise: each limit sits at least a third above the slowest healthy run measured.

## Code style

- Classes over loose functions, and functions of at most 50 lines.
- Named constants instead of magic numbers and strings.
- Name a non-obvious condition before you test it: `is_fresh = date > last_date`.
- No nested functions and no imports inside functions.
- Type everything; `mypy --strict` must pass.
- No comments that narrate the code. Short docstrings are welcome, and so is a comment
  for a hidden invariant.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/),
  for example `test(corpus): add initcap case`.

## Benchmarks and charts

```bash
uv run python -m benchmarks.run --runs 3 --java-home /path/to/jdk17
uv run python -m benchmarks.chart
```

The first command writes `benchmarks/RESULTS.md` and `benchmarks/results.json`. The
second redraws the README charts in `docs/assets/` from that JSON. Commit the results
and the charts together, and say which machine you used.

## License

By contributing, you agree that your contributions are licensed under the
[Apache-2.0 license](LICENSE).
