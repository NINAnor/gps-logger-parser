"""Compare two pytest-benchmark JSON result files and output a Markdown report.

Usage:
    python scripts/benchmark_compare.py <baseline.json> <current.json>

The report is printed to stdout and is suitable for posting as a GitHub PR
comment.  The script always exits 0 — it is informational only.
"""

import json
import pathlib
import sys

REGRESSION_THRESHOLD_PCT = 20  # warn marker above this percentage


def load_benchmarks(path: str) -> dict[str, dict]:
    """Return a dict mapping test name -> stats from a pytest-benchmark JSON."""
    with pathlib.Path(path).open() as file:
        data = json.load(file)
    return {bench["name"]: bench["stats"] for bench in data["benchmarks"]}


def short_name(full_name: str) -> str:
    """Extract the parametrize ID from a full test name."""
    start = full_name.find("[")
    end = full_name.rfind("]")
    if start != -1 and end != -1:
        return full_name[start + 1 : end]
    return full_name


def format_table(
    rows: list[tuple[str, float, float, float]],
) -> list[str]:
    """Format comparison rows into a Markdown table.

    Each row is (name, baseline_ms, current_ms, change_pct).
    """
    lines = [
        "| Test | Baseline (ms) | Current (ms) | Change |",
        "|---|--:|--:|--:|",
    ]
    for name, baseline_ms, current_ms, change_pct in rows:
        warn = " :warning:" if change_pct > REGRESSION_THRESHOLD_PCT else ""
        lines.append(
            f"| {name} | {baseline_ms:.2f} | {current_ms:.2f} "
            f"| {change_pct:+.1f}%{warn} |"
        )
    return lines


def compare(
    baseline: dict[str, dict],
    current: dict[str, dict],
    name_filter: str,
) -> tuple[list[tuple[str, float, float, float]], float]:
    """Compare benchmarks whose names contain *name_filter*.

    Returns (rows, average_change_pct).
    """
    rows: list[tuple[str, float, float, float]] = []
    for name in sorted(baseline):
        if name_filter not in name:
            continue
        if name not in current:
            continue
        baseline_ms = baseline[name]["mean"] * 1000
        current_ms = current[name]["mean"] * 1000
        change_pct = ((current_ms - baseline_ms) / baseline_ms) * 100
        rows.append((short_name(name), baseline_ms, current_ms, change_pct))

    average = sum(r[3] for r in rows) / len(rows) if rows else 0.0
    return rows, average


def main() -> None:
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <baseline.json> <current.json>", file=sys.stderr)
        sys.exit(1)

    baseline = load_benchmarks(sys.argv[1])
    current = load_benchmarks(sys.argv[2])

    output: list[str] = ["## Benchmark Comparison", ""]

    # --- Detection ---
    detect_rows, detect_avg = compare(baseline, current, "test_bench_detect[")
    # Exclude harmonize tests that also contain "detect"
    detect_rows = [r for r in detect_rows if "harmonize" not in r[0]]
    if detect_rows:
        # Recalculate average after filtering
        detect_avg = (
            sum(r[3] for r in detect_rows) / len(detect_rows) if detect_rows else 0.0
        )
        output.append("### Detection (`detect_file`)")
        output.append("")
        output.extend(format_table(detect_rows))
        output.append("")
        output.append(f"**Average: {detect_avg:+.1f}%**")
        output.append("")

    # --- Full pipeline ---
    harmonize_rows, harmonize_avg = compare(
        baseline, current, "test_bench_detect_and_harmonize["
    )
    if harmonize_rows:
        output.append("### Full Pipeline (`detect_file` + `as_table`)")
        output.append("")
        output.extend(format_table(harmonize_rows))
        output.append("")
        output.append(f"**Average: {harmonize_avg:+.1f}%**")
        output.append("")

    # --- Summary ---
    if not detect_rows and not harmonize_rows:
        output.append(
            "No matching benchmarks found in both baseline and current results."
        )
    else:
        regressions = [
            r for r in (detect_rows + harmonize_rows) if r[3] > REGRESSION_THRESHOLD_PCT
        ]
        if regressions:
            output.append(
                f":warning: **{len(regressions)} test(s) regressed "
                f"by more than {REGRESSION_THRESHOLD_PCT}%**"
            )
        else:
            output.append("No significant regressions detected.")

    print("\n".join(output))


if __name__ == "__main__":
    main()
