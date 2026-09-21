#!/usr/bin/env python3

import argparse
import csv
import math
import statistics
from pathlib import Path


PERCENTILES = (50.0, 90.0, 95.0, 99.0, 99.9)


def percentile(sorted_values: list[float], percentage: float) -> float:
    """Linear interpolation using the same rank convention as NumPy's default."""
    if not sorted_values:
        raise ValueError("cannot calculate a percentile of an empty sample")
    position = (len(sorted_values) - 1) * percentage / 100.0
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return sorted_values[lower]
    fraction = position - lower
    return sorted_values[lower] * (1.0 - fraction) + sorted_values[upper] * fraction


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Summarize one numeric column from a latency CSV file."
    )
    parser.add_argument("csv_file", type=Path)
    parser.add_argument(
        "--column",
        default="corrected_ns",
        help="CSV column containing numeric latency samples (default: corrected_ns)",
    )
    args = parser.parse_args()

    with args.csv_file.open(newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None or args.column not in reader.fieldnames:
            available = ", ".join(reader.fieldnames or ())
            raise SystemExit(
                f"column {args.column!r} not found; available columns: {available}"
            )
        values = [float(row[args.column]) for row in reader]

    if not values:
        raise SystemExit("no latency samples found")

    values.sort()
    print(f"file: {args.csv_file}")
    print(f"metric: {args.column}")
    print(f"sample_count: {len(values)}")
    print(f"mean: {statistics.fmean(values):.6f}")
    print(f"median: {statistics.median(values):.6f}")
    for percentage in PERCENTILES:
        label = str(percentage).rstrip("0").rstrip(".")
        print(f"p{label}: {percentile(values, percentage):.6f}")
    print(f"min: {values[0]:.6f}")
    print(f"max: {values[-1]:.6f}")
    print(f"standard_deviation: {statistics.pstdev(values):.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
