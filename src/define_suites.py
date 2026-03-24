import os
import json
import pandas as pd

DATA_DIR         = "data"
EXPECTATIONS_DIR = "expectations"

os.makedirs(EXPECTATIONS_DIR, exist_ok=True)


def build_generic_suite(table_name, df):
    expectations = []

    expectations.append({
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {"min_value": 1, "max_value": 10_000_000},
        "meta": {"check_type": "completeness"}
    })

    for col in df.columns:
        null_rate = df[col].isna().mean()
        threshold = max(null_rate + 0.05, 0.10)
        expectations.append({
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": col, "mostly": round(1 - threshold, 2)},
            "meta": {"check_type": "completeness",
                     "observed_null_rate": round(float(null_rate), 4)}
        })

    id_cols = [c for c in df.columns if c.endswith("_id") or c == "id"]
    for col in id_cols[:1]:
        expectations.append({
            "expectation_type": "expect_column_values_to_be_unique",
            "kwargs": {"column": col},
            "meta": {"check_type": "consistency"}
        })

    return {
        "expectation_suite_name": f"{table_name}_suite",
        "expectations": expectations,
        "meta": {
            "table_name":   table_name,
            "row_count":    len(df),
            "column_count": len(df.columns),
            "created_at":   pd.Timestamp.now().isoformat(),
        }
    }


def build_rides_suite(df):
    suite = build_generic_suite("rides", df)
    suite["expectations"] += [
        {"expectation_type": "expect_column_values_to_match_regex",
         "kwargs": {"column": "ride_id", "regex": r"^ride_\d{5}$", "mostly": 0.99},
         "meta": {"check_type": "consistency"}},
        {"expectation_type": "expect_column_values_to_be_in_set",
         "kwargs": {"column": "city", "value_set": ["Chicago", "New York",
                    "San Francisco", "Los Angeles", "Seattle"]},
         "meta": {"check_type": "consistency"}},
        {"expectation_type": "expect_column_values_to_be_in_set",
         "kwargs": {"column": "status",
                    "value_set": ["completed", "cancelled", "in_progress"]},
         "meta": {"check_type": "consistency"}},
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "fare_usd", "min_value": 0.01,
                    "max_value": 500.0, "mostly": 0.99},
         "meta": {"check_type": "accuracy"}},
    ]
    return suite


def build_payments_suite(df):
    suite = build_generic_suite("payments", df)
    suite["expectations"] += [
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "amount", "min_value": 0.01,
                    "max_value": 500.0, "mostly": 0.99},
         "meta": {"check_type": "accuracy",
                  "note": "intentional failures injected for testing"}},
        {"expectation_type": "expect_column_values_to_be_in_set",
         "kwargs": {"column": "method",
                    "value_set": ["card", "cash", "uber_cash"]},
         "meta": {"check_type": "consistency"}},
        {"expectation_type": "expect_column_values_to_be_in_set",
         "kwargs": {"column": "status",
                    "value_set": ["completed", "pending", "failed"]},
         "meta": {"check_type": "consistency"}},
    ]
    return suite


def build_drivers_suite(df):
    suite = build_generic_suite("drivers", df)
    suite["expectations"] += [
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "rating", "min_value": 1.0,
                    "max_value": 5.0, "mostly": 0.99},
         "meta": {"check_type": "accuracy"}},
        {"expectation_type": "expect_column_values_to_be_in_set",
         "kwargs": {"column": "car_type",
                    "value_set": ["UberX", "UberXL", "UberBlack"]},
         "meta": {"check_type": "consistency"}},
        {"expectation_type": "expect_column_values_to_match_regex",
         "kwargs": {"column": "driver_id", "regex": r"^drv_\d{4}$",
                    "mostly": 0.99},
         "meta": {"check_type": "consistency"}},
    ]
    return suite


def build_ride_events_suite(df):
    suite = build_generic_suite("ride_events", df)
    suite["expectations"] += [
        {"expectation_type": "expect_column_values_to_be_in_set",
         "kwargs": {"column": "event_type",
                    "value_set": ["request", "accept", "complete", "cancel"],
                    "mostly": 0.99},
         "meta": {"check_type": "consistency"}},
        {"expectation_type": "expect_column_values_to_be_in_set",
         "kwargs": {"column": "source",
                    "value_set": ["mobile_app", "web", "api"]},
         "meta": {"check_type": "consistency"}},
    ]
    return suite


def build_surge_rates_suite(df):
    suite = build_generic_suite("surge_rates", df)
    suite["expectations"] += [
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "multiplier", "min_value": 1.0, "max_value": 5.0},
         "meta": {"check_type": "accuracy"}},
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "hour", "min_value": 0, "max_value": 23},
         "meta": {"check_type": "accuracy"}},
        {"expectation_type": "expect_table_row_count_to_equal",
         "kwargs": {"value": 120},
         "meta": {"check_type": "completeness"}},
    ]
    return suite


SPECIFIC_BUILDERS = {
    "rides":       build_rides_suite,
    "payments":    build_payments_suite,
    "drivers":     build_drivers_suite,
    "ride_events": build_ride_events_suite,
    "surge_rates": build_surge_rates_suite,
}


def build_suite_for_table(table_name, df):
    if table_name in SPECIFIC_BUILDERS:
        print(f"  building SPECIFIC suite  {table_name}")
        return SPECIFIC_BUILDERS[table_name](df)
    return build_generic_suite(table_name, df)


def main():
    print("=" * 55)
    print("DEFINING EXPECTATION SUITES FOR ALL TABLES")
    print("=" * 55)

    csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in {DATA_DIR}/. "
            f"Run python src/generate_data.py first."
        )

    print(f"Found {len(csv_files)} tables in {DATA_DIR}/\n")

    suites_created = 0
    specific_count = 0
    generic_count  = 0

    for filename in sorted(csv_files):
        table_name = filename.replace(".csv", "")
        path       = os.path.join(DATA_DIR, filename)
        df         = pd.read_csv(path)
        suite      = build_suite_for_table(table_name, df)

        suite_path = os.path.join(EXPECTATIONS_DIR, f"{table_name}_suite.json")
        with open(suite_path, "w") as f:
            json.dump(suite, f, indent=2)

        suites_created += 1
        if table_name in SPECIFIC_BUILDERS:
            specific_count += 1
        else:
            generic_count += 1

    print(f"\n{'─' * 55}")
    print(f"Suites created : {suites_created}")
    print(f"  Specific     : {specific_count}")
    print(f"  Generic      : {generic_count}")
    print(f"  Saved to     : {EXPECTATIONS_DIR}/")
    print(f"\nNext: python src/run_validation.py")


if __name__ == "__main__":
    main()