import os
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)
Faker.seed(42)

OUTPUT_DIR  = "data"
NUM_RECORDS = 1000

CITIES   = ["Chicago", "New York", "San Francisco", "Los Angeles", "Seattle"]
STATUSES = ["completed", "cancelled", "in_progress"]

os.makedirs(OUTPUT_DIR, exist_ok=True)


def maybe_null(value, null_rate=0.02):
    """
    Introduce realistic null rate.
    2% of values will be None by default.
    Some tables get higher rates to trigger quality failures.
    """
    return None if random.random() < null_rate else value


def gen_rides():
    """
    Core rides table — mostly clean data.
    Small null rate on driver_id (drivers sometimes unassigned).
    """
    rows = []
    for i in range(NUM_RECORDS):
        rows.append({
            "ride_id":    f"ride_{i:05d}",
            "rider_id":   f"rider_{random.randint(0, 5000):05d}",
            "driver_id":  maybe_null(f"drv_{random.randint(0, 500):04d}"),
            "city":       random.choice(CITIES),
            "status":     random.choices(STATUSES, weights=[0.75, 0.15, 0.10])[0],
            "fare_usd":   round(random.uniform(3.0, 150.0), 2),
            "event_time": (datetime.now() - timedelta(
                               days=random.uniform(0, 7)
                           )).isoformat(),
        })
    return pd.DataFrame(rows)


def gen_payments():
    """
    Payments table — intentional bad data injected:
    - 5% negative fares (accuracy failure)
    - 3% null payment_ids (completeness failure)
    This ensures our quality checks actually catch something.
    """
    rows = []
    for i in range(NUM_RECORDS):
        # Intentional: 5% chance of negative fare — accuracy check catches this
        if random.random() < 0.05:
            fare = round(random.uniform(-50, -1), 2)
        else:
            fare = round(random.uniform(3.0, 150.0), 2)

        rows.append({
            # Intentional: 3% null payment_id — completeness check catches this
            "payment_id": maybe_null(f"pay_{i:05d}", null_rate=0.03),
            "ride_id":    f"ride_{random.randint(0, NUM_RECORDS-1):05d}",
            "amount":     fare,
            "method":     random.choice(["card", "cash", "uber_cash"]),
            "status":     random.choices(
                              ["completed", "pending", "failed"],
                              weights=[0.90, 0.07, 0.03]
                          )[0],
        })
    return pd.DataFrame(rows)


def gen_drivers():
    """
    Drivers table — intentional bad data:
    - 2% ratings outside 1-5 range (accuracy failure)
    - Driver names and emails present (PII — for masking checks)
    """
    rows = []
    for i in range(500):
        # Intentional: 2% chance of invalid rating
        if random.random() < 0.02:
            rating = round(random.uniform(5.5, 10.0), 1)  # invalid
        else:
            rating = round(random.uniform(3.5, 5.0), 2)   # valid

        rows.append({
            "driver_id":   f"drv_{i:04d}",
            "name":        fake.name(),        # PII
            "email":       fake.email(),       # PII
            "phone":       fake.phone_number(), # PII
            "city":        random.choice(CITIES),
            "car_type":    random.choice(["UberX", "UberXL", "UberBlack"]),
            "rating":      rating,
            "total_trips": random.randint(10, 5000),
            "is_active":   random.choice([True, True, True, False]),
        })
    return pd.DataFrame(rows)


def gen_ride_events():
    """
    Events table — intentional bad data:
    - 4% invalid event types (consistency failure)
    """
    valid_types   = ["request", "accept", "complete", "cancel"]
    invalid_types = ["unknown", "error", "timeout"]  # should never appear

    rows = []
    for i in range(NUM_RECORDS * 2):   # ~2 events per ride
        # Intentional: 4% invalid event type — consistency check catches this
        if random.random() < 0.04:
            event_type = random.choice(invalid_types)
        else:
            event_type = random.choice(valid_types)

        rows.append({
            "ride_id":    f"ride_{random.randint(0, NUM_RECORDS-1):05d}",
            "event_type": event_type,
            "event_time": (datetime.now() - timedelta(
                               hours=random.uniform(0, 168)
                           )).isoformat(),
            "source":     random.choice(["mobile_app", "web", "api"]),
        })
    return pd.DataFrame(rows)


def gen_surge_rates():
    """
    Surge rates table — clean data.
    Small lookup table: 5 cities × 24 hours = 120 rows.
    """
    rows = []
    for city in CITIES:
        for hour in range(24):
            is_peak = hour in [7, 8, 9, 17, 18, 19, 20]
            rows.append({
                "city":       city,
                "hour":       hour,
                "multiplier": round(random.uniform(1.5, 3.5), 2)
                              if is_peak
                              else round(random.uniform(1.0, 1.4), 2),
            })
    return pd.DataFrame(rows)


def gen_generic_table(table_num):
    """
    Generates one of the remaining 495 tables.
    These are simple dimension/fact tables with clean data.
    Used to simulate the full 500-table environment.
    """
    rows = []
    for i in range(NUM_RECORDS):
        rows.append({
            "id":         f"rec_{i:05d}",
            "table_id":   table_num,
            "category":   random.choice(["A", "B", "C", "D"]),
            "value":      round(random.uniform(0, 1000), 2),
            "is_active":  random.choice([True, False]),
            "created_at": (datetime.now() - timedelta(
                               days=random.uniform(0, 365)
                           )).isoformat(),
        })
    return pd.DataFrame(rows)


def main():
    print("=" * 55)
    print("GENERATING 500 SOURCE TABLES")
    print("=" * 55)

    # The 5 named tables with intentional quality issues
    named_tables = {
        "rides":       gen_rides(),
        "payments":    gen_payments(),
        "drivers":     gen_drivers(),
        "ride_events": gen_ride_events(),
        "surge_rates": gen_surge_rates(),
    }

    # Generate remaining 495 generic tables
    # Total = 5 named + 495 generic = 500 tables
    generic_tables = {
        f"table_{i:03d}": gen_generic_table(i)
        for i in range(6, 501)
    }

    all_tables = {**named_tables, **generic_tables}

    # Save all tables as CSV files
    for name, df in all_tables.items():
        path = os.path.join(OUTPUT_DIR, f"{name}.csv")
        df.to_csv(path, index=False)

    total_rows = sum(len(df) for df in all_tables.values())
    print(f"  Generated {len(all_tables)} tables")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Saved to:   {OUTPUT_DIR}/")
    print(f"\n  Named tables with intentional bad data:")
    print(f"  payments    → 5% negative fares, 3% null IDs")
    print(f"  drivers     → 2% invalid ratings")
    print(f"  ride_events → 4% invalid event types")
    print(f"\nDone — ready for quality checks")


if __name__ == "__main__":
    main()