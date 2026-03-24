import os
import json
import glob
from datetime import datetime

REPORTS_DIR   = "reports"
SLACK_TOKEN   = os.environ.get("SLACK_TOKEN", "")
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "#data-quality-alerts")


def load_latest_report():
    pattern = os.path.join(REPORTS_DIR, "report_*.json")
    files   = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(
            f"No reports found in {REPORTS_DIR}/. "
            f"Run python src/run_validation.py first."
        )
    latest = files[-1]
    print(f"Loading report: {latest}")
    with open(latest, "r") as f:
        return json.load(f)


def format_terminal_summary(report):
    summary      = report["summary"]
    failed_tables = report.get("failed_tables", [])
    duration     = report.get("duration_seconds", 0)
    all_failures = report.get("all_failures", [])

    lines = []
    lines.append("=" * 55)
    lines.append("COMPLIANCE REPORT SUMMARY")
    lines.append("=" * 55)
    lines.append(f"  Run time     : {duration:.2f}s")
    lines.append(f"  Total tables : {summary['total_tables']}")
    lines.append(f"  Passed       : {summary['tables_passed']} ✓")
    lines.append(f"  Failed       : {summary['tables_failed']} ✗")
    lines.append(f"  Total checks : {summary['total_checks_run']:,}")
    lines.append(f"  Pass rate    : {summary['pass_rate']:.1f}%")

    if failed_tables:
        lines.append(f"\n  Failed tables:")
        for table_name in failed_tables:
            lines.append(f"    ✗ {table_name}")

    if all_failures:
        lines.append(f"\n  Sample failures:")
        for failure in all_failures[:5]:
            lines.append(f"    → {failure}")

    lines.append("=" * 55)
    return "\n".join(lines)


def send_slack_notification(report):
    summary_text = format_terminal_summary(report)
    print(summary_text)

    summary      = report["summary"]
    failed_tables = report.get("failed_tables", [])
    duration     = report.get("duration_seconds", 0)

    if not SLACK_TOKEN:
        print("\n[INFO] No SLACK_TOKEN set — printing to terminal only.")
        print("[INFO] In production: export SLACK_TOKEN=xoxb-your-token")
        print()
        print(f"  Channel: {SLACK_CHANNEL}")
        print(f"  ┌─────────────────────────────────────────┐")
        print(f"  │ Data Quality Report — "
              f"{datetime.now().strftime('%Y-%m-%d %H:%M')}     │")
        print(f"  │                                         │")
        print(f"  │ Tables : {summary['total_tables']:<5}  "
              f"Passed : {summary['tables_passed']:<5}  "
              f"Failed : {summary['tables_failed']:<3}  │")
        print(f"  │ Pass rate : {summary['pass_rate']:.1f}%  "
              f"Duration : {duration:.1f}s              │")

        if failed_tables:
            print(f"  │                                         │")
            print(f"  │ Failed tables:                          │")
            for name in failed_tables[:3]:
                print(f"  │   ✗ {name:<37}│")

        print(f"  └─────────────────────────────────────────┘")
        return

    try:
        from slack_sdk import WebClient
        from slack_sdk.errors import SlackApiError

        client = WebClient(token=SLACK_TOKEN)
        client.chat_postMessage(
            channel=SLACK_CHANNEL,
            text=f"Data Quality: {summary['tables_passed']}"
                 f"/{summary['total_tables']} tables passed. "
                 f"Failed: {failed_tables}"
        )
        print(f"\n✅ Slack notification sent to {SLACK_CHANNEL}")

    except SlackApiError as e:
        print(f"\n⚠ Slack send failed: {e.response['error']}")

    except ImportError:
        print("\n⚠ slack_sdk not installed — run: pip install slack-sdk")


def main():
    print("=" * 55)
    print("NOTIFY — Sending compliance report")
    print("=" * 55)
    report = load_latest_report()
    send_slack_notification(report)
    print(f"\nReport location: {REPORTS_DIR}/")
    print("Done")


if __name__ == "__main__":
    main()