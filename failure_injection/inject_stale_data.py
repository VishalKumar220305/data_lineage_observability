"""
inject_stale_data.py

FAILURE INJECTION SCENARIO 3: Stale data.

Simulates a scheduler outage or silently-broken cron job: stg_sellers
hasn't actually been refreshed in 30 hours, past the configured 24-hour
freshness threshold. No data is touched here at all -- this scenario is
purely about time.

Unlike scenarios 1 and 2, this one directly backdates stg_sellers' EXISTING
most recent successful run (rather than inserting a new one) -- this is
the only mechanically reliable way to simulate staleness, since the
freshness check always looks at the single most recent successful run for
a job, and a new row can't out-rank a genuinely more recent real one. This
does move a real row's timestamp, which is why it's clearly labeled in
that row's own error_message rather than done quietly.

Usage:
    python -m failure_injection.inject_stale_data
"""

from failure_injection._inject_helpers import backdate_latest_run

JOB_NAME = "load_sellers"
HOURS_AGO = 30.0   # past the 24h max_staleness_hours threshold in config.yaml


def run():
    print("=" * 70)
    print("FAILURE INJECTION: Stale data on stg_sellers")
    print(f"Simulating: last successful run was {HOURS_AGO:.0f}h ago "
          f"(threshold is 24h)")
    print("=" * 70)

    backdate_latest_run(
        job_name=JOB_NAME,
        hours_ago=HOURS_AGO,
        scenario_label=(f"Stale data scenario -- simulating a scheduler outage; "
                         f"stg_sellers has not actually been refreshed in {HOURS_AGO:.0f}h"),
    )

    print("\nInjection complete. Now run: python -m data_quality.dq_runner")
    print("Expect stg_sellers to show:")
    print(f"  - FAIL on freshness (staleness={HOURS_AGO:.0f}h, threshold=24h)")


if __name__ == "__main__":
    run()
