"""
notifier.py

Sends ONE consolidated email per DQ run that has failures -- not one email
per failing check, which would be spammy and drown out the signal. The
email lists every failure with its blast radius, so the reader immediately
sees both "what broke" and "what it could be affecting."
"""

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime


def _build_email_body(failing_checks: list) -> str:
    """failing_checks: list of dicts with table_name, check_type, column_name,
    detail, and an optional 'blast_radius' dict (from blast_radius.py)."""
    lines = [
        f"Data Quality Alert -- {len(failing_checks)} check(s) failing",
        f"Generated: {datetime.now().isoformat(timespec='seconds')}",
        "=" * 60,
        "",
    ]

    for check in failing_checks:
        lines.append(f"Table:      {check['table_name']}")
        lines.append(f"Check type: {check['check_type']}")
        if check.get("column_name"):
            lines.append(f"Column:     {check['column_name']}")
        lines.append(f"Detail:     {check['detail']}")

        blast = check.get("blast_radius")
        if blast and blast.get("affected_tables"):
            lines.append(f"Blast radius: {blast['affected_count']} table(s) "
                          f"potentially affected -- {', '.join(blast['affected_tables'])}")
        else:
            lines.append("Blast radius: none (failure contained to this table)")

        lines.append("-" * 60)

    lines.append("")
    lines.append("See the Streamlit dashboard's Failure Drilldown page for full detail.")

    return "\n".join(lines)


def send_failure_alert(failing_checks: list) -> bool:
    """
    Sends the alert email. Returns True if sent successfully, False if
    email sending failed (this should NEVER crash the DQ run itself --
    a failed alert is a secondary problem, not a reason to lose the DQ
    results that already got written to the database).
    """
    if not failing_checks:
        return True  # nothing to alert about

    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT", 587))
    smtp_username = os.environ.get("SMTP_USERNAME")
    smtp_password = os.environ.get("SMTP_PASSWORD")
    sender = os.environ.get("ALERT_SENDER_EMAIL")
    recipient = os.environ.get("ALERT_RECIPIENT_EMAIL")

    if not all([smtp_host, smtp_username, smtp_password, sender, recipient]):
        print("[notifier] SMTP settings incomplete in .env -- skipping email alert.")
        return False

    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = f"[DQ Alert] {len(failing_checks)} check(s) failing"
    msg.attach(MIMEText(_build_email_body(failing_checks), "plain"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.sendmail(sender, recipient, msg.as_string())
        print(f"[notifier] Alert email sent to {recipient} "
              f"({len(failing_checks)} failing check(s)).")
        return True
    except Exception as e:
        print(f"[notifier] Failed to send alert email: {e}")
        return False
