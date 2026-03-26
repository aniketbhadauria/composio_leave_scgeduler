import os
import json
import datetime as dt
from typing import Dict, Any, List, Tuple

import requests

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


def ist_date_from_utc_date(date_utc: dt.date) -> dt.date:
    # IST is UTC+5:30. For the specific use-case (tomorrow boundary), we compute in IST directly.
    return date_utc


def get_ist_today() -> dt.date:
    now_utc = dt.datetime.now(dt.timezone.utc)
    ist = dt.timezone(dt.timedelta(hours=5, minutes=30))
    now_ist = now_utc.astimezone(ist)
    return now_ist.date()


def rfc3339_day_bounds_ist(day: dt.date) -> Tuple[str, str]:
    ist = dt.timezone(dt.timedelta(hours=5, minutes=30))
    start = dt.datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=ist)
    end = start + dt.timedelta(days=1)
    return start.isoformat(), end.isoformat()


def load_state(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"notified_event_ids": []}


def save_state(path: str, state: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def get_google_creds() -> Credentials:
    # OAuth installed-app style refresh token
    # Required env vars:
    # - GOOGLE_CLIENT_ID
    # - GOOGLE_CLIENT_SECRET
    # - GOOGLE_REFRESH_TOKEN
    # Optional:
    # - GOOGLE_TOKEN_URI (defaults to Google)
    token_uri = os.environ.get("GOOGLE_TOKEN_URI", "https://oauth2.googleapis.com/token")

    creds = Credentials(
        token=None,
        refresh_token=os.environ["GOOGLE_REFRESH_TOKEN"],
        token_uri=token_uri,
        client_id=os.environ["GOOGLE_CLIENT_ID"],
        client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
        scopes=["https://www.googleapis.com/auth/calendar.readonly"],
    )
    creds.refresh(Request())
    return creds


def fetch_leave_events_for_day(service, calendar_id: str, day: dt.date) -> List[Dict[str, Any]]:
    time_min, time_max = rfc3339_day_bounds_ist(day)
    events = []
    page_token = None
    while True:
        resp = (
            service.events()
            .list(
                calendarId=calendar_id,
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
                orderBy="startTime",
                q="Leave",
                pageToken=page_token,
            )
            .execute()
        )
        events.extend(resp.get("items", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    # Filter to title exactly "Leave" (case-insensitive trim)
    out = []
    for e in events:
        summary = (e.get("summary") or "").strip()
        if summary.lower() == "leave":
            out.append(e)
    return out


def post_to_slack(webhook_url: str, text: str) -> None:
    r = requests.post(webhook_url, json={"text": text}, timeout=20)
    r.raise_for_status()


def main() -> None:
    slack_webhook = os.environ["SLACK_WEBHOOK_URL"]
    calendar_id = os.environ.get("GCAL_CALENDAR_ID", "primary")
    message = os.environ.get("LEAVE_MESSAGE", "FYI: I’ll be on leave tomorrow.")

    state_path = os.environ.get("STATE_PATH", ".state/leave_notifier.json")
    state = load_state(state_path)
    notified = set(state.get("notified_event_ids", []))

    ist_today = get_ist_today()
    target_day = ist_today + dt.timedelta(days=1)

    creds = get_google_creds()
    service = build("calendar", "v3", credentials=creds, cache_discovery=False)

    events = fetch_leave_events_for_day(service, calendar_id, target_day)

    new_events = [e for e in events if e.get("id") and e.get("id") not in notified]

    if not new_events:
        print("No new Leave events for tomorrow.")
        return

    # Post once per event
    for e in new_events:
        post_to_slack(slack_webhook, message)
        notified.add(e["id"])
        print(f"Notified for event: {e['id']}")

    state["notified_event_ids"] = sorted(notified)
    save_state(state_path, state)


if __name__ == "__main__":
    main()
