"""Create the Metabase admin, the Snowflake connection and the credit-risk dashboard.

Idempotent: it reuses what already exists (admin, database, collection) and recreates the
dashboard cards. Everything is sent to the LOCAL Metabase only (http://localhost:3000).

Credentials come from the environment (.env, git-ignored):
    MB_ADMIN_EMAIL, MB_ADMIN_PASSWORD   the Metabase admin you will log in with
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE_DEV
The Snowflake key is read by Metabase from /run/snowflake/rsa_key.p8 (read-only mount in the
compose file), so no secret is ever sent through this script.

Usage:
    python bi/metabase/setup.py [--url http://localhost:3000]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

from credit_risk_pipeline import config  # noqa: F401  (loads .env)

DASHBOARD_NAME = "Credit risk: default story"

# (name, display, sql, visualization_settings). Every query reads the dbt marts only.
QUESTIONS = [
    (
        "Global default rate",
        "scalar",
        """select round(100 * sum(n_defaults) / sum(n_clients), 2) as default_rate_pct
from marts.mart_credit_risk
where dimension = 'code_gender'  -- any single dimension partitions the train clients once""",
        {"scalar.field": "DEFAULT_RATE_PCT"},
    ),
    (
        "Default rate by delinquency bucket (lifetime worst DPD)",
        "bar",
        """select dimension_value as dpd_bucket,
       round(100 * default_rate, 2) as default_rate_pct,
       n_clients
from marts.mart_credit_risk
where dimension = 'dpd_bucket' and dimension_value <> 'unpaid'
order by case dimension_value when 'no_history' then 0 when '0' then 1 when '1-30' then 2
                              when '31-60' then 3 when '61-90' then 4 when '90+' then 5 end""",
        {"graph.dimensions": ["DPD_BUCKET"], "graph.metrics": ["DEFAULT_RATE_PCT"]},
    ),
    (
        "Default rate by when the worst 90+ delay happened (recency)",
        "bar",
        """with paid as (
    select sk_id_curr, due_day, dpd from marts.fct_installments where dpd is not null
),
worst as (select sk_id_curr, max(dpd) as max_dpd from paid group by sk_id_curr),
worst_day as (
    select p.sk_id_curr, max(p.due_day) as worst_due_day
    from paid as p inner join worst as w on p.sk_id_curr = w.sk_id_curr and p.dpd = w.max_dpd
    group by p.sk_id_curr
)
select case when d.worst_due_day >= -365 then '1. last 12 months'
            when d.worst_due_day >= -730 then '2. 12 to 24 months ago'
            else '3. more than 24 months ago' end as when_worst_dpd_happened,
       round(100 * avg(case when c.is_default then 1.0 else 0.0 end), 2) as default_rate_pct,
       count(*) as n_clients
from worst as w
inner join worst_day as d on w.sk_id_curr = d.sk_id_curr
inner join marts.dim_client as c on w.sk_id_curr = c.sk_id_curr
where w.max_dpd > 90
group by 1 order by 1""",
        {
            "graph.dimensions": ["WHEN_WORST_DPD_HAPPENED"],
            "graph.metrics": ["DEFAULT_RATE_PCT"],
        },
    ),
    (
        "Clients with unpaid installments vs the average",
        "bar",
        """select 'All clients' as segment, 1 as ord,
       round(100 * sum(n_defaults) / sum(n_clients), 2) as default_rate_pct
from marts.mart_credit_risk where dimension = 'name_contract_type'
union all
select 'Without unpaid installments', 2,
       round(100 * (sum(n_defaults) - max(case when dimension_value = 'unpaid' then n_defaults end))
             / (sum(n_clients) - max(case when dimension_value = 'unpaid' then n_clients end)), 2)
from marts.mart_credit_risk where dimension = 'dpd_bucket'
union all
select 'With unpaid installments', 3, round(100 * default_rate, 2)
from marts.mart_credit_risk where dimension = 'dpd_bucket' and dimension_value = 'unpaid'
order by ord""",
        {"graph.dimensions": ["SEGMENT"], "graph.metrics": ["DEFAULT_RATE_PCT"]},
    ),
    (
        "Default rate by client seniority (months since first loan)",
        "bar",
        """select dimension_value as tenure_cohort,
       round(100 * default_rate, 2) as default_rate_pct,
       n_clients
from marts.mart_credit_risk
where dimension = 'tenure_cohort'
order by case when dimension_value = 'no_prior_loan' then '0' else dimension_value end""",
        {"graph.dimensions": ["TENURE_COHORT"], "graph.metrics": ["DEFAULT_RATE_PCT"]},
    ),
    (
        "Credit exposure by income band (dataset currency units, billions)",
        "bar",
        """select dimension_value as income_band,
       round(exposure_application_credit / 1e9, 2) as exposure_bn,
       round(100 * default_rate, 2) as default_rate_pct
from marts.mart_credit_risk
where dimension = 'income_band'
order by dimension_value""",
        {"graph.dimensions": ["INCOME_BAND"], "graph.metrics": ["EXPOSURE_BN"]},
    ),
]

NOTE = (
    "**How to read the delinquency chart.** The lifetime '90+' bucket (9.5%) defaults *less* than "
    "'61-90' (14.0%) only because of recency: 89% of its clients had that worst delay more than "
    "24 months before applying. When the worst delay was in the last 12 months, the default rate "
    "is 25.0% (164 clients, a small group). Amounts are in dataset currency units: the source does "
    "not say which currency."
)


class Metabase:
    def __init__(self, base: str):
        self.base = base.rstrip("/")
        self.token: str | None = None

    def call(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("X-Metabase-Session", self.token)
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            sys.exit(f"{method} {path} -> {exc.code}: {exc.read().decode()[:600]}")
        return json.loads(raw) if raw else None


def wait_until_up(mb: Metabase) -> None:
    for _ in range(60):
        try:
            if mb.call("GET", "/api/health").get("status") == "ok":
                return
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(5)
    sys.exit("Metabase did not become healthy in 5 minutes.")


def login_or_setup(mb: Metabase, email: str, password: str) -> None:
    props = mb.call("GET", "/api/session/properties")
    if props.get("setup-token"):
        print("First start: creating the admin user.")
        mb.call(
            "POST",
            "/api/setup",
            {
                "token": props["setup-token"],
                "user": {
                    "email": email,
                    "password": password,
                    "first_name": "Credit",
                    "last_name": "Risk",
                    "site_name": "Credit risk pipeline",
                },
                "prefs": {"site_name": "Credit risk pipeline", "allow_tracking": False},
            },
        )
    mb.token = mb.call("POST", "/api/session", {"username": email, "password": password})["id"]


def ensure_database(mb: Metabase) -> int:
    for db in mb.call("GET", "/api/database")["data"]:
        if db["name"] == "Snowflake dev (marts)":
            return db["id"]
    env = os.environ
    details = {
        "account": env["SNOWFLAKE_ACCOUNT"],
        "user": env["SNOWFLAKE_USER"],
        "role": env.get("SNOWFLAKE_ROLE", "CREDIT_RISK_ENGINEER"),
        "warehouse": env.get("SNOWFLAKE_WAREHOUSE", "CREDIT_RISK_WH"),
        "db": env.get("SNOWFLAKE_DATABASE_DEV", "CREDIT_RISK_DEV"),
        "use-password": False,
        "private-key-options": "local",
        "private-key-path": "/run/snowflake/rsa_key.p8",
        "schema-filters-type": "inclusion",
        "schema-filters-patterns": "MARTS",
    }
    created = mb.call(
        "POST",
        "/api/database",
        {
            "name": "Snowflake dev (marts)",
            "engine": "snowflake",
            "details": details,
            "is_full_sync": True,
            "auto_run_queries": False,
        },
    )
    return created["id"]


def build_dashboard(mb: Metabase, db_id: int) -> int:
    for coll in mb.call("GET", "/api/collection"):
        if coll.get("name") == "Credit risk":
            collection_id = coll["id"]
            break
    else:
        collection_id = mb.call("POST", "/api/collection", {"name": "Credit risk"})["id"]

    # recreate: drop the previous dashboard and cards of this collection
    for item in mb.call("GET", f"/api/collection/{collection_id}/items")["data"]:
        if item["model"] in ("dashboard", "card"):
            mb.call("DELETE", f"/api/{item['model']}/{item['id']}")

    card_ids = []
    for name, display, sql, viz in QUESTIONS:
        card = mb.call(
            "POST",
            "/api/card",
            {
                "name": name,
                "display": display,
                "collection_id": collection_id,
                "visualization_settings": viz,
                "dataset_query": {
                    "type": "native",
                    "database": db_id,
                    "native": {"query": sql},
                },
            },
        )
        card_ids.append(card["id"])

    dash = mb.call(
        "POST",
        "/api/dashboard",
        {
            "name": DASHBOARD_NAME,
            "collection_id": collection_id,
            "description": "Default risk story on the dbt marts (Snowflake dev).",
        },
    )
    layout = [  # (card index, col, row, width, height); the grid is 24 columns wide
        (0, 0, 0, 6, 4),
        (3, 6, 0, 18, 4),
        (1, 0, 4, 12, 7),
        (2, 12, 4, 12, 7),
        (4, 0, 12, 12, 7),
        (5, 12, 12, 12, 7),
    ]
    dashcards = [
        {
            "id": -(i + 1),
            "card_id": card_ids[idx],
            "col": col,
            "row": row,
            "size_x": w,
            "size_y": h,
            "series": [],
            "parameter_mappings": [],
            "visualization_settings": {},
        }
        for i, (idx, col, row, w, h) in enumerate(layout)
    ]
    dashcards.append(
        {
            "id": -99,
            "card_id": None,
            "col": 0,
            "row": 19,
            "size_x": 24,
            "size_y": 3,
            "series": [],
            "parameter_mappings": [],
            "visualization_settings": {
                "virtual_card": {
                    "name": None,
                    "display": "text",
                    "visualization_settings": {},
                    "dataset_query": {},
                    "archived": False,
                },
                "text": NOTE,
            },
        }
    )
    mb.call("PUT", f"/api/dashboard/{dash['id']}", {"dashcards": dashcards})
    return dash["id"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--url", default="http://localhost:3000")
    args = parser.parse_args()
    missing = [
        k
        for k in ("MB_ADMIN_EMAIL", "MB_ADMIN_PASSWORD", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER")
        if not os.environ.get(k)
    ]
    if missing:
        sys.exit(f"Missing environment variables: {', '.join(missing)} (see .env.example)")

    mb = Metabase(args.url)
    wait_until_up(mb)
    login_or_setup(mb, os.environ["MB_ADMIN_EMAIL"], os.environ["MB_ADMIN_PASSWORD"])
    db_id = ensure_database(mb)
    print(f"Snowflake connection ready (database id {db_id}).")
    dash_id = build_dashboard(mb, db_id)
    print(f"Dashboard ready: {args.url}/dashboard/{dash_id}")


if __name__ == "__main__":
    main()
