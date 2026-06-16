#!/usr/bin/env python3
"""Standalone HTML marketing report — queries Postgres analytics, renders Plotly.

Zero Superset dependency. Outputs a single self-contained HTML file
that marketing can open in any browser. Charts are interactive Plotly.

Usage:
    python3 scripts/export_marketing_report.py [--output report.html]
"""
import argparse, json, os, sys, textwrap
from datetime import datetime
from decimal import Decimal

PG_HOST = os.environ.get("PG_ANALYTICS_HOST", "svc-postgres-analytics")
PG_PORT = int(os.environ.get("PG_ANALYTICS_PORT", "5432"))
PG_USER = os.environ.get("PG_ANALYTICS_USER", "analytics")
PG_PASSWORD = os.environ.get("PG_ANALYTICS_PASSWORD", "analytics")
PG_DB = os.environ.get("PG_ANALYTICS_DB", "nyc_analytics")

TPL = textwrap.dedent("""\
    <!DOCTYPE html>
    <html lang="en">
    <head>
    <meta charset="utf-8">
    <title>NYC Taxi Gold Analytics — Marketing Report</title>
    <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
    <style>
      * {{ box-sizing: border-box; margin: 0; padding: 0; }}
      body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
             background: #f5f6fa; color: #222; padding: 20px; }}
      .header {{ text-align: center; padding: 30px 0; }}
      .header h1 {{ font-size: 28px; color: #1a1a2e; }}
      .header p {{ color: #666; margin-top: 6px; }}
      .kpi-row {{ display: flex; gap: 16px; justify-content: center; flex-wrap: wrap; margin-bottom: 20px; }}
      .kpi {{ background: linear-gradient(135deg, #667eea, #764ba2); color: #fff;
              border-radius: 10px; padding: 20px 30px; text-align: center; min-width: 160px; }}
      .kpi .val {{ font-size: 36px; font-weight: 700; }}
      .kpi .label {{ font-size: 12px; opacity: 0.85; margin-top: 4px; text-transform: uppercase; }}
      .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
      .grid-3 {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; }}
      .card {{ background: #fff; border-radius: 8px; padding: 16px;
              box-shadow: 0 1px 4px rgba(0,0,0,0.08); }}
      .card h3 {{ font-size: 14px; color: #555; margin-bottom: 10px; }}
      .section-title {{ font-size: 20px; color: #333; margin: 28px 0 12px 0;
                        border-bottom: 2px solid #1a73e8; padding-bottom: 6px; }}
      .footer {{ text-align: center; color: #999; font-size: 11px; margin-top: 40px; padding: 16px; }}
      @media (max-width: 1000px) {{ .grid-2, .grid-3 {{ grid-template-columns: 1fr; }} }}
    </style>
    </head>
    <body>
    <div class="header">
      <h1>NYC Taxi — Gold Analytics Report</h1>
      <p>Generated {ts} | {row_count} trips processed | NYC TLC Yellow Taxi Jan–Mar 2024</p>
    </div>
    {kpi_section}
    {sections}
    <div class="footer">NYC Taxi Data Pipeline — Gold Layer Analytics</div>
    </body>
    </html>
""")


def _f(v):
    if isinstance(v, Decimal):
        return float(v)
    return v


def connect():
    import psycopg2
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER,
                            password=PG_PASSWORD, dbname=PG_DB)


def query(cur, sql: str) -> list:
    cur.execute(textwrap.dedent(sql))
    return cur.fetchall()


def _plot(traces: list, layout: dict = None) -> str:
    layout = layout or {}
    layout.setdefault("margin", {"l": 0, "r": 0, "t": 0, "b": 40})
    layout.setdefault("height", 260)
    layout.setdefault("xaxis", {"tickfont": {"size": 10}})
    layout.setdefault("yaxis", {"tickfont": {"size": 10}})
    return json.dumps({"data": traces, "layout": layout, "config": {"displayModeBar": False}})


def _card(title: str, plot_json: str) -> str:
    pid = abs(hash(title))
    pj = json.loads(plot_json)
    return (f'<div class="card"><h3>{title}</h3><div id="p{pid}"></div>'
            f'<script>Plotly.newPlot("p{pid}",{json.dumps(pj["data"])},'
            f'{json.dumps(pj["layout"])},{json.dumps(pj["config"])})</script></div>')


def build(cur) -> str:
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    # KPIs
    rows = query(cur, "SELECT SUM(trips), SUM(revenue) FROM kpi_daily_overview")
    tt = float(rows[0][0] or 0)
    tr = float(rows[0][1] or 0)
    trips_fmt = f"{tt/1e6:.1f}M"
    rev_fmt = f"${tr/1e6:.1f}M"
    kpi = (f'<div class="kpi-row">'
           f'<div class="kpi"><div class="val">{trips_fmt}</div><div class="label">Total Trips</div></div>'
           f'<div class="kpi"><div class="val">{rev_fmt}</div><div class="label">Total Revenue</div></div>'
           f'</div>')

    sections = ""

    # DAILY TRENDS
    rows = query(cur, "SELECT pickup_date, trips, revenue FROM kpi_daily_overview ORDER BY pickup_date")
    d_dates = [r[0].strftime("%m-%d") if r[0] else "" for r in rows]
    sections += '<div class="section-title">Daily Trends</div><div class="grid-2">'
    sections += _card("Daily Trips", _plot([{"type": "scatter", "x": d_dates, "y": [_f(r[1]) for r in rows], "mode": "lines+markers", "marker": {"color": "#4e79a7"}}]))
    sections += _card("Daily Revenue", _plot([{"type": "scatter", "x": d_dates, "y": [_f(r[2]) for r in rows], "mode": "lines+markers", "marker": {"color": "#e15759"}}]))
    sections += '</div>'

    # HOURLY + PASSENGER
    r1 = query(cur, "SELECT pickup_hour, SUM(trip_count) FROM fact_trips_hourly GROUP BY 1 ORDER BY 1")
    r2 = query(cur, "SELECT passenger_count, SUM(trip_count) FROM ops_passenger_count_pattern GROUP BY 1 ORDER BY 1")
    sections += '<div class="grid-2">'
    sections += _card("Trips by Hour", _plot([{"type": "bar", "x": [str(r[0]) for r in r1], "y": [_f(r[1]) for r in r1], "marker": {"color": "#4e79a7"}}]))
    sections += _card("Passenger Count", _plot([{"type": "bar", "x": [str(r[0]) for r in r2], "y": [_f(r[1]) for r in r2], "marker": {"color": "#f28e2b"}}]))
    sections += '</div>'

    # PIE BREAKDOWNS
    sections += '<div class="section-title">Market Breakdowns</div><div class="grid-3">'
    for tbl, col, title, sum_col in [
        ("kpi_borough_comparison", "pickup_borough", "Borough Revenue", "revenue"),
        ("kpi_payment_trends", "payment_type", "Payment Types", "trip_count"),
        ("kpi_vendor_performance", "vendor_id", "Vendor Share", "trips"),
    ]:
        r = query(cur, f"SELECT {col}, SUM({sum_col}) FROM {tbl} GROUP BY 1 ORDER BY 2 DESC LIMIT 8")
        sections += _card(title, _plot([{"type": "pie", "labels": [str(v[0])[:20] for v in r],
            "values": [_f(v[1]) for v in r], "textinfo": "label+percent", "hole": 0.3}],
            {"showlegend": True, "legend": {"font": {"size": 10}}}))
    sections += '</div>'

    # TOP ZONES
    sections += '<div class="section-title">Top Zones</div><div class="grid-2">'
    r1 = query(cur, "SELECT pickup_zone, SUM(trip_count) FROM route_top_pickup_zones GROUP BY 1 ORDER BY 2 DESC LIMIT 10")
    r2 = query(cur, "SELECT dropoff_zone, SUM(trip_count) FROM route_top_dropoff_zones GROUP BY 1 ORDER BY 2 DESC LIMIT 10")
    sections += _card("Top Pickup Zones", _plot([{"type": "bar", "y": [str(v[0])[:25] for v in reversed(r1)], "x": [_f(v[1]) for v in reversed(r1)], "orientation": "h", "marker": {"color": "#4e79a7"}}], {"height": 300}))
    sections += _card("Top Dropoff Zones", _plot([{"type": "bar", "y": [str(v[0])[:25] for v in reversed(r2)], "x": [_f(v[1]) for v in reversed(r2)], "orientation": "h", "marker": {"color": "#e15759"}}], {"height": 300}))
    sections += '</div>'

    # OPERATIONS
    sections += '<div class="section-title">Operations</div><div class="grid-2">'
    r1 = query(cur, "SELECT distance_bucket, SUM(trip_count) FROM ops_trip_distance_distribution GROUP BY 1 ORDER BY 1")
    r2 = query(cur, "SELECT pickup_hour, SUM(trip_count) FROM ops_peak_hours_heatmap GROUP BY 1 ORDER BY 1")
    sections += _card("Trip Distance", _plot([{"type": "bar", "x": [str(v[0]) for v in r1], "y": [_f(v[1]) for v in r1], "marker": {"color": "#59a14f"}}]))
    sections += _card("Peak Hours", _plot([{"type": "bar", "x": [str(v[0]) for v in r2], "y": [_f(v[1]) for v in r2], "marker": {"color": "#f28e2b"}}]))
    sections += '</div>'

    # MONTHLY
    sections += '<div class="section-title">Monthly Summary</div><div class="grid-2">'
    r = query(cur, "SELECT pickup_month, total_revenue, trip_count FROM kpi_monthly_summary ORDER BY 1")
    months = [f"2024-{v[0]:02d}" if v[0] else "" for v in r]
    sections += _card("Revenue by Month", _plot([{"type": "bar", "x": months, "y": [_f(v[1]) for v in r], "marker": {"color": "#4e79a7"}}]))
    sections += _card("Trips by Month", _plot([{"type": "bar", "x": months, "y": [_f(v[2]) for v in r], "marker": {"color": "#59a14f"}}]))
    sections += '</div>'

    # DQ
    r = query(cur, "SELECT pickup_date, total_trips FROM dq_validation_summary ORDER BY pickup_date")
    if r:
        sections += '<div class="section-title">Data Quality</div><div class="grid-2">'
        sections += _card("Validated Trips", _plot([{"type": "scatter", "x": [v[0].strftime("%m-%d") if v[0] else "" for v in r], "y": [_f(v[1]) for v in r], "mode": "lines+markers", "marker": {"color": "#76b7b2"}}]))
        sections += '</div>'

    return TPL.format(ts=now, row_count=trips_fmt, kpi_section=kpi, sections=sections)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate standalone marketing HTML report")
    parser.add_argument("--output", "-o", default="marketing_report.html")
    args = parser.parse_args()

    conn = connect()
    cur = conn.cursor()
    html = build(cur)
    conn.close()

    with open(args.output, "w") as f:
        f.write(html)

    size_kb = os.path.getsize(args.output) / 1024
    print(f"Marketing report: {args.output} ({size_kb:.1f} KB)")
    print("Open in any browser — Plotly charts load from CDN, no server needed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
