#!/usr/bin/env python3
"""report.py — Đọc tất cả diff_result.yaml, sinh HTML report tổng hợp."""
import argparse, yaml, json, os
from datetime import datetime
from jinja2 import Template

HTML_TEMPLATE = """
<!DOCTYPE html><html><head><meta charset="utf-8">
<title>DB Config Report — {{ run_id }}</title>
<style>
  body { font-family: Arial, sans-serif; margin: 2rem; background: #f8fafc; }
  h1 { color: #1e3a5f; }
  .system-block { background:#fff; border-radius:8px; padding:1.5rem;
                  margin-bottom:1.5rem; border-left:4px solid #2563eb; box-shadow:0 1px 4px rgba(0,0,0,.08); }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th { background:#1e3a5f; color:#fff; padding:6px 12px; text-align:left; }
  td { padding:5px 12px; border-bottom:1px solid #e5e7eb; }
  tr:nth-child(even) { background:#f3f4f6; }
  .OK     { color:#065f46; font-weight:600; }
  .DRIFT  { color:#92400e; font-weight:600; background:#fef3c7; }
  .MISSING{ color:#8a2040; font-weight:600; background:#fde8ef; }
  .EXTRA  { color:#4c3fa0; font-weight:600; background:#ede9fe; }
  .COUNT_DIFF { color:#b45309; font-weight:600; background:#fef3c7; }
  .summary-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:1rem; margin:1rem 0; }
  .metric { background:#f3f4f6; border-radius:6px; padding:1rem; text-align:center; }
  .metric .n { font-size:2rem; font-weight:700; color:#1e3a5f; }
</style></head><body>
<h1>Infrastructure Config Report — {{ run_date }}</h1>
<div class="summary-grid">
  <div class="metric"><div class="n">{{ total_systems }}</div>Systems</div>
  <div class="metric"><div class="n" style="color:#065f46">{{ ok_count }}</div>In Sync</div>
  <div class="metric"><div class="n" style="color:#92400e">{{ drift_count }}</div>Drifts</div>
  <div class="metric"><div class="n" style="color:#8a2040">{{ obj_issues }}</div>Object Issues</div>
</div>
{% for sys in systems %}
<div class="system-block">
  <h2>{{ sys.name }} ({{ sys.db_type }} {{ sys.version }})</h2>
  {% for env_result in sys.envs %}
  <h3>{{ env_result.env }}</h3>
  <h4>DB Parameters</h4>
  <table><tr><th>Parameter</th><th>Status</th><th>Prod</th><th>Expected</th><th>Actual</th></tr>
  {% for r in env_result.db_params %}{% if r.status != "SKIP" %}
  <tr><td>{{r.param}}</td><td class="{{r.status}}">{{r.status}}</td>
      <td>{{r.prod_value}}</td><td>{{r.expected}}</td><td>{{r.actual}}</td></tr>
  {% endif %}{% endfor %}</table>
  <h4>OS Parameters</h4>
  <table><tr><th>Section</th><th>Parameter</th><th>Status</th><th>Expected</th><th>Actual</th></tr>
  {% for r in env_result.os_params %}{% if r.status != "SKIP" %}
  <tr><td>{{r.section}}</td><td>{{r.param}}</td><td class="{{r.status}}">{{r.status}}</td>
      <td>{{r.expected}}</td><td>{{r.actual}}</td></tr>
  {% endif %}{% endfor %}</table>
  <h4>DB Object Counts</h4>
  <table><tr><th>Schema</th><th>Type</th><th>Prod</th><th>Dev/Test</th><th>Diff</th><th>Status</th></tr>
  {% for r in env_result.db_objects %}
  <tr><td>{{r.schema}}</td><td>{{r.object_type}}</td>
      <td>{{r.prod_count}}</td><td>{{r.actual_count}}</td>
      <td>{{r.diff}}</td><td class="{{r.status}}">{{r.status}}</td></tr>
  {% endfor %}</table>
  {% endfor %}
</div>{% endfor %}
</body></html>"""

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    registry = yaml.safe_load(open("inventory/db_registry.yaml"))
    all_systems = []
    total_drift = 0; total_obj_issues = 0

    for sys_cfg in registry["systems"]:
        sys_data = { "name": sys_cfg["name"], "db_type": sys_cfg["db_type"],
                     "version": sys_cfg["version"], "envs": [] }
        for env in ["dev", "test"]:
            diff_file = f"tmp/{sys_cfg['system_id']}_{env}_diff.yaml"
            if not os.path.exists(diff_file): continue
            diff = yaml.safe_load(open(diff_file))
            sys_data["envs"].append(diff)
            total_drift      += sum(1 for x in diff["db_params"]+diff["os_params"] if x["status"]=="DRIFT")
            total_obj_issues += sum(1 for x in diff["db_objects"] if x["status"]!="OK")
        all_systems.append(sys_data)

    html = Template(HTML_TEMPLATE).render(
        run_id=args.run_id, run_date=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        total_systems=len(all_systems),
        ok_count=len(all_systems)-total_drift, drift_count=total_drift,
        obj_issues=total_obj_issues, systems=all_systems
    )
    out = f"reports/{datetime.utcnow().strftime('%Y%m%d')}_{args.run_id}_report.html"
    open(out, "w").write(html)
    print(f"[report] Generated: {out}")

if __name__ == "__main__":
    main()
