#!/usr/bin/env python3
"""apply.py — Sinh và chạy lệnh ALTER/sp_configure/sysctl trên dev/test.

Import shared helpers từ collect.py (cùng thư mục scripts/):
    load_credentials, build_oracle_dsn, build_mssql_conn_str,
    get_ssh_client, load_version_matrix
"""
import argparse, yaml, sys, os
import oracledb
import pyodbc
# Import helpers đã định nghĩa trong collect.py
from collect import (
    load_credentials, build_oracle_dsn,
    build_mssql_conn_str, get_ssh_client,
)

PROD_GUARD = True   # KHÔNG BAO GIỜ tắt flag này

def load_version_matrix(db_type: str, version: str) -> dict:
    """Trả về dict {param_name: {dynamic, requires_restart}} cho version này."""
    path = f"version_matrix/{db_type}/{version}.yaml"
    data = yaml.safe_load(open(path))
    return {p["name"]: p for p in data["supported_params"]}

# ─── DB apply helpers ────────────────────────────────────────────────────────

def apply_oracle_param(db_cfg, creds, param_name, expected_value,
                       matrix_entry, dry_run=True):
    """Sinh và chạy lệnh Oracle ALTER SYSTEM."""
    scope = "BOTH" if matrix_entry.get("dynamic", True) else "SPFILE"
    sql   = f"ALTER SYSTEM SET {param_name} = {expected_value} SCOPE={scope}"
    if dry_run:
        print(f"  [DRY-RUN] {sql}")
        return
    dsn  = build_oracle_dsn(db_cfg)
    conn = oracledb.connect(user=creds["user"], password=creds["password"], dsn=dsn)
    conn.cursor().execute(sql)
    conn.close()
    print(f"  [APPLIED] {sql}")
    if scope == "SPFILE":
        print(f"  [WARNING] {param_name} cần restart DB để có hiệu lực!")

def apply_mssql_param(db_cfg, creds, param_name, expected_value,
                      matrix_entry, dry_run=True):
    """Sinh và chạy lệnh MSSQL sp_configure."""
    sql1 = f"EXEC sp_configure '{param_name}', {expected_value}"
    sql2 = "RECONFIGURE WITH OVERRIDE"
    if dry_run:
        print(f"  [DRY-RUN] {sql1}; {sql2}")
        return
    conn_str = build_mssql_conn_str(db_cfg, creds)
    conn = pyodbc.connect(conn_str)
    cur  = conn.cursor()
    cur.execute(sql1)
    cur.execute(sql2)
    conn.commit()
    conn.close()
    print(f"  [APPLIED] {sql1}")

# ─── OS apply helpers ────────────────────────────────────────────────────────

def apply_linux_sysctl(os_cfg, creds, param_name, expected_value, dry_run=True):
    """Apply sysctl param qua SSH — cả runtime (sysctl -w) và persistent."""
    cmd_runtime    = f"sysctl -w {param_name}={expected_value}"
    cmd_persistent = (
        f"echo '{param_name} = {expected_value}'"
        f" | sudo tee /etc/sysctl.d/99-dbparams-{param_name.replace('.','-')}.conf"
    )
    if dry_run:
        print(f"  [DRY-RUN] {cmd_runtime}  (+persist)")
        return
    client = get_ssh_client(os_cfg, creds)
    for cmd in [f"sudo {cmd_runtime}", cmd_persistent]:
        _, stdout, stderr = client.exec_command(cmd)
        rc = stdout.channel.recv_exit_status()
        if rc != 0:
            raise RuntimeError(f"sysctl apply failed [{cmd}]: {stderr.read().decode()}")
    client.close()
    print(f"  [APPLIED] {cmd_runtime} + persisted")

def apply_linux_limit(os_cfg, creds, target_user, limit_type, value, dry_run=True):
    """Apply limits.conf entry qua SSH, lưu vào /etc/security/limits.d/."""
    dest = f"/etc/security/limits.d/99-{target_user}-dbparams.conf"
    content = (
        f"{target_user} soft {limit_type} {value}\n"
        f"{target_user} hard {limit_type} {value}"
    )
    cmd = f"echo '{content}' | sudo tee {dest}"
    if dry_run:
        print(f"  [DRY-RUN] limits: {target_user} {limit_type}={value} → {dest}")
        return
    client = get_ssh_client(os_cfg, creds)
    _, stdout, stderr = client.exec_command(cmd)
    rc = stdout.channel.recv_exit_status()
    if rc != 0:
        raise RuntimeError(f"limits apply failed: {stderr.read().decode()}")
    client.close()
    print(f"  [APPLIED] limits: {target_user} {limit_type}={value}")

# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Apply DB + OS params")
    parser.add_argument("--system",  required=True)
    parser.add_argument("--env",     required=True)
    parser.add_argument("--dry-run", action="store_true", default=False)
    args = parser.parse_args()

    # ── PROD GUARD: từ chối cứng, không phụ thuộc tham số Jenkins ──
    if PROD_GUARD and args.env == "prod":
        print("[ERROR] apply.py từ chối chạy với env=prod. Dừng lại.")
        sys.exit(1)

    diff_file = f"tmp/{args.system}_{args.env}_diff.yaml"
    if not os.path.exists(diff_file):
        raise FileNotFoundError(f"Không tìm thấy diff file: {diff_file}. Chạy diff.py trước.")

    diff_data = yaml.safe_load(open(diff_file))
    registry  = yaml.safe_load(open("inventory/db_registry.yaml"))
    sys_cfg   = next(s for s in registry["systems"] if s["system_id"] == args.system)
    env_cfg   = sys_cfg["environments"][args.env]
    matrix    = load_version_matrix(sys_cfg["db_type"], sys_cfg["version"])
    db_creds  = load_credentials(env_cfg["db"]["credential_id"])
    os_creds  = load_credentials(env_cfg["os"]["credential_id"])

    print(f"[apply] {args.system}/{args.env} — dry_run={args.dry_run}")

    # ── Apply DB params ──
    for item in diff_data.get("db_params", []):
        if item["status"] not in ("DRIFT", "MISSING"):
            continue
        entry = matrix.get(item["param"], {})
        if sys_cfg["db_type"] == "oracle":
            apply_oracle_param(env_cfg["db"], db_creds,
                               item["param"], item["expected"], entry, args.dry_run)
        else:
            apply_mssql_param(env_cfg["db"], db_creds,
                              item["param"], item["expected"], entry, args.dry_run)

    # ── Apply OS params ──
    for item in diff_data.get("os_params", []):
        if item["status"] not in ("DRIFT", "MISSING"):
            continue
        if item["section"] == "sysctl_params":
            apply_linux_sysctl(env_cfg["os"], os_creds,
                               item["param"], item["expected"], args.dry_run)
        elif item["section"] == "limits_conf":
            bsl = yaml.safe_load(open(sys_cfg["baseline_os"]))
            lim = bsl["limits_conf"][item["param"]]
            apply_linux_limit(env_cfg["os"], os_creds, lim["target_user"],
                              lim["limit_type"], item["expected"], args.dry_run)

    # ── Object diffs: CHỈ alert, KHÔNG tự apply ──
    obj_issues = [x for x in diff_data.get("db_objects", []) if x["status"] != "OK"]
    if obj_issues:
        print(f"[apply] {len(obj_issues)} object issues — không tự apply, cần DBA review")

if __name__ == "__main__":
    main()
