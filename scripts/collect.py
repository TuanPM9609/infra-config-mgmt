#!/usr/bin/env python3
"""collect.py — Thu thập DB params, OS params, Object counts từ DB server.

Dependencies:
    pip install oracledb pyodbc paramiko pyyaml
"""
import argparse, yaml, os, re
import oracledb
import pyodbc
import paramiko
from datetime import datetime


# ─── Credential & connection helpers ─────────────────────────────────────────

def load_credentials(credential_id: str) -> dict:
    """Đọc credentials từ Jenkins Credentials Store qua env vars.

    Jenkins withCredentials inject theo convention:
      - usernamePassword  → <PREFIX>_USR, <PREFIX>_PSW
      - sshUserPrivateKey → <PREFIX>_USR, <PREFIX>_PSW (path đến key file tạm)

    credential_id trong db_registry.yaml khớp với prefix env var:
      "ora-prod-cred" → ORA_PROD_CRED_USR, ORA_PROD_CRED_PSW
      "ora-prod-ssh"  → ORA_PROD_SSH_USR,  ORA_PROD_SSH_PSW
    """
    env_prefix = credential_id.upper().replace("-", "_")
    usr_key = f"{env_prefix}_USR"
    psw_key = f"{env_prefix}_PSW"
    usr = os.environ.get(usr_key)
    psw = os.environ.get(psw_key)

    if not usr or not psw:
        available = sorted(k for k in os.environ if k.endswith("_USR") or k.endswith("_PSW"))
        raise EnvironmentError(
            f"Credentials không tìm thấy: {usr_key} / {psw_key}\n"
            f"Kiểm tra withCredentials block trong Jenkinsfile.\n"
            f"Env vars credentials hiện có: {available or '(không có)'}"
        )
    return {"user": usr, "password": psw}


def build_oracle_dsn(cfg: dict) -> str:
    host = cfg["host"]
    port = cfg.get("port", 1521)
    if "service" in cfg:
        return oracledb.makedsn(host, port, service_name=cfg["service"])
    elif "sid" in cfg:
        return oracledb.makedsn(host, port, sid=cfg["sid"])
    raise ValueError(f"Oracle cfg thiếu 'service' hoặc 'sid': {cfg}")


def build_mssql_conn_str(cfg: dict, creds: dict) -> str:
    driver  = cfg.get("driver", "ODBC Driver 18 for SQL Server")
    host    = cfg["host"]
    port    = cfg.get("port", 1433)
    db      = cfg["database"]
    encrypt = cfg.get("encrypt", "yes")
    trust   = cfg.get("trust_server_cert", "yes")
    return (
        f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={db};"
        f"UID={creds['user']};PWD={creds['password']};"
        f"Encrypt={encrypt};TrustServerCertificate={trust};"
    )


def get_ssh_client(ssh_cfg: dict, creds: dict) -> paramiko.SSHClient:
    """Kết nối SSH, hỗ trợ cả key-based và password auth.

    Jenkins sshUserPrivateKey inject path đến file key tạm vào PSW.
    Vấn đề thường gặp với paramiko + key file:
      1. Key là ed25519 với OpenSSH format mới (BEGIN OPENSSH PRIVATE KEY)
         → paramiko < 2.7 không đọc được, cần upgrade hoặc convert sang PEM
      2. Key có passphrase → cần truyền passphrase vào from_private_key_file
      3. Key type không xác định được → cần thử từng loại

    Fix: đọc file key thủ công qua paramiko.RSAKey/Ed25519Key/ECDSAKey
         thay vì để paramiko tự đoán type (dễ lỗi với OpenSSH format).
    """
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    host     = ssh_cfg["host"]
    username = creds["user"]
    key_path = creds["password"]   # Jenkins sshUserPrivateKey → PSW = path file key
    passphrase = ssh_cfg.get("key_passphrase")  # optional, từ db_registry.yaml

    # add for debug:
    print(f"[SSH] host={host}, user={username}")
    print(f"[SSH] key_path={key_path}, exists={os.path.exists(key_path)}")
    # Nếu PSW là path đến file key → dùng key auth
    if os.path.isfile(key_path):
    #if creds.get("key_path"):
        pkey = _load_private_key(key_path, passphrase)
        client.connect(
            hostname=host,
            username=username,
            pkey=pkey,
            look_for_keys=False,   # tắt auto-discovery key, dùng đúng key đã load
            allow_agent=False,
            timeout=30,
        )
    else:
        # Fallback: password auth (không khuyến nghị cho prod)
        client.connect(
            hostname=host,
            username=username,
            password=key_path,
            look_for_keys=False,
            allow_agent=False,
            timeout=30,
        ) 
    return client


def _load_private_key(key_path: str, passphrase: str = None) -> paramiko.PKey:
    """Load private key từ file, thử lần lượt từng key type.

    Xử lý 2 vấn đề phổ biến:
      - OpenSSH format (BEGIN OPENSSH PRIVATE KEY): ed25519, ecdsa, rsa mới
      - PEM format cũ (BEGIN RSA/EC PRIVATE KEY)

    Thứ tự thử: Ed25519 → RSA → ECDSA → DSS
    Nếu tất cả fail → raise lỗi rõ ràng hướng dẫn convert.
    """
    pp = passphrase.encode() if isinstance(passphrase, str) else passphrase
    key_types = [
        ("Ed25519",  paramiko.Ed25519Key),
        ("RSA",      paramiko.RSAKey),
        ("ECDSA",    paramiko.ECDSAKey)
       # ("DSS",      paramiko.DSSKey),
    ]
    last_error = None
    for name, klass in key_types:
        try:
            return klass.from_private_key_file(key_path, password=pp)
        except Exception as e:
            last_error = (name, e)
            continue

    raise paramiko.ssh_exception.SSHException(
        f"Không load được private key từ '{key_path}'.\n"
        f"Lỗi cuối: {last_error[0]} → {last_error[1]}\n\n"
        f"Cách fix:\n"
        f"  1. Kiểm tra format key:  head -1 {key_path}\n"
        f"     - 'BEGIN OPENSSH PRIVATE KEY' → cần paramiko >= 2.7\n"
        f"     - 'BEGIN RSA PRIVATE KEY'     → PEM format, OK\n"
        f"  2. Convert OpenSSH sang PEM nếu dùng paramiko cũ:\n"
        f"     ssh-keygen -p -m PEM -f /path/to/key\n"
        f"  3. Nếu key có passphrase, thêm vào db_registry.yaml:\n"
        f"     os:\n"
        f"       host: ...\n"
        f"       key_passphrase: your_passphrase  # hoặc dùng Jenkins secret\n"
        f"  4. Kiểm tra quyền file: chmod 600 {key_path}"
    )


# ─── Oracle collectors ────────────────────────────────────────────────────────

def collect_oracle_params(cfg: dict, creds: dict) -> dict:
    dsn  = build_oracle_dsn(cfg)
    conn = oracledb.connect(
        user=creds["user"],
        password=creds["password"],
        dsn=dsn,
    )
    cur = conn.cursor()
    cur.execute("SELECT name, value, description FROM V$PARAMETER ORDER BY name")
    result = {row[0]: {"value": row[1], "description": row[2]} for row in cur}
    conn.close()
    return result


def collect_oracle_objects(cfg: dict, creds: dict) -> dict:
    dsn  = build_oracle_dsn(cfg)
    conn = oracledb.connect(user=creds["user"], password=creds["password"], dsn=dsn)
    cur  = conn.cursor()
    cur.execute("""
        SELECT owner, object_type, COUNT(*) AS cnt
        FROM   dba_objects
        WHERE  status = 'VALID'
          AND  owner NOT IN (
               'SYS','SYSTEM','DBSNMP','OUTLN',
               'MDSYS','ORDSYS','XDB','WMSYS')
        GROUP BY owner, object_type
        ORDER BY owner, object_type
    """)
    result: dict = {}
    for owner, obj_type, cnt in cur:
        result.setdefault(owner, {})[obj_type] = cnt
    conn.close()
    return result


# ─── MSSQL collectors ─────────────────────────────────────────────────────────

def collect_mssql_params(cfg: dict, creds: dict) -> dict:
    conn_str = build_mssql_conn_str(cfg, creds)
    conn = pyodbc.connect(conn_str)
    cur  = conn.cursor()
    cur.execute("SELECT name, value_in_use, description FROM sys.configurations ORDER BY name")
    result = {row[0]: {"value": str(row[1]), "description": row[2]} for row in cur}
    conn.close()
    return result


def collect_mssql_objects(cfg: dict, creds: dict) -> dict:
    conn_str = build_mssql_conn_str(cfg, creds)
    conn = pyodbc.connect(conn_str)
    cur  = conn.cursor()
    cur.execute("""
        SELECT s.name AS schema_name, o.type_desc, COUNT(*) AS cnt
        FROM   sys.objects  o
        JOIN   sys.schemas  s ON o.schema_id = s.schema_id
        WHERE  o.is_ms_shipped = 0
        GROUP BY s.name, o.type_desc
        ORDER BY s.name, o.type_desc
    """)
    result: dict = {}
    for schema, obj_type, cnt in cur:
        result.setdefault(schema, {})[obj_type] = cnt
    conn.close()
    return result


# ─── OS collectors ────────────────────────────────────────────────────────────

def collect_linux_os_params(os_cfg: dict, creds: dict) -> dict:
    client = get_ssh_client(os_cfg, creds)

    def run(cmd: str) -> str:
        _, stdout, _ = client.exec_command(cmd)
        return stdout.read().decode().strip()

    sysctl: dict = {}
    for line in run("sysctl -a 2>/dev/null").splitlines():
        if " = " in line:
            k, v = line.split(" = ", 1)
            sysctl[k.strip()] = v.strip()

    limits: dict = {}
    raw = run("cat /etc/security/limits.conf /etc/security/limits.d/*.conf 2>/dev/null")
    for line in raw.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            parts = line.split()
            if len(parts) >= 4:
                limits[f"{parts[0]}_{parts[2]}"] = parts[3]

    schedulers: dict = {}
    raw = run(
        "for d in /sys/block/sd* /sys/block/nvme*; do "
        "[ -f $d/queue/scheduler ] && echo $(basename $d):$(cat $d/queue/scheduler); "
        "done 2>/dev/null"
    )
    for line in raw.splitlines():
        if ":" not in line:
            continue
        dev, sched = line.split(":", 1)
        m = re.search(r'\[([^\]]+)\]', sched)
        schedulers[dev.strip()] = m.group(1) if m else sched.strip()

    client.close()
    return {"sysctl": sysctl, "limits": limits, "disk_scheduler": schedulers}


def collect_windows_os_params(os_cfg: dict, creds: dict) -> dict:
    client = get_ssh_client(os_cfg, creds)

    def ps(expr: str) -> str:
        _, stdout, _ = client.exec_command(
            f'powershell -NonInteractive -Command "{expr}"'
        )
        return stdout.read().decode().strip()

    result = {
        "power_plan": ps(
            "Get-WmiObject Win32_PowerPlan -Namespace root\\cimv2\\power "
            "| Where-Object { $_.IsActive } | Select-Object -ExpandProperty ElementName"
        ),
        "page_file_mb": ps("(Get-WmiObject Win32_PageFileSetting).InitialSize"),
        "lock_pages_in_memory": ps(
            "(Get-ItemProperty 'HKLM:\\SYSTEM\\CurrentControlSet\\Control"
            "\\Session Manager\\Memory Management').LockPagesPrivilege"
        ),
        "tcp_timestamps": ps("(Get-NetTCPSetting).Timestamps | Select-Object -Unique"),
    }
    client.close()
    return result


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Collect DB + OS params")
    parser.add_argument("--system", required=True)
    parser.add_argument("--env",    required=True)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    out_file = args.output or f"tmp/{args.system}_{args.env}_state.yaml"
    os.makedirs(os.path.dirname(out_file) or ".", exist_ok=True)

    registry = yaml.safe_load(open("inventory/db_registry.yaml"))
    sys_cfg  = next((s for s in registry["systems"] if s["system_id"] == args.system), None)
    if not sys_cfg:
        raise ValueError(f"system_id '{args.system}' không tồn tại trong db_registry.yaml")
    if args.env not in sys_cfg["environments"]:
        raise ValueError(f"env '{args.env}' không có trong system '{args.system}'")

    env_cfg  = sys_cfg["environments"][args.env]
    db_creds = load_credentials(env_cfg["db"]["credential_id"])
    os_creds = load_credentials(env_cfg["os"]["credential_id"])

    result = {
        "system":       args.system,
        "env":          args.env,
        "collected_at": datetime.utcnow().isoformat() + "Z",
        "db_type":      sys_cfg["db_type"],
        "version":      sys_cfg["version"],
    }

    print(f"[collect] {args.system}/{args.env} — db_type={sys_cfg['db_type']}")

    if sys_cfg["db_type"] == "oracle":
        result["db_params"]  = collect_oracle_params(env_cfg["db"], db_creds)
        result["db_objects"] = collect_oracle_objects(env_cfg["db"], db_creds)
    elif sys_cfg["db_type"] == "mssql":
        result["db_params"]  = collect_mssql_params(env_cfg["db"], db_creds)
        result["db_objects"] = collect_mssql_objects(env_cfg["db"], db_creds)
    else:
        raise ValueError(f"db_type không hỗ trợ: {sys_cfg['db_type']}")

    os_type = env_cfg.get("os_type", "linux")
    if os_type == "linux":
        result["os_params"] = collect_linux_os_params(env_cfg["os"], os_creds)
    elif os_type == "windows":
        result["os_params"] = collect_windows_os_params(env_cfg["os"], os_creds)
    else:
        print(f"[collect] Warning: os_type '{os_type}' chưa hỗ trợ, bỏ qua OS collect")

    yaml.dump(result, open(out_file, "w"), allow_unicode=True, sort_keys=False)
    print(f"[collect] Done → {out_file}")


if __name__ == "__main__":
    main()
