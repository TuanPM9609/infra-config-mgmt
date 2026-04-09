#!/usr/bin/env python3
"""diff.py — So sánh prod baseline vs actual env, áp dụng sync_rule."""
import argparse, yaml, re

def apply_sync_rule(prod_value, rule_cfg: dict, env: str):
    """Tính expected value cho env theo sync_rule."""
    rule = rule_cfg.get("sync_rule", "copy")

    if rule == "skip":
        return None   # không quản lý param này

    if rule == "copy":
        return prod_value

    if rule == "fixed":
        return str(rule_cfg["fixed_values"][env])

    if rule == "scale_down":
        ratio = rule_cfg["ratios"][env]
        # Xử lý giá trị memory có unit: 4G, 512M, 2048K
        m = re.match(r'(\d+(?:\.\d+)?)([GMKB]?)', str(prod_value), re.IGNORECASE)
        if m:
            num, unit = float(m.group(1)), m.group(2).upper()
            return f"{int(num * ratio)}{unit}"
        return int(float(prod_value) * ratio)

    raise ValueError(f"Unknown sync_rule: {rule}")

def load_version_matrix(db_type: str, version: str) -> set:    # dùng nội bộ trong diff.py
    """Trả về set param hợp lệ cho db_type+version này."""
    path = f"version_matrix/{db_type}/{version}.yaml"
    data = yaml.safe_load(open(path))
    return {p["name"] for p in data["supported_params"]}

def diff_db_params(baseline_file, prod_state, target_state, env, db_type, version) -> list:
    """So sánh DB params, trả về list kết quả."""
    baseline = yaml.safe_load(open(baseline_file))["parameters"]
    valid_params = load_version_matrix(db_type, version)
    results = []

    for param_name, rule_cfg in baseline.items():
        # Bỏ qua param không hợp lệ với version này
        if param_name not in valid_params:
            continue

        prod_val = prod_state["db_params"].get(param_name, {}).get("value")
        expected = apply_sync_rule(prod_val, rule_cfg, env)
        if expected is None:
            results.append({"param": param_name, "status": "SKIP"})
            continue

        actual = target_state["db_params"].get(param_name, {}).get("value")
        if actual is None:
            status = "MISSING"
        elif str(actual).strip() != str(expected).strip():
            status = "DRIFT"
        else:
            status = "OK"

        results.append({
            "param": param_name, "status": status,
            "prod_value": prod_val, "expected": expected, "actual": actual
        })
    return results

def diff_os_params(baseline_file, prod_os, target_os, env) -> list:
    """So sánh OS params (sysctl + limits + scheduler)."""
    baseline = yaml.safe_load(open(baseline_file))
    results = []

    for section in ["sysctl_params", "limits_conf", "disk_scheduler"]:
        for param_name, rule_cfg in baseline.get(section, {}).items():
            prod_val = prod_os.get(section.replace("_params","").replace("_conf",""), {}).get(param_name)
            expected = apply_sync_rule(prod_val, rule_cfg, env)
            if expected is None:
                results.append({"param": f"[{section}] {param_name}", "status": "SKIP"})
                continue
            actual = target_os.get(section.replace("_params","").replace("_conf",""), {}).get(param_name)
            status = "MISSING" if actual is None else ("DRIFT" if str(actual) != str(expected) else "OK")
            results.append({
                "section": section, "param": param_name, "status": status,
                "prod_value": prod_val, "expected": expected, "actual": actual
            })
    return results

def diff_objects(baseline_file, prod_objects, target_objects) -> list:
    """So sánh số lượng object theo schema và loại."""
    baseline = yaml.safe_load(open(baseline_file))["objects"]["by_schema"]
    thresholds = yaml.safe_load(open(baseline_file)).get("diff_thresholds", {})
    critical_types = set(thresholds.get("critical_object_types", []))
    results = []

    for schema, obj_counts in baseline.items():
        for obj_type, expected_count in obj_counts.items():
            actual_count = target_objects.get(schema, {}).get(obj_type, 0)
            prod_count   = prod_objects.get(schema, {}).get(obj_type, 0)

            if actual_count == prod_count:
                status = "OK"
            elif actual_count < prod_count:
                status = "MISSING"
            else:
                status = "EXTRA"

            is_critical = obj_type in critical_types
            results.append({
                "schema": schema, "object_type": obj_type,
                "prod_count": prod_count, "actual_count": actual_count,
                "diff": actual_count - prod_count,
                "status": status, "critical": is_critical
            })
    return results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--system",  required=True)
    parser.add_argument("--env",     required=True)
    parser.add_argument("--output",  default="diff_result.yaml")
    args = parser.parse_args()

    registry = yaml.safe_load(open("inventory/db_registry.yaml"))
    sys_cfg  = next(s for s in registry["systems"] if s["system_id"] == args.system)

    prod_state   = yaml.safe_load(open(f"tmp/{args.system}_prod_state.yaml"))
    target_state = yaml.safe_load(open(f"tmp/{args.system}_{args.env}_state.yaml"))

    diff = {
        "system": args.system, "env": args.env,
        "db_params":  diff_db_params(sys_cfg["baseline_db"], prod_state, target_state,
                                     args.env, sys_cfg["db_type"], sys_cfg["version"]),
        "os_params":  diff_os_params(sys_cfg["baseline_os"], prod_state["os_params"],
                                     target_state["os_params"], args.env),
        "db_objects": diff_objects(sys_cfg["baseline_objects"],
                                   prod_state["db_objects"], target_state["db_objects"])
    }
    yaml.dump(diff, open(args.output, "w"), allow_unicode=True)
    # In summary ra stdout để Jenkins log
    total = len(diff["db_params"]) + len(diff["os_params"])
    drifts = sum(1 for x in diff["db_params"]+diff["os_params"] if x["status"]=="DRIFT")
    missing = sum(1 for x in diff["db_objects"] if x["status"]=="MISSING")
    print(f"[diff] {args.system}/{args.env}: {drifts} drifts, {missing} missing objects")

if __name__ == "__main__":
    main()
