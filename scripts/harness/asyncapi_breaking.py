#!/usr/bin/env python3
"""Breaking-change gate for apis/asyncapi.yaml (the pinned producer contract).

Compares the BASE revision of the spec against the HEAD revision and exits 1
on changes that break the byte-copy pinners (the refresh-is-a-copy contract):

  1. removed message      a message documented in base is gone in head — a
                          components.messages key deleted, or a message
                          dropped from a channel's publish/subscribe list;
  2. removed/renamed      a field the base payload REQUIRED (envelope or
     required field       data) no longer exists in head; a rename surfaces
                          as a removal;
  3. type narrowing       a shared field's type set shrank, a const/enum (or
                          format) restriction appeared, an enum lost values,
                          or an optional field became required.

Additive changes (new messages, new optional fields, new enum values) are NOT
breaking: fleet consumers tolerate unknown additive keys (ADR-0027).

Usage: asyncapi_breaking.py BASE_YAML HEAD_YAML        (exit 0 = no breaking)
       asyncapi_breaking.py --self-test                (no PyYAML needed)
Pinning consumers: warehouse-docs network/model/contract-edges.json (read-only),
cross-checked against each consumer's contracts/pinned.json@develop; this repo's
own projector also self-pins the spec (warehouse.wes.analytics).
"""
import argparse
import sys

PINNERS = ("exception-management", "fulfillment-execution", "network-fulfillment",
           "order-management")  # contract-edges.json, origin/main: warehouse.work-planning.events
SELF_PIN = "wes-work-planning (own projector, warehouse.wes.analytics)"


def load(path):
    try:
        import yaml
    except ImportError:
        sys.exit("PyYAML required: pip install --require-hashes -r .github/requirements/contract-breaking.txt")
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def resolve(spec, node, depth=0):
    """Resolve $ref and merge allOf into one plain schema dict (shallow)."""
    if not isinstance(node, dict) or depth > 20:
        return node if isinstance(node, dict) else {}
    if "$ref" in node:
        target = spec
        for part in node["$ref"].lstrip("#/").split("/"):
            target = target[part]
        return resolve(spec, target, depth + 1)
    if "allOf" in node:
        merged = {k: v for k, v in node.items() if k != "allOf"}
        for branch in node["allOf"]:
            branch = resolve(spec, branch, depth + 1)
            for key, val in branch.items():
                if key == "properties" and key in merged:
                    merged["properties"] = {**merged["properties"], **val}
                else:
                    merged[key] = val
        return merged
    return node


def messages(spec):
    out = {}
    for name, msg in (spec.get("components", {}).get("messages", {}) or {}).items():
        out[name] = resolve(spec, msg.get("payload", {})) if isinstance(msg, dict) else {}
    return out


def channel_messages(spec):
    """channel -> set of referenced components.messages names."""
    out = {}
    for channel, item in (spec.get("channels", {}) or {}).items():
        names = set()
        for op in ("publish", "subscribe"):
            msg = (item or {}).get(op, {}).get("message") if isinstance(item, dict) else None
            entries = msg.get("oneOf", [msg]) if isinstance(msg, dict) else []
            for entry in entries:
                ref = entry.get("$ref", "") if isinstance(entry, dict) else ""
                if ref.startswith("#/components/messages/"):
                    names.add(ref.rsplit("/", 1)[1])
        out[channel] = names
    return out


def _types(node):
    t = node.get("type")
    return {t} if isinstance(t, str) else (set(t) if isinstance(t, list) else set())


def narrowed(base, head):
    if _types(base) and _types(head) and not _types(base) <= _types(head):
        return f"type {sorted(_types(base))} -> {sorted(_types(head))}"
    if "const" in head and "const" not in base:
        return f"const '{head['const']}' introduced"
    if "enum" in head and "enum" not in base:
        return f"enum {head['enum']} introduced"
    if "enum" in base and "enum" in head:
        dropped = set(base["enum"]) - set(head["enum"])
        if dropped:
            return f"enum value(s) {sorted(dropped)} dropped"
    if "format" in head and "format" not in base:
        return f"format '{head['format']}' introduced"
    return None


def diff_schema(path, base, head, base_spec, head_spec, out):
    props = base.get("properties", {}) or {}
    for name in base.get("required", []) or []:
        if name not in (head.get("properties", {}) or {}):
            out.append(f"{path}: required field '{name}' removed or renamed")
    for name, node in props.items():
        if name not in (head.get("properties", {}) or {}):
            if name in (base.get("required", []) or []):
                out.append(f"{path}: required field '{name}' removed or renamed")
            continue
        other = head["properties"][name]
        if name in (head.get("required", []) or []) and name not in (base.get("required", []) or []):
            out.append(f"{path}: field '{name}' became required")
        why = narrowed(node, other)
        if why:
            out.append(f"{path}: field '{name}' narrowed ({why})")
        diff_schema(f"{path}.{name}", resolve(base_spec, node), resolve(head_spec, other),
                    base_spec, head_spec, out)


def compare(base_spec, head_spec):
    out = []
    base_msgs, head_msgs = messages(base_spec), messages(head_spec)
    for name in sorted(set(base_msgs) - set(head_msgs)):
        out.append(f"message '{name}' removed")
    for name in sorted(set(base_msgs) & set(head_msgs)):
        diff_schema(f"message '{name}'", base_msgs[name], head_msgs[name], base_spec, head_spec, out)
    base_ch, head_ch = channel_messages(base_spec), channel_messages(head_spec)
    for channel in sorted(set(base_ch) - set(head_ch)):
        if base_ch[channel]:
            out.append(f"channel '{channel}' removed (carried {len(base_ch[channel])} message(s))")
    for channel in sorted(set(base_ch) & set(head_ch)):
        for name in sorted(base_ch[channel] - head_ch[channel]):
            out.append(f"message '{name}' removed from channel '{channel}'")
    return list(dict.fromkeys(out))  # a field can be flagged on both passes


def self_test():
    envelope = {"type": "object", "required": ["specversion", "type", "data"],
                "properties": {"specversion": {"type": "string", "enum": ["1.0"]},
                               "type": {"type": "string"}, "data": {"type": "object"}}}
    import copy

    def spec(msgs, channel):
        schemas = {name: copy.deepcopy(envelope) for name in msgs}
        return {"channels": {"t": {"publish": {"message": {"oneOf": [
            {"$ref": f"#/components/messages/{n}"} for n in channel]}}}},
                "components": {"messages": {n: {"payload": {"$ref": f"#/components/schemas/{n}"}}
                                             for n in msgs},
                               "schemas": schemas}}

    base = spec(["M", "R", "K"], ["M", "R", "K"])                      # R will vanish, K loses 'data'
    assert compare(base, spec(["M", "R", "K"], ["M", "R", "K"])) == []
    assert compare(base, spec(["M", "R", "K", "New"], ["M", "R", "K", "New"])) == [], \
        "additive change (new message, new channel entry) must not be breaking"

    head = spec(["M", "K"], ["M", "K"])                                # 'R' removed
    head["components"]["schemas"]["M"]["properties"]["type"]["enum"] = ["com.a.B"]  # narrowing
    del head["components"]["schemas"]["K"]["properties"]["data"]                     # required field gone
    head["components"]["schemas"]["K"]["required"].remove("data")
    findings = compare(base, head)
    assert any(f == "message 'R' removed" for f in findings), findings
    assert any(f == "message 'R' removed from channel 't'" for f in findings), findings
    assert any("'type' narrowed" in f and "enum" in f for f in findings), findings
    assert any("required field 'data' removed or renamed" in f for f in findings), findings
    print(f"asyncapi_breaking self-check passed ({len(findings)} findings on the negative fixture)")


def main():
    ap = argparse.ArgumentParser(description="AsyncAPI breaking-change gate (see module docstring)")
    ap.add_argument("base_yaml", nargs="?")
    ap.add_argument("head_yaml", nargs="?")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        self_test()
        return 0
    if not (args.base_yaml and args.head_yaml):
        ap.error("need BASE_YAML HEAD_YAML (or --self-test)")
    findings = compare(load(args.base_yaml), load(args.head_yaml))
    if findings:
        print(f"::error::Breaking change to apis/asyncapi.yaml rejected "
              f"({len(findings)} finding(s)). Consumers pin this spec byte-for-byte: "
              f"{', '.join(PINNERS)}, {SELF_PIN} (warehouse-docs network/model/contract-edges.json; contracts/pinned.json). "
              "Keep changes additive or coordinate a versioned contract migration.")
        for finding in findings:
            print(f"  - {finding}")
        return 1
    print("No breaking AsyncAPI changes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
