#!/usr/bin/env python3
"""Reject failed/cancelled jobs and skipped checks selected for this PR."""

from __future__ import annotations

import json
import os


def check(needs: dict, *, draft: bool) -> list[str]:
    outputs = needs.get("release-qualification-contract", {}).get("outputs", {})
    errors = []
    if outputs.get("runtime") not in {"true", "false"}:
        errors.append("release-qualification-contract: missing/invalid runtime selection")
    runtime = outputs.get("runtime") == "true"
    ready = runtime and not draft
    required = {"release-qualification-contract", "lsn-verification", "upgrade-check"}
    if runtime:
        required.update({"unit-tests", "integration-tests"})
    if runtime or outputs.get("monitoring") == "true":
        required.add("otel-collector-contract")
    if ready:
        required.update({
            "windows-compile-gate", "light-e2e-package",
            "light-e2e-tests", "release-evidence-runtime", "lsn-candidate-runtime",
            "e2e-sensitive-gate",
        })
        flags = outputs
        for name in ("unsafe_boundary_changed", "failure_recovery_changed"):
            if flags.get(name) not in {"true", "false"}:
                errors.append(f"release-qualification-contract: missing/invalid {name}")
        if flags.get("unsafe_boundary_changed") == "true":
            required.add("unsafe-boundary-asan")
    if not draft and (runtime or outputs.get("shipping") == "true"):
        required.add("shipping-image-upgrade")
    for name in sorted(required | needs.keys()):
        result = needs.get(name, {}).get("result", "missing")
        if result in {"failure", "cancelled"} or (name in required and result != "success"):
            errors.append(f"{name}: {result}")
    return errors


if __name__ == "__main__":
    errors = check(json.loads(os.environ["CI_NEEDS"]), draft=os.environ["CI_DRAFT"] == "true")
    for error in errors:
        print(error)
    raise SystemExit(bool(errors))
