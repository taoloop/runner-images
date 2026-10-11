"""Installed image entry point for the validator harness. Runs inside the VPS sandbox only.

The runner mounts ``/inputs`` read-only: ``harness/`` holds the scoring files a reviewed
subnet profile names, read at its pinned commit, and ``candidate.py`` the candidate. This
driver checks both against their digests, copies the tree into scratch space once per
role, places the candidate at the profile's path (the baseline role keeps the repository's
own file there), and runs the profile's entrypoint once per case as a separate process
group with a per-case deadline. The entrypoint writes ``{"score": <number>}`` to its
``{output}`` path; anything else marks the case failed. The sandbox has no network.

The harness is the subnet's code and the candidate is untrusted; both run as the sandbox
user, so the controls are the container isolation, the read-only inputs, the exclusive
report creation and the runner's binding of the report to its request. Scores are local
evidence for comparing candidates, not a validator's weights or a competition result.
"""

import argparse
import hashlib
import json
import math
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

INPUTS = Path("/inputs")
WORKSPACE = Path("/workspace")
SCRATCH = Path("/tmp/taoloop-harness")
OUTPUT_LIMIT = 4096
PLACEHOLDERS = ("candidate", "output", "case", "cases", "seed")


def blob_id(content):
    return hashlib.sha1(f"blob {len(content)}\0".encode() + content).hexdigest()


def tree_digest(tree):
    """The sorted (path, Git blob id) manifest the runner computed before mounting."""
    manifest = []
    for path in sorted(tree.rglob("*")):
        if path.is_symlink():
            raise SystemExit("Harness inputs contain a link")
        if path.is_file():
            manifest.append([path.relative_to(tree).as_posix(), blob_id(path.read_bytes())])
    manifest.sort()
    return hashlib.sha256(json.dumps(manifest, separators=(",", ":")).encode()).hexdigest()


def arguments(entrypoint, values):
    argv = []
    for item in entrypoint:
        for name in PLACEHOLDERS:
            item = item.replace("{" + name + "}", values[name])
        argv.append(item)
    if argv[0] != "python":
        raise SystemExit("The harness entrypoint runs the sandbox's python")
    return [sys.executable, *argv[1:]]


def place(tree, directory, candidate_path, candidate):
    """A private copy of the harness tree, with the candidate placed when given."""
    shutil.copytree(tree, directory)
    target = directory.joinpath(*candidate_path.split("/"))
    if candidate is not None:
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            target.unlink()
        target.write_bytes(candidate)
    if not target.is_file():
        raise SystemExit("The harness has no file at its candidate path")
    return target


def score_of(output):
    try:
        with output.open("rb") as stream:
            content = stream.read(OUTPUT_LIMIT + 1)
        if len(content) > OUTPUT_LIMIT:
            return None
        value = json.loads(content)
    except (OSError, ValueError):
        return None
    score = value.get("score") if isinstance(value, dict) else None
    if isinstance(score, bool) or not isinstance(score, int | float):
        return None
    score = float(score)
    return score if math.isfinite(score) else None


def run_case(directory, candidate, entrypoint, case, cases, seed, seconds, scratch, role):
    output = scratch / f"{role}-{case}.json"
    if output.exists():
        output.unlink()
    argv = arguments(
        entrypoint,
        {
            "candidate": str(candidate),
            "output": str(output),
            "case": str(case),
            "cases": str(cases),
            "seed": str(seed),
        },
    )
    environment = {
        "PATH": os.defpath,
        "HOME": str(scratch),
        "PYTHONPATH": str(directory),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONHASHSEED": "0",
        "OPENBLAS_NUM_THREADS": "1",
        "OMP_NUM_THREADS": "1",
        "TAOLOOP_HARNESS_CASE": str(case),
        "TAOLOOP_HARNESS_CASES": str(cases),
        "TAOLOOP_HARNESS_SEED": str(seed),
        "TAOLOOP_HARNESS_OUTPUT": str(output),
        "TAOLOOP_HARNESS_CANDIDATE": str(candidate),
    }
    started = time.monotonic()
    process = subprocess.Popen(
        argv,
        cwd=directory,
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    status = "failed"
    try:
        code = process.wait(timeout=max(1.0, seconds))
        if code == 0:
            status = "ok"
    except subprocess.TimeoutExpired:
        status = "timeout"
    finally:
        # The whole process group goes, including anything the harness started.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
    score = score_of(output) if status == "ok" else None
    if status == "ok" and score is None:
        status = "invalid_output"
    return {
        "nonce": case,
        "scenario": f"case-{case}",
        "status": status,
        "score": score,
        "seconds": round(time.monotonic() - started, 3),
    }


def evaluate(inputs, scratch, harness, seed, timeout_seconds):
    """Run the harness for each role and case; returns the report's role scores."""
    tree = inputs / "harness"
    candidate = (inputs / "candidate.py").read_bytes()
    roles = [("baseline", None)] if harness["baseline"] else []
    roles.append(("candidate", candidate))
    cases = harness["cases"]
    per_case = (timeout_seconds - 5) / (cases * len(roles))
    results = {}
    for role, content in roles:
        directory = scratch / role
        placed = place(tree, directory, harness["candidate_path"], content)
        scores = [
            run_case(
                directory,
                placed,
                harness["entrypoint"],
                case,
                cases,
                seed,
                per_case,
                scratch,
                role,
            )
            for case in range(cases)
        ]
        completed = [item["score"] for item in scores if item["score"] is not None]
        results[role] = {
            "average_quality": sum(completed) / len(completed) if completed else None,
            "completed": len(completed),
            "instances": scores,
        }
        shutil.rmtree(directory, ignore_errors=True)
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-digest", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--instances", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=int, required=True)
    parser.add_argument("--harness-digest", required=True)
    parser.add_argument("--harness", required=True)
    args = parser.parse_args()
    harness = json.loads(args.harness)
    if (
        os.getuid() == 0
        or os.getcwd() != str(WORKSPACE)
        or not 1 <= args.instances <= 10
        or args.instances != harness["cases"]
        or args.revision != harness["commit"]
        or not 0 <= args.seed < 2**32
        or not 1 <= args.timeout_seconds <= 600
    ):
        raise SystemExit("Invalid isolated evaluation context")
    signal.alarm(args.timeout_seconds)
    with (INPUTS / "candidate.py").open("rb") as stream:
        candidate = stream.read(50001)
    if (
        not candidate
        or len(candidate) > 50000
        or hashlib.sha256(candidate).hexdigest() != args.candidate_digest
    ):
        raise SystemExit("Candidate does not match its authorized digest")
    if tree_digest(INPUTS / "harness") != args.harness_digest:
        raise SystemExit("Harness files do not match their pinned manifest")
    SCRATCH.mkdir(mode=0o700)
    results = evaluate(INPUTS, SCRATCH, harness, args.seed, args.timeout_seconds)
    result = {
        "revision": args.revision,
        "candidate_digest": args.candidate_digest,
        "seed": args.seed,
        "instances": args.instances,
        "report": {
            "schema_version": 3,
            "harness_commit": harness["commit"],
            "harness_digest": args.harness_digest,
            "metric": harness["metric"],
            "direction": harness["direction"],
            **results,
        },
    }
    content = json.dumps(result, sort_keys=True, allow_nan=False).encode()
    if len(content) > 32768:
        raise SystemExit("Evaluation report exceeds its limit")
    # Exclusive creation refuses a report pre-written by the harness or the candidate.
    with (WORKSPACE / "evaluation.json").open("xb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


if __name__ == "__main__":
    main()
