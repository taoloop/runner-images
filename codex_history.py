"""Trusted finalizer, run only after the coding container has been removed."""

import argparse
import base64
import hashlib
import json
import os
import re
import stat
import subprocess
import tempfile
import threading
from datetime import datetime
from pathlib import Path

from codex_workload import Bridge, Server


class HistoryError(Exception):
    pass


# .taoloop/ is runner-private except a subnet starter's manifest and agent definitions
# (ADR 021), which a coding task must be able to publish.
PUBLISHED = re.compile(r"\.taoloop/(?:starter\.json|agents/[A-Za-z0-9_-]{1,64}\.md)")


def published_entry(path, entry):
    """Inside .taoloop/: the entries to publish (True), to descend into (None for the agents
    folder) or to leave private (False). A linked published path is refused."""
    if path == ".taoloop/agents":
        if entry.is_symlink():
            raise HistoryError("Unsafe candidate path")
        return None if entry.is_dir(follow_symlinks=False) else False
    return bool(PUBLISHED.fullmatch(path))


def candidate_files(root):
    total = 0
    count = 0
    pending = [(root, "")]
    while pending:
        directory, prefix = pending.pop()
        for entry in sorted(os.scandir(directory), key=lambda entry: entry.name):
            if not prefix and entry.name == ".git":
                continue
            if not prefix and entry.name == ".taoloop":
                if entry.is_dir(follow_symlinks=False):
                    pending.append((Path(entry.path), ".taoloop/"))
                continue
            path = prefix + entry.name
            if prefix.startswith(".taoloop/"):
                decision = published_entry(path, entry)
                if decision is None:
                    pending.append((Path(entry.path), path + "/"))
                    continue
                if not decision:
                    continue
            parts = Path(path).parts
            if (
                any(
                    (part in {".git", ".taoloop", ".env"} and not PUBLISHED.fullmatch(path))
                    or part.startswith(".env.")
                    for part in parts
                )
                or "\\" in path
                or "\n" in path
                or "\r" in path
                or Path(path).suffix.lower() in {".pem", ".key", ".p12", ".pfx"}
            ):
                raise HistoryError("Unsafe candidate path")
            info = entry.stat(follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                count += 1
                if count > 10000:
                    raise HistoryError("Candidate file count exceeded")
                pending.append((Path(entry.path), path + "/"))
                continue
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise HistoryError("Candidate requires regular files without aliases")
            with os.fdopen(os.open(entry.path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
                opened = os.fstat(stream.fileno())
                if not stat.S_ISREG(opened.st_mode) or opened.st_ino != info.st_ino:
                    raise HistoryError("Candidate changed while recording")
                content = stream.read(2_000_001)
            total += len(content)
            count += 1
            if len(content) > 2_000_000 or total > 32_000_000 or count > 10000:
                raise HistoryError("Candidate exceeds history limits")
            if re.search(
                rb"-----BEGIN [A-Z ]*PRIVATE KEY-----|gh[pousr]_[A-Za-z0-9]{20,}|"
                rb"github_pat_[A-Za-z0-9_]{20,}|sk-ant-[A-Za-z0-9_-]{20,}",
                content,
            ):
                raise HistoryError("Possible credential in candidate")
            yield path, "100755" if info.st_mode & 0o111 else "100644", content


def evaluation_candidates(files, preferred=None):
    """Bounded hints only; each future evaluation still verifies these exact bytes."""
    result = []
    for path in sorted(files, key=lambda path: (path != preferred, path)):
        digest, size = files[path]
        if (
            len(path) <= 255
            and re.fullmatch(r"[a-zA-Z0-9_./-]+\.py", path)
            and len(path.split("/")) <= 8
            and all(part not in {"", ".", "..", ".git", ".taoloop"} for part in path.split("/"))
            and 1 <= size <= 50000
        ):
            result.append({"path": path, "digest": digest, "size_bytes": size})
            if len(result) == 8:
                break
    return result


def research_draft(content):
    if not 1 <= len(content) <= 8192:
        return None
    return {
        "path": "research/reward-spec.json",
        "digest": hashlib.sha256(content).hexdigest(),
        "size_bytes": len(content),
        "content_base64": base64.b64encode(content).decode("ascii"),
    }


def record(args, port, home):
    directory = Path("/history")
    if any(directory.iterdir()):
        raise HistoryError("History directory must be empty")
    scope = {"user_id": args.user_id, "network": args.network, "netuid": args.netuid}
    env = {
        "PATH": os.defpath,
        "HOME": home,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ALLOW_PROTOCOL": "http",
        "GIT_AUTHOR_NAME": "TaoLoop subnet coder",
        "GIT_AUTHOR_EMAIL": "coder@taoloop.local",
        "GIT_COMMITTER_NAME": "TaoLoop subnet coder",
        "GIT_COMMITTER_EMAIL": "coder@taoloop.local",
        "GIT_AUTHOR_DATE": args.commit_date,
        "GIT_COMMITTER_DATE": args.commit_date,
    }
    repo = directory / "history.git"
    command = [
        "git",
        "--git-dir=" + str(repo),
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "commit.gpgsign=false",
        "-c",
        "credential.helper=",
        "-c",
        "http.followRedirects=false",
        "-c",
        "core.fsync=all",
    ]

    def git(*parts, body=None, optional=False):
        result = subprocess.run(
            command + list(parts),
            env=env,
            input=body,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=120,
            check=False,
        )
        if result.returncode and not optional:
            raise HistoryError("History Git operation failed")
        return result.returncode, result.stdout

    templates = Path(home) / "templates"
    templates.mkdir()
    git(
        "clone",
        "--bare",
        "--template=" + str(templates),
        f"http://127.0.0.1:{port}/repo.git",
        str(repo),
    )
    if git("cat-file", "-t", args.base_commit)[1].strip() != b"commit":
        raise HistoryError("Invalid history baseline")
    status, previous = git("rev-parse", "--verify", "refs/heads/history", optional=True)
    history_exists = not status
    if history_exists and previous.decode().strip() != args.base_commit:
        raise HistoryError("Remote history advanced beyond the task baseline")
    status, identity = git("show", args.base_commit + ":.taoloop/scope.json", optional=True)
    if not status and json.loads(identity) != scope:
        raise HistoryError("History belongs to another user or subnet")
    git("read-tree", args.base_commit)
    _, tracked = git("ls-tree", "-r", "--name-only", "-z", args.base_commit)
    # Rebuild candidate paths without interpreting its Git configuration,
    # attributes, hooks, index, ignore rules, or claimed HEAD. Keep trusted history.
    for path in tracked.split(b"\0"):
        if path and not path.startswith(b".taoloop/"):
            git("update-index", "-z", "--index-info", body=b"0 " + b"0" * 40 + b"\t" + path + b"\0")
    hashes = {}
    file_metadata = {}
    draft = None

    def add(path, mode, content):
        digest = git("hash-object", "-w", "--stdin", body=content)[1].decode().strip()
        git("update-index", "-z", "--index-info", body=f"{mode} {digest}\t{path}\0".encode())

    for path, mode, content in candidate_files(Path("/workspace")):
        if args.research_draft and path == "research/reward-spec.json":
            draft = research_draft(content)
        add(path, mode, content)
        hashes[path] = hashlib.sha256(content).hexdigest()
        file_metadata[path] = (hashes[path], len(content))
    metadata = {
        "scope": scope,
        "task_id": args.task_id,
        "attempt_id": args.attempt_id,
        "spec_digest": args.spec_digest,
        "base_commit": args.base_commit,
        "files": hashes,
    }
    add(".taoloop/scope.json", "100644", json.dumps(scope, sort_keys=True).encode())
    add(
        f".taoloop/runner-attempts/{args.attempt_id}.json",
        "100644",
        json.dumps(metadata, sort_keys=True).encode(),
    )
    tree = git("write-tree")[1].decode().strip()
    commit = (
        git(
            "commit-tree",
            tree,
            "-p",
            args.base_commit,
            body=f"Record TaoLoop coding attempt {args.attempt_id}\n".encode(),
        )[1]
        .decode()
        .strip()
    )
    git(
        "update-ref",
        "refs/heads/history",
        commit,
        previous.decode().strip() if history_exists else "0" * 40,
    )
    result = {
        "commit": commit,
        "base_commit": args.base_commit,
        "spec_digest": args.spec_digest,
        "attempt_id": args.attempt_id,
        "scope": scope,
        "published": False,
    }
    if draft is not None:
        result["research_draft"] = draft
    candidates = evaluation_candidates(file_metadata, args.candidate_path)
    if candidates:
        result["candidates"] = candidates
    with (directory / "result.json").open("x") as stream:
        json.dump(result, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps(result, sort_keys=True))


def main():
    from uuid import UUID

    parser = argparse.ArgumentParser()
    for name in (
        "user-id",
        "network",
        "task-id",
        "attempt-id",
        "spec-digest",
        "base-commit",
        "commit-date",
    ):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--candidate-path", default=None)
    parser.add_argument("--research-draft", action="store_true")
    parser.add_argument("--netuid", type=int, required=True)
    args = parser.parse_args()
    if (
        os.getuid() == 0
        or os.getcwd() != "/workspace"
        or not 1 <= args.netuid <= 65535
        or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", args.user_id)
        or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", args.network)
        or not re.fullmatch(r"[a-f0-9]{40}", args.base_commit)
        or not re.fullmatch(r"[a-f0-9]{64}", args.spec_digest)
    ):
        raise SystemExit("Invalid history context")
    UUID(args.task_id)
    UUID(args.attempt_id)
    date = datetime.fromisoformat(args.commit_date)
    if date.tzinfo is None:
        raise SystemExit("History requires an aware timestamp")
    args.commit_date = f"{int(date.timestamp())} +0000"
    server = Server(("127.0.0.1", 0), Bridge)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        with tempfile.TemporaryDirectory(prefix="taoloop-history-") as home:
            record(args, server.server_port, home)
    except (HistoryError, ValueError, OSError, subprocess.SubprocessError):
        raise SystemExit("History recording failed; candidate retained") from None
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
