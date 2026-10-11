"""Single-attempt private history publisher; remote reads resolve lost acknowledgments."""

import json
import os
import re
import subprocess
import tempfile
import threading
from pathlib import Path

from codex_workload import Bridge, Server


class PublishBridge(Bridge):
    allow_push = True
    allow_model = False


def publish(manifest, port, home):
    commit, base = manifest["commit"], manifest["base_commit"]
    if not all(
        isinstance(value, str) and re.fullmatch(r"[a-f0-9]{40}", value) for value in (commit, base)
    ):
        raise ValueError("Invalid publication commit")
    env = {
        "PATH": os.defpath,
        "HOME": home,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ALLOW_PROTOCOL": "http",
    }
    command = [
        "git",
        "--git-dir=/history/history.git",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "credential.helper=",
        "-c",
        "http.followRedirects=false",
        "-c",
        "http.postBuffer=67108864",
        "-c",
        "push.followTags=false",
    ]
    url = f"http://127.0.0.1:{port}/repo.git"

    def git(*args):
        result = subprocess.run(
            command + list(args),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=120,
        )
        if len(result.stdout) > 4096:
            raise ValueError("Unexpected Git response")
        return result.returncode, result.stdout.strip()

    def remote():
        status, output = git("ls-remote", "--refs", "--exit-code", url, "refs/heads/history")
        if status == 2 and not output:
            return None
        if status != 0 or not re.fullmatch(rb"[a-f0-9]{40}\trefs/heads/history", output):
            raise ValueError("Cannot verify remote history")
        return output[:40].decode()

    result = {**manifest, "publication": "local", "published": False}
    try:
        status, parent = git("rev-parse", commit + "^")
        if status or parent.decode() != base:
            raise ValueError("Publication commit has another parent")
        current = remote()
        if current == commit:
            result.update(publication="confirmed", published=True)
        elif current in (None, base):
            # Mark ambiguity before invocation; the proxy durably limits dispatch
            # to one request even if Git or the network loses the response.
            result["publication"] = "uncertain"
            try:
                git("push", "--porcelain", url, commit + ":refs/heads/history")
            except (OSError, ValueError, subprocess.SubprocessError):
                pass
            if remote() == commit:
                result.update(publication="confirmed", published=True)
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    path = Path("/history/publication.json")
    with path.open("x") as stream:
        json.dump(result, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps(result, sort_keys=True))


def main():
    if os.getuid() == 0 or os.getcwd() != "/workspace":
        raise SystemExit("Publisher requires the isolated workload environment")
    content = Path("/history/result.json").read_bytes()
    if len(content) > 16384:
        raise SystemExit("Invalid private history manifest")
    manifest = json.loads(content)
    if manifest.get("published") is not False:
        raise SystemExit("Publisher requires an unpublished private commit")
    server = Server(("127.0.0.1", 0), PublishBridge)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        with tempfile.TemporaryDirectory(prefix="taoloop-publish-") as home:
            publish(manifest, server.server_port, home)
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
