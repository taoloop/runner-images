"""Installed inside the coder image; never run repository commands on the host."""

import argparse
import base64
import hashlib
import http.client
import json
import math
import os
import re
import resource
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class TaskConnection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("local")
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(180)
        self.sock.connect(self.path)


class Bridge(BaseHTTPRequestHandler):
    allow_push = False
    allow_model = True
    # "codex" speaks Responses at /v1/responses; "claude" (Claude Code) speaks Anthropic
    # Messages at /v1/messages?beta=true. Set once in main().
    agent = "codex"
    # "relay": model requests go to TaoLoop's metered relay, reduced to the fields it
    # accepts. "subscription" (ADR 010): they go to the provider on the user's own plan, as
    # the agent sent them; the runner outside this container substitutes the credential.
    auth = "relay"

    def log_message(self, *args):
        pass

    def do_GET(self):
        self.forward()

    def do_POST(self):
        self.forward()

    def do_PUT(self):
        if self.path.startswith("/svc/"):
            self.forward_service()
        else:
            self.send_error(405)

    def do_HEAD(self):
        if self.path.startswith("/svc/"):
            self.forward_service()
            return
        # Claude Code probes its API base for reachability; answer locally, forward nothing.
        self.send_response(204 if self.path == "/api/hello" else 404)
        self.end_headers()

    def forward_service(self):
        """A third-party service connection (ADR 011): to the runner's service socket,
        which forwards to TaoLoop's relay; the relay attaches the user's key."""
        self.connection.settimeout(30)
        parts = self.path.split("/", 3)
        if len(parts) < 3 or parts[2] not in SERVICES:
            self.send_error(404, "Unknown service connection")
            return
        lengths = self.headers.get_all("Content-Length", [])
        length = lengths[0] if len(lengths) == 1 else "0"
        if (
            not length.isdigit()
            or int(length) > SERVICE_REQUEST_MAX
            or self.headers.get("Transfer-Encoding")
            or self.headers.get("Expect")
        ):
            self.send_error(400, "Unsupported service request")
            return
        body = self.rfile.read(int(length)) if int(length) else None
        headers = {
            name: self.headers[name]
            for name in ("Content-Type", "Accept", "Range", "If-None-Match", "If-Modified-Since")
            if self.headers.get(name)
        }
        if body is not None:
            headers["Content-Length"] = str(len(body))
        connection = TaskConnection("/run/taoloop/services.sock")
        try:
            connection.request(self.command, self.path, body, headers)
            response = connection.getresponse()
            self.send_response(response.status)
            for name in ("Content-Type", "Content-Length", "ETag", "Last-Modified"):
                value = response.getheader(name)
                if value:
                    self.send_header(name, value)
            self.end_headers()
            if self.command != "HEAD":
                while chunk := response.read1(65536):
                    self.wfile.write(chunk)
                    self.wfile.flush()
        except OSError:
            self.send_error(502, "Service connection unavailable")
        finally:
            connection.close()

    def forward(self):
        if self.path.startswith("/svc/"):
            self.forward_service()
            return
        self.connection.settimeout(10)
        plan = self.auth == "subscription"
        claude = self.agent == "claude"
        model_paths = (
            ("/v1/messages", "/v1/messages?beta=true")
            if claude
            else ("/backend-api/codex/responses",)
            if plan
            else ("/v1/responses",)
        )
        model = self.allow_model and self.command == "POST" and self.path in model_paths
        # Codex on a ChatGPT sign-in reads its model catalogue once, without a body.
        catalogue = bool(
            self.allow_model
            and plan
            and not claude
            and self.command == "GET"
            and CATALOGUE.fullmatch(self.path)
        )
        discovery = (
            self.command == "GET" and self.path == "/repo.git/info/refs?service=git-upload-pack"
        )
        fetch = self.command == "POST" and self.path == "/repo.git/git-upload-pack"
        push_discovery = (
            self.allow_push
            and self.command == "GET"
            and self.path == "/repo.git/info/refs?service=git-receive-pack"
        )
        push = (
            self.allow_push and self.command == "POST" and self.path == "/repo.git/git-receive-pack"
        )
        discovery = discovery or push_discovery
        limit = 67_108_864 if push else 4_194_304 if model and (claude or plan) else 1_048_576
        lengths = self.headers.get_all("Content-Length", [])
        bodiless = discovery or catalogue
        length = lengths[0] if len(lengths) == 1 else "0" if not lengths and bodiless else ""
        encoding = self.headers.get("Content-Encoding")
        if (
            not (model or catalogue or discovery or fetch or push)
            or not length.isdigit()
            or not (int(length) == 0 if bodiless else 0 < int(length) <= limit)
            or self.headers.get("Transfer-Encoding")
            # Codex compresses its requests on a ChatGPT sign-in, and Git gzips a fetch
            # request over 1 KiB (a repository with many refs); nothing else may.
            or (
                encoding
                and not (plan and model and not claude and encoding == "zstd")
                and not (fetch and encoding == "gzip")
            )
            or self.headers.get("Expect")
            or (
                push
                and self.headers.get("Content-Type") != "application/x-git-receive-pack-request"
            )
            or (
                fetch
                and self.headers.get("Content-Type") != "application/x-git-upload-pack-request"
            )
        ):
            self.send_error(400, "Unsupported task request")
            return
        connection = TaskConnection(
            "/run/taoloop/model.sock" if model or catalogue else "/run/taoloop/git.sock"
        )
        try:
            body = self.rfile.read(int(length))
            if len(body) != int(length):
                return
            headers = {}
            if plan and (model or catalogue):
                # The agent's own request, unchanged. Its authorization header carries a
                # placeholder; the runner sets the real one outside this container.
                for name, value in self.headers.items():
                    if name.lower() not in UNFORWARDED:
                        headers[name] = value
            elif model:
                headers["Content-Type"] = "application/json"
                if self.headers.get("Idempotency-Key"):
                    headers["Idempotency-Key"] = self.headers["Idempotency-Key"]
                if self.agent == "claude":
                    if self.headers.get("anthropic-beta"):
                        headers["anthropic-beta"] = self.headers["anthropic-beta"][:512]
                    # Every Claude Code request (subagents and quick side calls included)
                    # runs on the task's one model; the relay refuses any other.
                    try:
                        payload = json.loads(body)
                    except ValueError:
                        payload = None
                    if isinstance(payload, dict) and payload.get("model") != TASK_MODEL:
                        payload["model"] = TASK_MODEL
                        body = json.dumps(payload).encode()
                else:
                    # Codex commonly uses bare model names while the relay uses canonical
                    # provider-prefixed ids. Normalize only names in this task's frozen pool;
                    # both this bridge and the relay refuse anything outside it.
                    try:
                        payload = json.loads(body)
                    except ValueError:
                        payload = None
                    if not isinstance(payload, dict):
                        self.send_error(400, "Invalid model request")
                        return
                    requested = payload.get("model")
                    canonical = MODEL_ALIASES.get(requested, requested)
                    if not isinstance(canonical, str) or canonical not in ALLOWED_MODELS:
                        self.send_error(403, "Task model is not authorized")
                        return
                    payload["model"] = canonical
                    body = json.dumps(payload).encode()
            else:
                if fetch or push:
                    service = "git-receive-pack" if push else "git-upload-pack"
                    headers["Content-Type"] = f"application/x-{service}-request"
                    if fetch and encoding == "gzip":
                        headers["Content-Encoding"] = "gzip"
                if self.headers.get("Git-Protocol") == "version=2":
                    headers["Git-Protocol"] = "version=2"
            path = "/v1/messages" if model and claude and not plan else self.path
            connection.request(self.command, path, body, headers)
            response = connection.getresponse()
            self.send_response(response.status)
            self.send_header("Content-Type", response.getheader("Content-Type", "application/json"))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            while chunk := response.read1(65536):
                self.wfile.write(chunk)
                self.wfile.flush()
        finally:
            connection.close()


# Third-party service connections (ADR 011): connection id -> provider, and the SDK
# variables pointing at this bridge; set once in main() from --services.
SERVICES: dict[str, str] = {}
SERVICE_ENV: dict[str, str] = {}
SERVICE_REQUEST_MAX = 16 * 1024 * 1024
SERVICE_ID = re.compile(r"^[0-9a-f-]{36}$")
SERVICE_PROVIDER = re.compile(r"^[a-z0-9][a-z0-9-]{1,39}$")
SERVICE_VARIABLE = re.compile(r"^[A-Z][A-Z0-9_]{1,63}$")


def configure_services(encoded, port):
    """Point each connection's SDK variables at this bridge. S3 SDKs sign with a
    placeholder key; TaoLoop's proxy outside the sandbox signs with the real one."""
    items = json.loads(base64.b64decode(encoded, validate=True)) if encoded else []
    if not isinstance(items, list) or len(items) > 4:
        raise SystemExit("Invalid service connections")
    for item in items:
        connection, provider = item.get("connection_id"), item.get("provider")
        variables = item.get("base_url_env") or []
        if (
            not isinstance(connection, str)
            or not SERVICE_ID.match(connection)
            or not isinstance(provider, str)
            or not SERVICE_PROVIDER.match(provider)
            or not isinstance(variables, list)
            or any(not isinstance(v, str) or not SERVICE_VARIABLE.match(v) for v in variables)
        ):
            raise SystemExit("Invalid service connection")
        SERVICES[connection] = provider
        url = f"http://127.0.0.1:{port}/svc/{connection}"
        for variable in [
            *variables,
            "TAOLOOP_SERVICE_" + provider.upper().replace("-", "_") + "_URL",
        ]:
            SERVICE_ENV[variable] = url
    if SERVICES:
        SERVICE_ENV.setdefault("AWS_ACCESS_KEY_ID", "taoloop-placeholder")
        SERVICE_ENV.setdefault("AWS_SECRET_ACCESS_KEY", "taoloop-placeholder")


CATALOGUE = re.compile(r"/backend-api/codex/models(\?[a-z0-9_=.-]{0,64})?")
# Never copied from the agent's request on a plan sign-in task: connection framing, which
# this bridge sets itself, and the placeholder credential.
UNFORWARDED = {
    "host",
    "connection",
    "keep-alive",
    "content-length",
    "transfer-encoding",
    "accept-encoding",
    "authorization",
    "proxy-authorization",
    "expect",
    "te",
    "upgrade",
}
# (name Codex is configured with, name the relay policy expects); set once in main().
MODEL_ALIAS: tuple[str, str] | None = None
# Every bare Codex model name that is unambiguous within the frozen task pool.
MODEL_ALIASES: dict[str, str] = {}
# The relay policy model ids the driver may forward; relay membership remains authoritative.
ALLOWED_MODELS: set[str] = set()
# Ordered model ids from the server capability, primary first.
MODEL_POOL: tuple[str, ...] = ()
# The relay policy's default model id for this task; set once in main().
TASK_MODEL = ""

NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,39}$")
MODEL_ID = re.compile(r"^[A-Za-z0-9_.-]{1,64}(?:/[A-Za-z0-9_.:-]{1,64})?$")
COMMAND_PATTERN = re.compile(r"^(?:[A-Za-z0-9_.-]{1,64}|/usr/local/bin/[A-Za-z0-9_.-]{1,64})$")
TOOLKIT_MAX_BYTES = 131_072
CONTEXT_MAX_BYTES = 65_536
WORKSPACE = "/workspace"
# Repository analysis (ADR 009): the input the runner hands over and the files an
# untrusted upstream could use to instruct the agent, held aside under a new name.
ANALYSIS_INPUT_MAX = 120_000
HELD_NAMES = {
    "AGENTS.md",
    "AGENTS.override.md",
    "CLAUDE.md",
    "CLAUDE.local.md",
    ".claude",
    ".codex",
    ".cursor",
    ".cursorrules",
}
HELD_PATHS = {".github/copilot-instructions.md"}
# The image's Python lives in /usr/local/bin; agents run the candidate's code with it.
AGENT_PATH = "/usr/local/bin:" + os.defpath


def configure_model_pool(encoded, primary):
    """Decode only the server-frozen model ids supplied by the trusted runner."""
    global MODEL_ALIASES, ALLOWED_MODELS, MODEL_POOL
    if not encoded:
        values = [primary]
    else:
        try:
            raw = base64.b64decode(encoded, validate=True)
            values = json.loads(raw)
        except (ValueError, UnicodeError):
            raise SystemExit("Invalid model pool") from None
        if (
            len(raw) > 16384
            or not isinstance(values, list)
            or not 1 <= len(values) <= 64
            or any(not isinstance(value, str) or not MODEL_ID.fullmatch(value) for value in values)
            or len(set(values)) != len(values)
        ):
            raise SystemExit("Invalid model pool")
    if primary not in values:
        raise SystemExit("Model pool does not include the task model")
    ALLOWED_MODELS = set(values)
    MODEL_POOL = tuple(values)
    aliases = {}
    for value in values:
        bare = value.rsplit("/", 1)[-1]
        if bare in aliases:
            aliases[bare] = ""
        else:
            aliases[bare] = value
    MODEL_ALIASES = {bare: value for bare, value in aliases.items() if value}


def apply_codex_role_definitions(home):
    """Install the task-local Codex roles from the frozen capability pool.

    Codex discovers TOML role files natively. The lifecycle skill remains the shared
    Markdown source of BDD/TDD guidance; these narrow role files only say which part of
    that lifecycle each child owns. Owners can steer delegation in the task prompt, but
    the bridge and relay remain the model-authorization boundary.
    """
    if not MODEL_POOL:
        return
    choices = [
        bare if MODEL_ALIASES.get(bare) == model else model
        for model in MODEL_POOL
        for bare in (model.rsplit("/", 1)[-1],)
    ]
    roles = (
        (
            "taoloop_coordinator",
            choices[0],
            "Coordinator. Turn the task into a bounded plan, delegate read-only specification, test-author and review work in sequence, collect their evidence, and keep writer ownership serialized. Do not change models, permissions, tools or network access.",
        ),
        (
            "taoloop_explorer",
            choices[min(1, len(choices) - 1)],
            "Behaviour and specification agent. Read relevant code and tests, turn the task into observable acceptance scenarios, identify affected boundaries, and return concise evidence. Do not edit files.",
        ),
        (
            "taoloop_test_author",
            choices[min(1, len(choices) - 1)],
            "Test author. After the coordinator accepts the scenarios, add or identify focused BDD/TDD regression tests that fail for the current behaviour. Do not edit implementation files, weaken assertions, or make the production change.",
        ),
        (
            "taoloop_implementer",
            choices[0],
            "Implementation agent. Follow the task's BDD/TDD lifecycle: make a focused failing test, implement the smallest change, then refactor only after it passes.",
        ),
        (
            "taoloop_reviewer",
            choices[min(2, len(choices) - 1)],
            "Independent reviewer. Check correctness, security, regressions, and missing tests. Report concrete findings with files and reproduction steps; do not edit files.",
        ),
    )
    directory = Path(home) / ".codex" / "agents"
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    for name, model, instructions in roles:
        content = "\n".join(
            (
                "name = " + json.dumps(name),
                "description = " + json.dumps(instructions.split(". ", 1)[0] + "."),
                "model = " + json.dumps(model),
                "developer_instructions = " + json.dumps(instructions),
                "",
            )
        )
        descriptor = os.open(
            directory / (name + ".toml"), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600
        )
        with os.fdopen(descriptor, "w") as target:
            target.write(content)


def agent_env(home):
    """Environment shared by both agents: the image's tools on PATH, and bytecode caches
    kept in the private home so `python -m py_compile` never adds .pyc files to the
    candidate the finalizer publishes."""
    return {
        "PATH": AGENT_PATH,
        "HOME": home,
        "PYTHONPYCACHEPREFIX": str(Path(home) / "pycache"),
        **SERVICE_ENV,
    }


CLAUDE = "/usr/local/bin/claude"


def skill_document(name, content):
    """A SKILL.md both agents load natively: YAML frontmatter naming the skill, then the
    body. Content that already starts with frontmatter is kept as written."""
    if content.startswith("---\n"):
        return content
    summary = next((line.strip("# ").strip() for line in content.splitlines() if line.strip()), "")
    description = (summary or "Skill from the TaoLoop toolkit")[:200].replace('"', "'")
    return f'---\nname: {name}\ndescription: "{description}"\n---\n\n{content}'


def apply_toolkit(home, toolkit, agent="codex", context=""):
    """Materialise the user's toolkit and the task's subnet context in the private home,
    in each agent's native layout. Codex: ``~/.codex/skills/<name>/SKILL.md``, a global
    ``~/.codex/AGENTS.md`` and ``mcp_servers`` config pairs (returned). Claude Code:
    ``~/.claude/skills/<name>/SKILL.md``, a user ``~/.claude/CLAUDE.md`` and an MCP config
    file ``~/.claude/taoloop-mcp.json`` (returns no pairs). The agents discover and invoke
    skills and MCP tools themselves. Bounded and validated again here; the workload trusts
    nothing it did not check."""
    root = Path(home) / (".claude" if agent == "claude" else ".codex")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    memory = "CLAUDE.md" if agent == "claude" else "AGENTS.md"
    sections = []
    if context:
        sections += [context.strip(), ""]
    config = []
    servers = {}
    items = toolkit.get("items") if isinstance(toolkit, dict) else None
    listed = []
    for item in (items if isinstance(items, list) else [])[:40]:
        if not isinstance(item, dict):
            continue
        name, kind = str(item.get("name", "")), str(item.get("kind", ""))
        if not NAME_PATTERN.match(name):
            continue
        if kind == "skill":
            content = str(item.get("content", ""))[:20000]
            skill_dir = root / "skills" / name
            skill_dir.mkdir(parents=True, exist_ok=True)
            (skill_dir / "SKILL.md").write_text(skill_document(name, content), encoding="utf-8")
            listed += [f"- Skill `{name}`: installed as a native skill; use it when it applies."]
        elif kind == "mcp":
            server = item.get("mcp") or {}
            command = str(server.get("command", ""))
            if not COMMAND_PATTERN.match(command):
                continue
            args = [str(arg)[:200] for arg in list(server.get("args") or [])[:16]]
            env = {
                str(key)[:64]: str(value)[:200]
                for key, value in dict(server.get("env") or {}).items()
                if str(key).isupper() and str(key).replace("_", "").isalnum()
            }
            config += [
                ("mcp_servers." + name + ".command", command),
                ("mcp_servers." + name + ".args", args),
            ]
            if env:
                config.append(("mcp_servers." + name + ".env", env))
            servers[name] = {"command": command, "args": args, **({"env": env} if env else {})}
            listed += [f"- MCP server `{name}` (`{command}`): its tools are available to you."]
        elif kind == "tool":
            pattern = str(item.get("tool_pattern", ""))[:200]
            notes = str(item.get("content", ""))[:20000]
            listed += [
                "",
                f"## Tool: {name}",
                "",
                f"Command: `{pattern}`" if pattern else "",
                notes,
            ]
    if listed:
        sections += ["# TaoLoop toolkit", "", "What the operator configured for you.", *listed]
    (root / memory).write_text("\n".join(sections).strip() + "\n", encoding="utf-8")
    if agent == "claude":
        (root / "taoloop-mcp.json").write_text(
            json.dumps({"mcpServers": servers}), encoding="utf-8"
        )
        return []
    return config


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        # Never log request content or diagnostic tracebacks from the bridge.
        pass


def repoint_origin(port, home):
    """A continuation's Git bridge listens on a new port: point the kept checkout's origin
    at it, so the resumed agent's own fetches reach the task's repository (ADR 015)."""
    subprocess.run(
        [
            "git",
            "-c",
            "core.hooksPath=/dev/null",
            "remote",
            "set-url",
            "origin",
            f"http://127.0.0.1:{port}/repo.git",
        ],
        cwd=WORKSPACE,
        env={
            "PATH": os.defpath,
            "HOME": home,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_TERMINAL_PROMPT": "0",
        },
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=60,
        check=False,
    )


def checkout(base_commit, port, home, scope, analysis=False, shallow=False):
    if any(Path(WORKSPACE).iterdir()):
        raise SystemExit("Checkout requires an empty task workspace")
    templates = Path(home) / "empty-templates"
    templates.mkdir()
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
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "http.followRedirects=false",
        "-c",
        "submodule.recurse=false",
        "-c",
        "credential.helper=",
    ]

    def git(*args):
        result = subprocess.run(
            command + list(args),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=120,
            check=True,
        )
        return result.stdout.strip()

    url = f"http://127.0.0.1:{port}/repo.git"
    try:
        if shallow:
            # A platform run is bound to the observed default-branch head. Fetch only
            # that commit, by id, so repository history cannot exhaust the bounded Git
            # proxy; the branch may have moved past it since it was observed.
            git("init", "--quiet", "--template=" + str(templates), WORKSPACE)
            git("remote", "add", "origin", url)
            try:
                git("fetch", "--depth=1", "--no-tags", "origin", base_commit)
            except subprocess.CalledProcessError:
                raise SystemExit(64) from None
        else:
            git(
                "clone",
                "--no-checkout",
                "--no-recurse-submodules",
                "--template=" + str(templates),
                url,
                WORKSPACE,
            )
        try:
            baseline_type = git("cat-file", "-t", base_commit)
        except subprocess.CalledProcessError:
            raise SystemExit(64) from None
        if baseline_type != b"commit":
            raise SystemExit(64)
        if analysis:
            # A public upstream is read, never extended: its own branches and files carry
            # no TaoLoop scope, so the history and scope checks below do not apply.
            git("checkout", "--detach", base_commit, "--")
            if git("rev-parse", "HEAD").decode("ascii") != base_commit:
                raise SystemExit(64)
            return
        # A replaced VPS must hydrate from the repository's current history,
        # not spend credits on a stale baseline that cannot be published.
        refs = git(
            "for-each-ref", "--format=%(refname) %(objectname)", "refs/remotes/origin/history"
        )
        history = dict(line.split(b" ", 1) for line in refs.splitlines()).get(
            b"refs/remotes/origin/history"
        )
        if history and history.decode("ascii") != base_commit:
            raise SystemExit(66)
        identity = git("ls-tree", base_commit, "--", ".taoloop/scope.json")
        if identity:
            if not re.fullmatch(rb"100644 blob [a-f0-9]{40}\t\.taoloop/scope.json", identity):
                raise SystemExit(65)
            blob = base_commit + ":.taoloop/scope.json"
            if int(git("cat-file", "-s", blob)) > 4096:
                raise SystemExit(65)
            try:
                owner = json.loads(git("cat-file", "blob", blob))
            except (ValueError, UnicodeError):
                raise SystemExit(65) from None
            if (
                not isinstance(owner, dict)
                or type(owner.get("netuid")) is not int
                or owner != scope
            ):
                raise SystemExit(65)
        git("checkout", "--detach", base_commit, "--")
        if git("rev-parse", "HEAD").decode("ascii") != base_commit:
            raise SystemExit(64)
    except (subprocess.SubprocessError, OSError):
        raise SystemExit(67) from None


def analysis_mode(encoded):
    """The analysis task's mode, or None outside analysis. A map or an audit reads the
    pinned head alone, so its checkout is shallow: a full history of a large upstream
    exceeds the bounded Git proxy. A change review reads the range, so it is not."""
    if not encoded:
        return "review"  # not analysis: keep the full checkout
    try:
        payload = json.loads(base64.b64decode(encoded, validate=True))
    except (ValueError, UnicodeError):
        return "review"
    return payload.get("mode") if isinstance(payload, dict) else "review"


def prepare_analysis(encoded, base_commit):
    """Hold aside the upstream's agent instruction files (renamed with an .untrusted
    suffix, still readable as data), then write the analysis input and the result schema
    under .taoloop/analysis/. Any .taoloop the upstream ships is removed first, so the
    result file can only be the one the agent writes."""
    if len(encoded) > ANALYSIS_INPUT_MAX:
        raise SystemExit("Analysis input exceeds its bound")
    payload = json.loads(base64.b64decode(encoded, validate=True))
    if not isinstance(payload, dict) or payload.get("commit") != base_commit:
        raise SystemExit("Analysis input does not match the checkout")
    root = Path(WORKSPACE)
    for path in sorted(root.rglob("*"), key=lambda item: len(item.parts), reverse=True):
        relative = path.relative_to(root).as_posix()
        if relative == ".git" or relative.startswith(".git/"):
            continue
        if path.name in HELD_NAMES or relative in HELD_PATHS:
            path.rename(path.with_name(path.name + ".untrusted"))
    marker = root / ".taoloop"
    if marker.is_symlink() or marker.is_file():
        marker.unlink()
    elif marker.exists():
        shutil.rmtree(marker)
    directory = root / ".taoloop" / "analysis"
    directory.mkdir(parents=True)
    schema = payload.pop("schema")
    (directory / "schema.json").write_text(json.dumps(schema, indent=1), encoding="utf-8")
    (directory / "input.json").write_text(json.dumps(payload, indent=1), encoding="utf-8")


PLACEHOLDER = "local-only"


def shadow_sign_in(home, encoded):
    """Write the auth.json that puts Codex in ChatGPT sign-in mode without a credential.

    The runner passes the plan name and the real access token's expiry; the tokens here
    are placeholders that authenticate nothing. The real sign-in stays on the host and the
    runner's proxy sets the real bearer and account on each request. Mirroring the real
    expiry keeps Codex from attempting a renewal it could not complete from here."""
    try:
        plan = json.loads(base64.b64decode(encoded, validate=True))
        plan_type, expires_at, last_refresh = (
            plan["plan_type"],
            plan["expires_at"],
            plan["last_refresh"],
        )
    except (ValueError, KeyError, TypeError):
        raise SystemExit("Invalid plan sign-in input") from None
    if (
        not isinstance(plan_type, str)
        or not re.fullmatch(r"[a-z0-9_]{1,32}", plan_type)
        or not isinstance(expires_at, int)
        or isinstance(expires_at, bool)
        or not isinstance(last_refresh, str)
        or not re.fullmatch(r"[0-9TZ:.+-]{1,64}", last_refresh)
    ):
        raise SystemExit("Invalid plan sign-in input")

    def token(claims):
        parts = ({"alg": "none", "typ": "JWT"}, claims)
        encoded_parts = [
            base64.urlsafe_b64encode(json.dumps(part).encode()).rstrip(b"=").decode()
            for part in parts
        ]
        return ".".join([*encoded_parts, PLACEHOLDER])

    account = "00000000-0000-4000-8000-000000000000"
    profile = {
        "chatgpt_account_id": account,
        "chatgpt_plan_type": plan_type,
        "chatgpt_user_id": PLACEHOLDER,
        "user_id": PLACEHOLDER,
    }
    common = {"iss": "https://auth.openai.com", "sub": PLACEHOLDER, "exp": expires_at}
    value = {
        "auth_mode": "chatgpt",
        "OPENAI_API_KEY": None,
        "tokens": {
            "id_token": token(
                {**common, "aud": [PLACEHOLDER], "https://api.openai.com/auth": profile}
            ),
            "access_token": token(
                {
                    **common,
                    "aud": ["https://api.openai.com/v1"],
                    "https://api.openai.com/auth": profile,
                }
            ),
            "refresh_token": PLACEHOLDER,
            "account_id": account,
        },
        "last_refresh": last_refresh,
    }
    directory = Path(home) / ".codex"
    directory.mkdir(mode=0o700, exist_ok=True)
    descriptor = os.open(directory / "auth.json", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as target:
        json.dump(value, target)


def run_codex(args, home, port, toolkit, context, resume=None, prompt=None):
    """Native Codex CLI (`codex exec`) against the metered Responses route. In an
    interactive session, ``resume`` names the recorded thread to continue with
    ``codex exec resume <id>`` (Codex CLI 0.154); the output is then teed so the
    thread id of a new conversation can be recorded."""
    global MODEL_ALIAS
    codex_model = args.model.rsplit("/", 1)[-1]
    if args.auth == "subscription":
        # The user's ChatGPT plan (ADR 010): Codex signs in from the placeholder auth.json
        # and sends its own requests to the bridge, which the runner forwards to OpenAI.
        shadow_sign_in(home, args.plan)
        config = {
            "model_provider": "plan",
            "model": args.model,
            "web_search": "disabled",
            "cli_auth_credentials_store": "file",
            "chatgpt_base_url": f"http://127.0.0.1:{port}/backend-api/",
            "model_providers.plan.name": "OpenAI",
            "model_providers.plan.base_url": f"http://127.0.0.1:{port}/backend-api/codex",
            "model_providers.plan.wire_api": "responses",
            "model_providers.plan.requires_openai_auth": True,
            "model_providers.plan.request_max_retries": 0,
            "model_providers.plan.stream_max_retries": 0,
        }
    else:
        if codex_model != args.model:
            MODEL_ALIAS = (codex_model, args.model)
        config = {
            "model_provider": "taoloop",
            "model": codex_model,
            "web_search": "disabled",
            "model_providers.taoloop.name": "TaoLoop",
            "model_providers.taoloop.base_url": f"http://127.0.0.1:{port}/v1",
            "model_providers.taoloop.env_key": "TAOLOOP_PROXY_PLACEHOLDER",
            "model_providers.taoloop.wire_api": "responses",
            "model_providers.taoloop.request_max_retries": 0,
            "model_providers.taoloop.stream_max_retries": 0,
        }
    if args.auth != "subscription":
        apply_codex_role_definitions(home)
        config["agents.enabled"] = True
    command = ["/usr/local/bin/codex"]
    for key, value in config.items():
        command.extend(["-c", key + "=" + json.dumps(value)])
    if toolkit or context:
        for key, value in apply_toolkit(home, toolkit, "codex", context):
            command.extend(["-c", key + "=" + json.dumps(value)])
    env = agent_env(home)
    if args.auth != "subscription":
        env["TAOLOOP_PROXY_PLACEHOLDER"] = "local-only"
    if getattr(args, "session_stream", False):
        # ADR 025: one app-server for the whole leg; later messages steer the running turn.
        return codex_stream([*command, "app-server"], prompt or args.objective, env, None, resume)
    command.extend(["exec", *(["resume", resume] if resume else [])])
    command.extend(
        [
            "--skip-git-repo-check",
            "--dangerously-bypass-approvals-and-sandbox",
            "--json",
            "-",
        ]
    )
    if args.session_turn or args.keep_home:
        return tee(command, prompt or args.objective, env, None)
    return subprocess.run(command, input=args.objective, text=True, check=False, env=env)


def run_claude(args, home, port, toolkit, context, resume=None, prompt=None, session=None):
    """Native Claude Code (`claude -p`, headless) against the metered Messages route. Its
    own agent loop, tools, subagents, skills and MCP client do the work; the container is
    the sandbox, so permission prompts are bypassed inside it only. Project settings
    (hooks, .mcp.json) from the repository are not loaded; its CLAUDE.md is."""
    apply_toolkit(home, toolkit, "claude", context)
    config_dir = Path(home) / ".claude"
    model = args.model
    env = {
        **agent_env(home),
        "CLAUDE_CONFIG_DIR": str(config_dir),
        "ANTHROPIC_BASE_URL": f"http://127.0.0.1:{port}",
        "ANTHROPIC_MODEL": model,
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
        "DISABLE_TELEMETRY": "1",
        "DISABLE_ERROR_REPORTING": "1",
        "DISABLE_AUTOUPDATER": "1",
        "IS_SANDBOX": "1",
    }
    if args.auth == "subscription":
        # The user's Claude plan (ADR 010): Claude Code signs in with its own long-lived
        # token variable and behaves as it does on any machine. The value here is a
        # placeholder; the runner's proxy outside the container holds the real token. No
        # API-key or bearer variable is set: either would outrank the plan sign-in.
        env["CLAUDE_CODE_OAUTH_TOKEN"] = "sk-ant-oat01-" + PLACEHOLDER
    else:
        env.update(
            {
                # A placeholder: the runner's proxy outside the container holds the real
                # token.
                "ANTHROPIC_AUTH_TOKEN": "local-only",
                "ANTHROPIC_DEFAULT_OPUS_MODEL": model,
                "ANTHROPIC_DEFAULT_SONNET_MODEL": model,
                "ANTHROPIC_DEFAULT_HAIKU_MODEL": model,
                "CLAUDE_CODE_SUBAGENT_MODEL": model,
                # Only the stable Messages protocol the relay validates; no experimental
                # betas.
                "CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS": "1",
            }
        )
    command = [
        CLAUDE,
        "-p",
        "--output-format",
        "stream-json",
        "--verbose",
        "--model",
        model,
        "--permission-mode",
        "bypassPermissions",
        "--setting-sources",
        "user",
        "--strict-mcp-config",
        "--mcp-config",
        str(config_dir / "taoloop-mcp.json"),
        "--disallowedTools",
        "WebSearch",
        "WebFetch",
    ]
    if args.session_turn:
        # Claude Code 2.1: a caller-chosen conversation id, continued with --resume.
        command.extend(["--resume", resume] if resume else ["--session-id", session])
        if getattr(args, "session_stream", False):
            # ADR 025: one process for the whole leg; later messages join the running turn.
            # Every message the agent consumes is echoed back, so the runner can tell when
            # all the messages it wrote have been answered and the leg's turn is over.
            command.extend(["--input-format", "stream-json", "--replay-user-messages"])
            return stream(command, prompt or args.objective, env, WORKSPACE)
        return tee(command, prompt or args.objective, env, WORKSPACE)
    return subprocess.run(
        command, input=prompt or args.objective, text=True, check=False, env=env, cwd=WORKSPACE
    )


SESSION_HOME = "/session"
UUID_PATTERN = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
TRANSCRIPT_MAX_BYTES = 65_536


class Teed:
    """What a teed agent run left behind: its exit code, how many JSON events it printed
    and the conversation id it announced (Codex ``thread.started``)."""

    def __init__(self, returncode, events, thread):
        self.returncode, self.events, self.thread = returncode, events, thread


def tee(command, prompt, env, cwd):
    """Run the agent with its JSON event stream passed through unchanged to stdout (the
    runner reads it), noting the events it printed and any new Codex thread id."""
    process = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=env,
        cwd=cwd,
    )
    try:
        process.stdin.write(prompt.encode())
        process.stdin.close()
    except BrokenPipeError:
        pass
    events, thread = 0, None
    output = os.fdopen(os.dup(1), "wb", buffering=0)
    try:
        for line in process.stdout:
            output.write(line)
            stripped = line.strip()
            if not stripped.startswith(b"{"):
                continue
            events += 1
            if thread is None and b"thread.started" in stripped:
                try:
                    event = json.loads(stripped)
                except ValueError:
                    continue
                value = event.get("thread_id") if isinstance(event, dict) else None
                if event.get("type") == "thread.started" and isinstance(value, str):
                    thread = value if UUID_PATTERN.match(value.lower()) else None
    finally:
        output.close()
    return Teed(process.wait(), events, thread)


STREAM_MESSAGE_CHARS = 20_000


def user_line(text):
    """One Claude Code stream-json user message line."""
    return (
        json.dumps(
            {
                "type": "user",
                "message": {"role": "user", "content": text},
                "parent_tool_use_id": None,
            }
        )
        + "\n"
    ).encode()


def stream(command, prompt, env, cwd):
    """Run Claude Code for a whole session leg (ADR 025): the first message, then every
    message the runner writes to this container's stdin (one ``{"text": ...}`` line each),
    passed to the running agent as a user line, so a message sent mid-turn joins that turn.
    Its JSON events pass through to stdout unchanged, as ``tee`` does. Closing the
    container's stdin ends the leg: the agent finishes its turn and exits."""
    process = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=env,
        cwd=cwd,
    )

    def forward():
        try:
            process.stdin.write(user_line(prompt))
            process.stdin.flush()
            for raw in sys.stdin:
                try:
                    value = json.loads(raw)
                except ValueError:
                    continue
                text = value.get("text") if isinstance(value, dict) else None
                if not isinstance(text, str) or not text.strip():
                    continue
                process.stdin.write(user_line(text[:STREAM_MESSAGE_CHARS]))
                process.stdin.flush()
        except (BrokenPipeError, OSError, ValueError):
            pass
        finally:
            try:
                process.stdin.close()
            except (BrokenPipeError, OSError):
                pass

    writer = threading.Thread(target=forward, daemon=True)
    writer.start()
    events = 0
    output = os.fdopen(os.dup(1), "wb", buffering=0)
    try:
        for line in process.stdout:
            output.write(line)
            if line.strip().startswith(b"{"):
                events += 1
    finally:
        output.close()
    writer.join(5)
    return Teed(process.wait(), events, None)


def codex_stream(command, prompt, env, cwd, resume=None):
    """Run ``codex app-server`` for a whole session leg (ADR 025): the first message starts a
    turn; each later message the runner writes to this container's stdin is steered into the
    running turn (``turn/steer``), or starts the next turn when none runs. App-server
    notifications pass through to stdout; beside them it prints the two markers the runner
    follows: ``{"type":"user","isReplay":true}`` for each message Codex took in, and
    ``{"type":"result"}`` when a turn completes. Closing stdin ends the leg after its turn."""
    process = subprocess.Popen(
        command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=env, cwd=cwd
    )
    lock = threading.RLock()
    state = {"next": 0, "thread": None, "turn": None, "starting": False, "eof": False}
    pending = {}
    waiting = []
    output = os.fdopen(os.dup(1), "wb", buffering=0)

    def emit(event):
        output.write((json.dumps(event) + "\n").encode())

    def send(method, params=None, *, notice=False, text=None):
        with lock:
            message = {"jsonrpc": "2.0", "method": method}
            if not notice:
                state["next"] += 1
                message["id"] = state["next"]
                pending[state["next"]] = (method, text)
            if params is not None:
                message["params"] = params
            try:
                process.stdin.write((json.dumps(message) + "\n").encode())
                process.stdin.flush()
            except (BrokenPipeError, OSError, ValueError):
                pass

    def text_input(text):
        return [{"type": "text", "text": text[:STREAM_MESSAGE_CHARS]}]

    def start(text):
        state["starting"] = True
        send("turn/start", {"threadId": state["thread"], "input": text_input(text)}, text=text)

    def deliver(text):
        with lock:
            if state["thread"] is None or state["starting"]:
                waiting.append(text)
            elif state["turn"] is not None:
                send(
                    "turn/steer",
                    {
                        "threadId": state["thread"],
                        "expectedTurnId": state["turn"],
                        "input": text_input(text),
                    },
                    text=text,
                )
            else:
                start(text)

    def settle():
        """Start the next waiting message, or end the leg once stdin closed and all is done."""
        with lock:
            if state["starting"] or state["turn"] is not None or state["thread"] is None:
                return
            if waiting:
                start(waiting.pop(0))
            elif state["eof"]:
                try:
                    process.stdin.close()
                except (BrokenPipeError, OSError):
                    pass

    def forward():
        for raw in sys.stdin:
            try:
                value = json.loads(raw)
            except ValueError:
                continue
            text = value.get("text") if isinstance(value, dict) else None
            if isinstance(text, str) and text.strip():
                deliver(text)
        with lock:
            state["eof"] = True
        settle()

    send("initialize", {"clientInfo": {"name": "taoloop", "version": "1"}})
    # Messages read before the thread and its first turn exist wait for them.
    threading.Thread(target=forward, daemon=True).start()
    events = 0
    try:
        for line in process.stdout:
            output.write(line)
            try:
                message = json.loads(line)
            except ValueError:
                continue
            if not isinstance(message, dict):
                continue
            events += 1
            ident = message.get("id")
            if ident in pending and "method" not in message:
                method, text = pending.pop(ident)
                failed = "error" in message
                result = message.get("result") or {}
                with lock:
                    if method == "initialize":
                        send("initialized", notice=True)
                        if resume:
                            send("thread/resume", {"threadId": resume})
                        else:
                            send(
                                "thread/start",
                                {"approvalPolicy": "never", "sandbox": "danger-full-access"},
                            )
                    elif method == "thread/resume" and failed:
                        send(
                            "thread/start",
                            {"approvalPolicy": "never", "sandbox": "danger-full-access"},
                        )
                    elif method in ("thread/start", "thread/resume") and not failed:
                        state["thread"] = (result.get("thread") or {}).get("id") or resume
                        start(prompt)
                    elif method == "turn/start":
                        state["starting"] = False
                        state["turn"] = None if failed else (result.get("turn") or {}).get("id")
                        if failed:
                            emit({"type": "result", "subtype": "error"})
                    elif method == "turn/steer" and failed and text is not None:
                        waiting.insert(0, text)  # the turn had just ended: start a new one
                if method in ("turn/start", "turn/steer"):
                    if method == "turn/start" and not failed:
                        while waiting and state["turn"] is not None:
                            deliver(waiting.pop(0))
                    settle()
                continue
            method = message.get("method")
            params = message.get("params") or {}
            if (
                method == "item/completed"
                and (params.get("item") or {}).get("type") == "userMessage"
            ):
                emit({"type": "user", "isReplay": True})
            elif method == "turn/completed":
                with lock:
                    state["turn"] = None
                emit({"type": "result", "subtype": "success"})
                settle()
    finally:
        output.close()
    return Teed(process.wait(), events, state["thread"])


def session_home():
    """The session's retained private agent home, mounted by the runner at /session."""
    info = os.lstat(SESSION_HOME)
    if (
        not os.path.isdir(SESSION_HOME)
        or os.path.islink(SESSION_HOME)
        or info.st_uid != os.getuid()
    ):
        raise SystemExit("Session turns require the retained private agent home")
    return SESSION_HOME


AGENT_HOME = "/agent-home"
# The competition refuses a candidate above this many bytes, and nothing larger is ever
# evaluated or submitted; a step whose planned candidate is still larger exits with this.
CANDIDATE_LIMIT_BYTES = 50_000
CANDIDATE_TOO_LARGE = 68
CANDIDATE_PATH = re.compile(r"[a-zA-Z0-9_./-]{1,255}\.py")


def candidate_size(path):
    target = Path(WORKSPACE) / path
    try:
        if target.is_symlink() or not target.is_file():
            return None
        return target.stat().st_size
    except OSError:
        return None


def shrink_prompt(path, size):
    return (
        f"The candidate at {path} is {size:,} bytes, but the competition accepts at most "
        f"{CANDIDATE_LIMIT_BYTES:,} bytes and refuses anything larger, so it cannot be "
        "evaluated or submitted. Reduce it below the limit without losing what made it "
        "better: remove comments, docstrings, dead code and unused helpers, and shorten long "
        "identifiers if needed. Check that it still runs, then stop."
    )


def keep_within_bound(args, home, port, toolkit, context, result):
    """The planned candidate must fit the competition's limit (2026-10-06: a 112,827-byte
    candidate was published and could not be evaluated). One more turn of the agent's own
    conversation is asked to shrink it; still too large, the step ends with its own code."""
    size = candidate_size(args.candidate_bound)
    if result.returncode != 0 or size is None or size <= CANDIDATE_LIMIT_BYTES:
        return result
    prompt = shrink_prompt(args.candidate_bound, size)
    thread = getattr(result, "thread", None) or (
        session_state(home).get("id") if args.keep_home else None
    )
    if args.agent == "codex" and thread:
        again = run_codex(args, home, port, toolkit, context, resume=thread, prompt=prompt)
    elif args.agent == "claude":
        again = run_claude(args, home, port, toolkit, context, prompt=prompt)
    else:
        again = run_codex(args, home, port, toolkit, context, prompt=prompt)
    size = candidate_size(args.candidate_bound)
    if size is not None and size > CANDIDATE_LIMIT_BYTES:
        raise SystemExit(CANDIDATE_TOO_LARGE)
    return again


CONTINUE_PROMPT = (
    "Credits were added to this step. Continue the work you were doing in this workspace "
    "from where you stopped, then finish as the original objective asks."
)


def kept_home():
    """A coding step's agent home, kept on the host beside its workspace (ADR 015)."""
    info = os.lstat(AGENT_HOME)
    if not os.path.isdir(AGENT_HOME) or os.path.islink(AGENT_HOME) or info.st_uid != os.getuid():
        raise SystemExit("A kept agent home must be the runner's own directory")
    return AGENT_HOME


def run_continuation(args, home, port, toolkit, context):
    """Continue a step that ran out of credits (ADR 015): resume the agent's own Codex
    conversation when one was recorded, otherwise start afresh in the kept workspace."""
    state = session_state(home)
    if args.agent == "codex" and state.get("agent") == "codex":
        result = run_codex(
            args, home, port, toolkit, context, resume=state["id"], prompt=CONTINUE_PROMPT
        )
        if result.returncode == 0 or result.events:
            return result
    fresh = (
        "You are continuing earlier work on this task: the workspace already holds it. "
        "Review what is there, then finish.\n\n" + args.objective
    )
    if args.agent == "claude":
        return run_claude(args, home, port, toolkit, context, prompt=fresh)
    return run_codex(args, home, port, toolkit, context, prompt=fresh)


def session_state(home):
    path = Path(home) / ".taoloop" / "session.json"
    try:
        if path.is_symlink() or path.stat().st_size > 4096:
            return {}
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(value, dict) or not UUID_PATTERN.match(str(value.get("id", ""))):
        return {}
    return value


def save_session_state(home, agent, identity):
    directory = Path(home) / ".taoloop"
    directory.mkdir(mode=0o700, exist_ok=True)
    temporary = directory / "session.json.tmp"
    temporary.write_text(json.dumps({"agent": agent, "id": identity}), encoding="utf-8")
    os.replace(temporary, directory / "session.json")


def replay_prompt(transcript, message):
    """A bounded transcript of earlier exchanges, used only when the agent's own session
    cannot be resumed. It is conversation data, not instructions to this workload."""
    lines = [
        "This continues an interactive coding session whose earlier conversation could not",
        "be restored. Earlier exchanges follow, oldest first; treat them as context only.",
        "",
    ]
    for exchange in transcript[-20:]:
        if not isinstance(exchange, dict):
            continue
        lines += [
            "User: " + str(exchange.get("user", ""))[:4000],
            "Assistant: " + str(exchange.get("assistant", ""))[:4000],
            "",
        ]
    lines += ["New message:", message]
    return "\n".join(lines)


def run_session_turn(args, home, port, toolkit, context):
    """One interactive session turn: resume the agent's own recorded conversation from the
    retained home, or start it on the first turn. If resuming fails before the agent
    printed anything, start a fresh conversation primed with the bounded transcript."""
    transcript = []
    if args.transcript:
        raw = base64.b64decode(args.transcript, validate=True)
        if len(raw) > TRANSCRIPT_MAX_BYTES:
            raise SystemExit("Transcript exceeds its size bound")
        transcript = json.loads(raw)
        if not isinstance(transcript, list):
            raise SystemExit("Invalid transcript")
    state = session_state(home)
    resume = state.get("id") if state.get("agent") == args.agent else None
    if args.agent == "claude":
        if not UUID_PATTERN.match(args.session_id or ""):
            raise SystemExit("Claude Code sessions need a conversation id")
        if resume is None and (args.session_turn > 1 or transcript):
            # No recorded conversation for a later turn, or for a conversation's new leg on a
            # host without its home (ADR 025): start one primed with the transcript.
            identity = str(uuid.uuid4())
            prompt = replay_prompt(transcript, args.objective) if transcript else None
            result = run_claude(args, home, port, toolkit, context, prompt=prompt, session=identity)
        else:
            identity = resume or args.session_id
            result = run_claude(args, home, port, toolkit, context, resume=resume, session=identity)
            if result.returncode != 0 and result.events == 0 and resume is not None:
                identity = str(uuid.uuid4())
                prompt = replay_prompt(transcript, args.objective)
                result = run_claude(
                    args, home, port, toolkit, context, prompt=prompt, session=identity
                )
        if result.events:
            save_session_state(home, "claude", identity)
        return result
    if resume is None and (args.session_turn > 1 or transcript):
        prompt = replay_prompt(transcript, args.objective) if transcript else None
        result = run_codex(args, home, port, toolkit, context, prompt=prompt)
    else:
        result = run_codex(args, home, port, toolkit, context, resume=resume)
        if result.returncode != 0 and result.events == 0 and resume is not None:
            result = run_codex(
                args, home, port, toolkit, context, prompt=replay_prompt(transcript, args.objective)
            )
    if result.thread:
        save_session_state(home, "codex", result.thread)
    return result


RUNNABILITY_LEVELS = ("build", "self_check", "offline_loop", "local_chain")
EMPTY_DIGEST = hashlib.sha256(b"").hexdigest()


# A line version's evaluate prints its score on its last stdout line (ADR 023 Phase 7).
SCORE_TAIL = 16384
ENV_NAME = re.compile(r"[A-Z][A-Z0-9_]{0,63}")
METRIC_NAME = re.compile(r"[A-Za-z0-9_.:-]{1,64}")
COMMAND_ARGUMENT = 1024


class BoundedCapture:
    def __init__(self, keep=2000):
        self.digest = hashlib.sha256()
        self.tail = bytearray()
        self.keep = keep
        self.lock = threading.Lock()

    def consume(self, stream):
        while chunk := stream.read(65536):
            with self.lock:
                self.digest.update(chunk)
                self.tail.extend(chunk)
                if len(self.tail) > self.keep:
                    del self.tail[: -self.keep]

    def last_line(self):
        with self.lock:
            text = bytes(self.tail).decode("utf-8", "replace")
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return lines[-1] if lines else ""

    def excerpt(self):
        with self.lock:
            text = bytes(self.tail).decode("utf-8", "replace")
        return "".join(char if char in "\n\t" or ord(char) >= 32 else " " for char in text)[-2000:]


def workspace_disk_bytes():
    total = 0
    for root, directories, files in os.walk(WORKSPACE, followlinks=False):
        directories[:] = [name for name in directories if not Path(root, name).is_symlink()]
        for name in files:
            try:
                total += Path(root, name).lstat().st_size
            except OSError:
                continue
    return total


def read_local_score(path):
    parts = path.split("/")
    descriptors = []
    try:
        parent = os.open(WORKSPACE, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        descriptors.append(parent)
        for name in parts[:-1]:
            parent = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            descriptors.append(parent)
        handle = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        with os.fdopen(handle, "rb") as stream:
            content = stream.read(4097)
    except OSError:
        return None
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
    try:
        value = json.loads(content)
        score = value["score"]
    except (ValueError, UnicodeError, KeyError, TypeError):
        return None
    if type(score) not in (int, float) or not math.isfinite(float(score)):
        return None
    return float(score)


def parse_score_line(line):
    """The score object a line version's evaluate printed last, or None:
    ``{"score": <finite number>, "metrics": {<name>: <finite number>, ...}}``."""
    try:
        value = json.loads(line)
    except (ValueError, UnicodeError):
        return None
    if not isinstance(value, dict):
        return None
    score = value.get("score")
    if type(score) not in (int, float) or not math.isfinite(float(score)):
        return None
    metrics = value.get("metrics", {})
    if not isinstance(metrics, dict) or len(metrics) > 32:
        return None
    parsed = {}
    for name, number in metrics.items():
        if (
            not isinstance(name, str)
            or not METRIC_NAME.fullmatch(name)
            or type(number) not in (int, float)
            or not math.isfinite(float(number))
        ):
            return None
        parsed[name] = float(number)
    return float(score), parsed


def write_runnability_report(report):
    content = json.dumps(report, sort_keys=True, separators=(",", ":")).encode()
    if len(content) > 65536:
        raise SystemExit("Runnability report exceeds its limit")
    descriptors = []
    try:
        parent = os.open(WORKSPACE, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        descriptors.append(parent)
        for name in (".taoloop", "runnability"):
            try:
                os.mkdir(name, mode=0o700, dir_fd=parent)
            except FileExistsError:
                pass
            parent = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            descriptors.append(parent)
        try:
            os.unlink("result.json", dir_fd=parent)
        except FileNotFoundError:
            pass
        handle = os.open(
            "result.json",
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=parent,
        )
        with os.fdopen(handle, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
    except OSError:
        raise SystemExit("Cannot write runnability report safely") from None
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def validate_runnability_plan(encoded, base_commit):
    try:
        raw = base64.b64decode(encoded, validate=True)
        plan = json.loads(raw)
    except (ValueError, UnicodeError):
        raise SystemExit("Invalid runnability plan") from None
    if len(raw) > 32768 or not isinstance(plan, dict):
        raise SystemExit("Invalid runnability plan")
    if (
        plan.get("schema_version") != 1
        or plan.get("repository_commit") != base_commit
        or not re.fullmatch(r"sha256:[a-f0-9]{64}", plan.get("image", ""))
        or plan.get("scope") not in ("platform_shared", "user_private", "line_version")
        or type(plan.get("gpu_required")) is not bool
        or type(plan.get("disk_mb")) is not int
        or not 64 <= plan["disk_mb"] <= 8192
        or not isinstance(plan.get("steps"), list)
        or not 1 <= len(plan["steps"]) <= 4
    ):
        raise SystemExit("Invalid runnability plan")
    total = 0
    for index, step in enumerate(plan["steps"]):
        command = step.get("command") if isinstance(step, dict) else None
        if (
            step.get("level") != RUNNABILITY_LEVELS[index]
            or not isinstance(command, list)
            or not 1 <= len(command) <= 32
            or any(
                not isinstance(item, str)
                or not item
                or len(item) > COMMAND_ARGUMENT
                or any(ord(char) < 32 for char in item)
                for item in command
            )
            or type(step.get("timeout_seconds")) is not int
            or not 1 <= step["timeout_seconds"] <= 1800
        ):
            raise SystemExit("Invalid runnability plan")
        total += step["timeout_seconds"]
        service = step.get("required_service")
        if (service is not None) != (step["level"] == "local_chain") or (
            service is not None and not re.fullmatch(r"[a-z][a-z0-9-]{1,39}", service)
        ):
            raise SystemExit("Invalid runnability plan")
        score_path = step.get("score_path")
        if score_path is not None and (
            step["level"] != "offline_loop"
            or not re.fullmatch(r"[a-zA-Z0-9_.-]+(?:/[a-zA-Z0-9_.-]+)*", score_path)
            or len(score_path) > 255
        ):
            raise SystemExit("Invalid runnability plan")
        if step.get("score_stdout") not in (None, True) or (
            step.get("score_stdout") and (step["level"] != "offline_loop" or score_path)
        ):
            raise SystemExit("Invalid runnability plan")
    if total > 3600:
        raise SystemExit("Invalid runnability plan")
    names = plan.get("env", [])
    if (
        not isinstance(names, list)
        or len(names) > 64
        or any(not isinstance(name, str) or not ENV_NAME.fullmatch(name) for name in names)
    ):
        raise SystemExit("Invalid runnability plan")
    return plan


def skipped_result(step):
    return {
        "level": step["level"],
        "command": step["command"],
        "status": "skipped",
        "duration_ms": 0,
        "cpu_seconds": 0.0,
        "peak_rss_kib": 0,
        "disk_bytes": workspace_disk_bytes(),
        "stdout_digest": EMPTY_DIGEST,
        "stderr_digest": EMPTY_DIGEST,
        "failure_excerpt": "",
    }


def run_runnability(encoded, base_commit):
    plan = validate_runnability_plan(encoded, base_commit)
    results = []
    stopped = False
    for step in plan["steps"]:
        if stopped:
            results.append(skipped_result(step))
            continue
        blocked = "gpu" if plan["gpu_required"] else step.get("required_service")
        if blocked is not None:
            result = skipped_result(step)
            result.update(status="blocked", blocked_service=blocked)
            results.append(result)
            stopped = True
            continue
        stdout = BoundedCapture(SCORE_TAIL if step.get("score_stdout") else 2000)
        stderr = BoundedCapture()
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        started = time.monotonic()
        timed_out = False
        process = subprocess.Popen(
            step["command"],
            cwd=WORKSPACE,
            # A line version's miner settings: exactly the variables the plan names, from
            # this container's environment; the fixed ones always win.
            env={
                **{name: os.environ[name] for name in plan.get("env", []) if name in os.environ},
                "PATH": AGENT_PATH,
                "HOME": "/tmp",
                "PYTHONDONTWRITEBYTECODE": "1",
            },
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        readers = [
            threading.Thread(target=stdout.consume, args=(process.stdout,), daemon=True),
            threading.Thread(target=stderr.consume, args=(process.stderr,), daemon=True),
        ]
        for reader in readers:
            reader.start()
        try:
            code = process.wait(timeout=step["timeout_seconds"])
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGKILL)
            code = process.wait()
        for reader in readers:
            reader.join(timeout=2)
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        disk_bytes = workspace_disk_bytes()
        failed_disk = disk_bytes > plan["disk_mb"] * 1024 * 1024
        status = "succeeded" if code == 0 and not timed_out and not failed_disk else "failed"
        excerpt = stderr.excerpt() or stdout.excerpt()
        if timed_out:
            excerpt = "command timed out" + (": " + excerpt if excerpt else "")
        elif failed_disk:
            excerpt = "workspace exceeded reviewed disk bound" + (": " + excerpt if excerpt else "")
        result = {
            "level": step["level"],
            "command": step["command"],
            "status": status,
            "duration_ms": min(3_600_000, int((time.monotonic() - started) * 1000)),
            "exit_code": code,
            "cpu_seconds": max(
                0.0,
                (after.ru_utime + after.ru_stime) - (before.ru_utime + before.ru_stime),
            ),
            "peak_rss_kib": max(0, after.ru_maxrss),
            "disk_bytes": disk_bytes,
            "stdout_digest": stdout.digest.hexdigest(),
            "stderr_digest": stderr.digest.hexdigest(),
            "failure_excerpt": excerpt[-2000:] if status == "failed" else "",
        }
        if status == "succeeded" and step.get("score_path"):
            score = read_local_score(step["score_path"])
            if score is not None:
                result["local_score"] = score
        if status == "succeeded" and step.get("score_stdout"):
            parsed = parse_score_line(stdout.last_line())
            if parsed is not None:
                result["local_score"], result["local_metrics"] = parsed
        results.append(result)
        stopped = status != "succeeded"
    incomplete = next((item for item in results if item["status"] != "succeeded"), None)
    completed = [item["level"] for item in results if item["status"] == "succeeded"]
    write_runnability_report(
        {
            "schema_version": 1,
            "repository_commit": plan["repository_commit"],
            "image": plan["image"],
            "scope": plan["scope"],
            "network_mode": "none",
            "gpu_required": plan["gpu_required"],
            "steps": results,
            "highest_completed": completed[-1] if completed else None,
            "stopped_at": incomplete["level"] if incomplete else None,
            "refused_outbound_connections": [],
        }
    )


# --- One model call on a plan sign-in (ADR 024) ---------------------------------------------
# The runner writes the call's input into the empty workspace and reads the reply after the
# container is removed. No repository, no Git bridge, no tools, no network but the bridge to
# the runner's plan proxy.
MODEL_CALL_INPUT_MAX = 1_000_000
MODEL_CALL_REPLY_MAX = 131_072


def model_call_directory():
    return Path(WORKSPACE) / ".taoloop" / "model_call"


def model_call_input():
    path = model_call_directory() / "input.json"
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError:
        raise SystemExit("Missing model call input") from None
    with os.fdopen(descriptor, "rb") as source:
        raw = source.read(MODEL_CALL_INPUT_MAX + 1)
    try:
        value = json.loads(raw)
    except ValueError:
        raise SystemExit("Invalid model call input") from None
    if (
        len(raw) > MODEL_CALL_INPUT_MAX
        or not isinstance(value, dict)
        or set(value) != {"prompt", "instructions", "max_output_tokens"}
        or not isinstance(value["prompt"], str)
        or not value["prompt"]
        or not isinstance(value["instructions"], str)
        or not value["instructions"]
        or type(value["max_output_tokens"]) is not int
        or not 16 <= value["max_output_tokens"] <= 32_000
    ):
        raise SystemExit("Invalid model call input")
    return value


def claude_once(args, home, port, value):
    """Claude Code headless, once: no tools, one turn, the plan's placeholder sign-in. Its
    JSON result is the reply; an error result is no reply."""
    config_dir = Path(home) / ".claude"
    config_dir.mkdir(mode=0o700, exist_ok=True)
    mcp = config_dir / "taoloop-mcp.json"
    mcp.write_text(json.dumps({"mcpServers": {}}))
    env = {
        **agent_env(home),
        "CLAUDE_CONFIG_DIR": str(config_dir),
        "ANTHROPIC_BASE_URL": f"http://127.0.0.1:{port}",
        "ANTHROPIC_MODEL": args.model,
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
        "CLAUDE_CODE_MAX_OUTPUT_TOKENS": str(value["max_output_tokens"]),
        "DISABLE_TELEMETRY": "1",
        "DISABLE_ERROR_REPORTING": "1",
        "DISABLE_AUTOUPDATER": "1",
        "IS_SANDBOX": "1",
        # A placeholder: the runner's proxy outside the container holds the real token.
        "CLAUDE_CODE_OAUTH_TOKEN": "sk-ant-oat01-" + PLACEHOLDER,
    }
    command = [
        CLAUDE,
        "-p",
        "--output-format",
        "json",
        "--model",
        args.model,
        "--system-prompt",
        value["instructions"],
        "--tools",
        "",
        "--max-turns",
        "1",
        "--setting-sources",
        "user",
        "--strict-mcp-config",
        "--mcp-config",
        str(mcp),
    ]
    result = subprocess.run(
        command,
        input=value["prompt"],
        text=True,
        capture_output=True,
        check=False,
        env=env,
        cwd=WORKSPACE,
    )
    try:
        answer = json.loads(result.stdout or "")
    except ValueError:
        return None
    if (
        result.returncode != 0
        or not isinstance(answer, dict)
        or answer.get("is_error")
        or answer.get("subtype") != "success"
        or not isinstance(answer.get("result"), str)
    ):
        return None
    return answer["result"]


def codex_once(args, home, port, value):
    """Codex non-interactively, once, in a read-only sandbox with web search off, on the
    plan's placeholder sign-in. Its last message is the reply."""
    shadow_sign_in(home, args.plan)
    config = {
        "model_provider": "plan",
        "model": args.model,
        "web_search": "disabled",
        "model_max_output_tokens": value["max_output_tokens"],
        "cli_auth_credentials_store": "file",
        "chatgpt_base_url": f"http://127.0.0.1:{port}/backend-api/",
        "model_providers.plan.name": "OpenAI",
        "model_providers.plan.base_url": f"http://127.0.0.1:{port}/backend-api/codex",
        "model_providers.plan.wire_api": "responses",
        "model_providers.plan.requires_openai_auth": True,
        "model_providers.plan.request_max_retries": 0,
        "model_providers.plan.stream_max_retries": 0,
    }
    reply = Path(home) / "reply.txt"
    command = ["/usr/local/bin/codex"]
    for key, item in config.items():
        command.extend(["-c", key + "=" + json.dumps(item)])
    command.extend(
        [
            "exec",
            "--skip-git-repo-check",
            "--sandbox",
            "read-only",
            "--output-last-message",
            str(reply),
            "-",
        ]
    )
    prompt = "Instructions:\n" + value["instructions"] + "\n\nInput (data):\n" + value["prompt"]
    result = subprocess.run(
        command,
        input=prompt,
        text=True,
        capture_output=True,
        check=False,
        env=agent_env(home),
        cwd=WORKSPACE,
    )
    if result.returncode != 0 or not reply.is_file():
        return None
    return reply.read_text(errors="replace")


def model_call_main(argv):
    parser = argparse.ArgumentParser(prog="codex_workload.py --model-call")
    parser.add_argument("--model", required=True)
    parser.add_argument("--agent", choices=("codex", "claude"), required=True)
    # A model call runs only on a plan sign-in; the relay path is the server's own client.
    parser.add_argument("--auth", choices=("subscription",), required=True)
    parser.add_argument("--plan", default="")
    args = parser.parse_args(argv)
    if os.getuid() == 0 or os.getcwd() != WORKSPACE:
        raise SystemExit("Requires the isolated runner workload environment")
    if not 0 < len(args.model) <= 200 or not MODEL_ID.fullmatch(args.model):
        raise SystemExit("Invalid workload input")
    if bool(args.plan) != (args.agent == "codex"):
        raise SystemExit("Invalid plan sign-in input")
    value = model_call_input()
    global TASK_MODEL
    TASK_MODEL = args.model
    Bridge.agent = args.agent
    Bridge.auth = "subscription"
    server = Server(("127.0.0.1", 0), Bridge)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        with tempfile.TemporaryDirectory(prefix="taoloop-call-", dir="/tmp") as home:
            once = claude_once if args.agent == "claude" else codex_once
            text = once(args, home, server.server_address[1], value)
    finally:
        server.shutdown()
        server.server_close()
    if not text or len(text) > MODEL_CALL_REPLY_MAX:
        raise SystemExit(1)
    descriptor = os.open(
        model_call_directory() / "reply.json",
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o600,
    )
    with os.fdopen(descriptor, "w") as target:
        json.dump({"text": text}, target)
    raise SystemExit(0)


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--model-call":
        model_call_main(sys.argv[2:])
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-commit", required=True)
    parser.add_argument("--model", required=True)
    # Base64 JSON of the immutable server-issued allowed model ids; never user prompt text.
    parser.add_argument("--model-pool", default="")
    parser.add_argument("--objective", required=True)
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--network", required=True)
    parser.add_argument("--netuid", type=int, required=True)
    parser.add_argument("--toolkit", default="")
    parser.add_argument("--agent", choices=("codex", "claude"), default="codex")
    parser.add_argument("--context", default="")
    # Interactive coding sessions (ADR 007 phase B): a checkout-only run, then one run
    # per agent turn with the retained agent home mounted at /session.
    parser.add_argument("--session-checkout", action="store_true")
    parser.add_argument("--session-turn", type=int, default=0)
    parser.add_argument("--session-id", default="")
    parser.add_argument("--transcript", default="")
    # ADR 025: one Claude Code process for a whole session leg; the runner writes later
    # messages to this container's stdin and they join the running turn.
    parser.add_argument("--session-stream", action="store_true")
    # Repository analysis (ADR 009): base64 JSON with the mode, commits, previous map and
    # result schema; the checkout is a public upstream and nothing is published.
    parser.add_argument("--analysis", default="")
    # Reviewed package execution: checkout the pinned base and run this JSON argv without an agent.
    parser.add_argument("--package-command", default="")
    parser.add_argument("--runnability-plan", default="")
    # Third-party service connections (ADR 011): base64 JSON of connection ids, providers
    # and the SDK base URL variables to point at this bridge.
    parser.add_argument("--services", default="")
    # Plan sign-in tasks (ADR 010): model requests go to the provider on the user's own
    # plan; --plan carries the non-secret facts Codex's placeholder sign-in mirrors.
    parser.add_argument("--auth", choices=("relay", "subscription"), default="relay")
    parser.add_argument("--plan", default="")
    # A coding step's agent home kept on the host at /agent-home, and continuing a step that
    # ran out of credits in its kept workspace (ADR 015).
    parser.add_argument("--keep-home", action="store_true")
    parser.add_argument("--continue", dest="continue_mode", choices=("resume", "publish"))
    # The candidate an evaluation plan names: checked against the competition's size limit
    # before the step ends.
    parser.add_argument("--candidate-bound", default="")
    args = parser.parse_args()
    configure_model_pool(args.model_pool, args.model)
    if args.candidate_bound and (
        not CANDIDATE_PATH.fullmatch(args.candidate_bound)
        or any(part in {"", ".", "..", ".git"} for part in args.candidate_bound.split("/"))
    ):
        raise SystemExit("Invalid candidate path")
    if (args.keep_home or args.continue_mode) and (
        args.analysis
        or args.package_command
        or args.runnability_plan
        or args.session_checkout
        or args.session_turn
    ):
        raise SystemExit("A kept agent home belongs to a coding step")
    if args.continue_mode and not args.keep_home:
        raise SystemExit("A continuation requires the kept agent home")
    if args.session_stream and not args.session_turn:
        raise SystemExit("A stream session is a session turn")
    if not re.fullmatch(r"[a-f0-9]{40}", args.base_commit):
        raise SystemExit("Invalid task baseline")
    # The runner appends research, reward, evaluation and outcome context to the task's
    # own objective (itself at most 8192 characters), so the bound here is the container's.
    if not 0 < len(args.objective) <= 65536 or not 0 < len(args.model) <= 200:
        raise SystemExit("Invalid workload input")
    if (
        not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", args.user_id)
        or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", args.network)
        or not 1 <= args.netuid <= 65535
    ):
        raise SystemExit("Invalid repository scope")
    if os.getuid() == 0 or os.getcwd() != "/workspace":
        raise SystemExit("Requires the isolated runner workload environment")
    if args.session_checkout and args.session_turn:
        raise SystemExit("Invalid session mode")
    if args.analysis and (
        args.session_checkout
        or args.session_turn
        or args.toolkit
        or args.package_command
        or args.runnability_plan
    ):
        raise SystemExit("Repository analysis runs alone")
    if args.package_command and (
        args.session_checkout
        or args.session_turn
        or args.toolkit
        or args.context
        or args.runnability_plan
    ):
        raise SystemExit("Package execution runs alone")
    if args.runnability_plan and (
        args.session_checkout or args.session_turn or args.toolkit or args.context
    ):
        raise SystemExit("Runnability execution runs alone")
    if not 0 <= args.session_turn <= 10000:
        raise SystemExit("Invalid session turn")
    # ADR 024: repository analysis may run on a platform operator's plan.
    if args.auth == "subscription" and (
        args.package_command or args.runnability_plan or args.session_checkout or args.session_turn
    ):
        raise SystemExit("A plan sign-in serves coding tasks only")
    if bool(args.plan) != (args.auth == "subscription" and args.agent == "codex"):
        raise SystemExit("Invalid plan sign-in input")
    if args.session_checkout or args.package_command or args.runnability_plan:
        # Checkout/package/runnability execution: the Git bridge only, no model route or agent.
        Bridge.allow_model = False
    server = Server(("127.0.0.1", 0), Bridge)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    if args.services:
        if args.analysis or args.package_command or args.runnability_plan or args.session_checkout:
            raise SystemExit("Service connections belong to coding agent runs")
        configure_services(args.services, server.server_address[1])
    try:
        with tempfile.TemporaryDirectory(prefix="taoloop-codex-", dir="/tmp") as home:
            if args.session_turn:
                # The checkout happened in an earlier run; the workspace is retained.
                if not any(Path(WORKSPACE).iterdir()):
                    raise SystemExit("Session turns require the retained workspace")
                home = session_home()
            elif args.continue_mode:
                # The kept workspace of the step this one continues (ADR 015).
                if not any(Path(WORKSPACE).iterdir()):
                    raise SystemExit("A continuation requires the kept workspace")
                home = kept_home()
                if args.continue_mode == "publish":
                    raise SystemExit(0)
                repoint_origin(server.server_port, home)
            else:
                if args.keep_home:
                    home = kept_home()
                checkout(
                    args.base_commit,
                    server.server_port,
                    home,
                    {"user_id": args.user_id, "network": args.network, "netuid": args.netuid},
                    analysis=bool(args.analysis or args.runnability_plan),
                    shallow=bool(args.runnability_plan) or analysis_mode(args.analysis) != "review",
                )
                if args.analysis:
                    prepare_analysis(args.analysis, args.base_commit)
            if args.session_checkout:
                raise SystemExit(0)
            if args.runnability_plan:
                run_runnability(args.runnability_plan, args.base_commit)
                raise SystemExit(0)
            if args.package_command:
                try:
                    raw = base64.b64decode(args.package_command, validate=True)
                    command = json.loads(raw)
                except (ValueError, UnicodeError):
                    raise SystemExit("Invalid package command") from None
                if (
                    len(raw) > 16384
                    or not isinstance(command, list)
                    or not 1 <= len(command) <= 32
                    or any(
                        not isinstance(item, str)
                        or not item
                        or len(item) > 256
                        or any(ord(char) < 32 for char in item)
                        for item in command
                    )
                ):
                    raise SystemExit("Invalid package command")
                result = subprocess.run(
                    command,
                    cwd=WORKSPACE,
                    env={"PATH": AGENT_PATH, "HOME": home, "PYTHONDONTWRITEBYTECODE": "1"},
                    stdin=subprocess.DEVNULL,
                    check=False,
                )
                raise SystemExit(0 if result.returncode == 0 else 1)
            toolkit, context = {}, ""
            if args.toolkit:
                raw = base64.b64decode(args.toolkit, validate=True)
                if len(raw) > TOOLKIT_MAX_BYTES:
                    raise SystemExit("Toolkit exceeds its size bound")
                toolkit = json.loads(raw)
            if args.context:
                raw = base64.b64decode(args.context, validate=True)
                if len(raw) > CONTEXT_MAX_BYTES:
                    raise SystemExit("Context exceeds its size bound")
                context = raw.decode("utf-8")
            global TASK_MODEL
            TASK_MODEL = args.model
            Bridge.agent = args.agent
            Bridge.auth = args.auth
            if args.session_turn:
                result = run_session_turn(args, home, server.server_port, toolkit, context)
            elif args.continue_mode:
                result = run_continuation(args, home, server.server_port, toolkit, context)
            elif args.agent == "claude":
                result = run_claude(args, home, server.server_port, toolkit, context)
            else:
                result = run_codex(args, home, server.server_port, toolkit, context)
            if args.keep_home and getattr(result, "thread", None):
                save_session_state(home, args.agent, result.thread)
            if args.candidate_bound and not args.session_turn:
                result = keep_within_bound(args, home, server.server_port, toolkit, context, result)
            # Reserve 64–68 for installed checks (68: candidate too large), never child exit
            # codes.
            raise SystemExit(0 if result.returncode == 0 else 1)
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
