# TaoLoop runner images

Generic workload images the TaoLoop runner runs on a miner's VPS. Licensed Apache-2.0.

| Image | Dockerfile | What it does |
| --- | --- | --- |
| coder | `Codex.Dockerfile` | Sandbox for coding agents, repository analysis, coding sessions, packages and runnability runs. Codex and Claude Code are not included: the runner downloads the pinned versions on the host and mounts them read-only. |
| harness | `Harness.Dockerfile` | Runs a subnet's own scoring code, mounted from its pinned commit, against a candidate. |
| miner_server | `MinerServer.Dockerfile` | Generic axon serving. |

Containers run with no network and a read-only root filesystem.

## Verifying an image

Each build is a GitHub release with `image.tar.zst` and `record.json`, and a build provenance attestation:

    gh attestation verify image.tar.zst --repo taoloop/runner-images
    zstd -dc image.tar.zst | docker load
    docker image inspect --format '{{.Id}}' <loaded tag>   # equals record.json image_id on Docker's containerd image store (config_digest on the classic store)

## Third-party content

Only pinned PyPI packages on a Python base image. A new third-party file needs its licence listed here.
