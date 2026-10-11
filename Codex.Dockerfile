# Build context contains only the three installed drivers. Codex and Claude Code are not
# part of the image; one coder image serves both agents.
FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --chmod=0444 codex_workload.py /opt/taoloop/codex_workload.py
COPY --chmod=0444 codex_history.py /opt/taoloop/codex_history.py
COPY --chmod=0444 codex_publish.py /opt/taoloop/codex_publish.py
RUN chmod 0755 /opt/taoloop
WORKDIR /workspace
# Codex and Claude Code are mounted read-only by the TaoLoop runner at task start.
USER 10001:10001
RUN python /opt/taoloop/codex_workload.py --help > /dev/null \
    && python /opt/taoloop/codex_history.py --help > /dev/null
ENTRYPOINT ["python", "/opt/taoloop/codex_workload.py"]
