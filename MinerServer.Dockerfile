# Reference miner-server image for the hosting lifecycle (docs/DEPLOYMENTS.md). Stdlib only.
# Build from infra/runner:  docker build -f MinerServer.Dockerfile -t taoloop-miner-server:ref .
FROM python:3.12-slim
COPY --chmod=0555 miner_server.py /opt/taoloop/miner_server
RUN chmod 0555 /opt/taoloop && sed -i '1i #!/usr/local/bin/python3' /opt/taoloop/miner_server
WORKDIR /candidate
USER 10001:10001
ENV PYTHONDONTWRITEBYTECODE=1
ENTRYPOINT ["/opt/taoloop/miner_server"]
