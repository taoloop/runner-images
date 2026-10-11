# Validator harness image: a pinned Python runtime and the image-owned driver. It carries
# no subnet code; the runner mounts the reviewed profile's scoring files read-only at
# /inputs for each evaluation. A subnet whose scoring code needs other packages needs its
# own reviewed image (approved like any other: setting plus signed manifest role).
# Build context: infra/runner.
FROM ghcr.io/astral-sh/uv:0.12.8 AS uv
FROM python:3.12-slim
COPY --from=uv /uv /bin/uv
COPY --chmod=0444 harness-requirements.txt /opt/taoloop/harness-requirements.txt
RUN uv pip install --system --no-cache -r /opt/taoloop/harness-requirements.txt
COPY --chmod=0444 harness_workload.py /opt/taoloop/harness_workload.py
RUN chmod -R a+rX /opt/taoloop
WORKDIR /workspace
USER 10001:10001
ENV PYTHONDONTWRITEBYTECODE=1 OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
ENTRYPOINT ["python", "/opt/taoloop/harness_workload.py"]
