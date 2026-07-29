FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY src src
COPY models models
RUN uv sync --frozen --no-dev && useradd -r -u 10001 app && mkdir /data && chown app /data
USER app
ENTRYPOINT ["/app/.venv/bin/python", "-m", "pipeline.run"]
