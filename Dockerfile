FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"

COPY "pyproject.toml" "uv.lock" ".python-version" ./

RUN uv sync --locked

COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python", "ingest_data.py"]