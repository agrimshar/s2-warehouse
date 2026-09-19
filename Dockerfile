FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
RUN apt-get update \
    && apt-get install -y --no-install-recommends libexpat1 \
    && rm -rf /var/lib/apt/lists/*
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy 
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project 
COPY src ./src
RUN uv sync --frozen --no-dev 
ENTRYPOINT ["uv", "run", "--frozen", "--no-dev", "python", "-m"]
CMD ["s2warehouse.ingest", "--limit", "1"]