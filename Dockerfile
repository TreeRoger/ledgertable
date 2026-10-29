FROM maven:3.9.9-eclipse-temurin-21
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /usr/local/bin/uv
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv make curl \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ENV UV_PYTHON_DOWNLOADS=never UV_LINK_MODE=copy

# Dependencies first, so source edits do not invalidate them.
COPY pom.xml .
RUN mvn -q dependency:go-offline
COPY py/pyproject.toml py/uv.lock py/
RUN mkdir -p py/ledger && touch py/ledger/__init__.py && cd py && uv sync --frozen -q

COPY . .
RUN make build
CMD ["make", "test"]
