ARG PYTHON_IMAGE=ghcr.io/astral-sh/uv:python3.10-bookworm-slim
FROM $PYTHON_IMAGE

ENV REGION_NAME=eu-north-1

WORKDIR /project

COPY uv.lock pyproject.toml ./
RUN uv sync --frozen --no-cache

COPY . .