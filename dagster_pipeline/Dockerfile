ARG PYTHON_IMAGE=ghcr.io/astral-sh/uv:python3.10-bookworm-slim
FROM $PYTHON_IMAGE

ENV PYTHONUNBUFFERED=1
ENV PATH="/project/.venv/bin:$PATH"

WORKDIR /project

# Copy dependency files first
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-cache

COPY . .

EXPOSE 3030

