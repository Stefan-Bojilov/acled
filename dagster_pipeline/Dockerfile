ARG PYTHON_IMAGE=ghcr.io/astral-sh/uv:python3.10-bookworm-slim
FROM $PYTHON_IMAGE

ENV REGION_NAME=eu-north-1
ENV EMAIL=s.a.bojilov@umail.leidenuniv.nl
ENV S3_BUCKET=dagster-acled-bucket

WORKDIR /project

COPY uv.lock pyproject.toml ./

RUN uv sync --frozen --no-cache

COPY . . 

EXPOSE 3000

CMD ["uv", "run", "dagster", "dev", "--host", "0.0.0.0", "--port", "3000"]