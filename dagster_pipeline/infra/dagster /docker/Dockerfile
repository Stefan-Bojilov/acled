ARG PYTHON_IMAGE=ghcr.io/astral-sh/uv:python3.10-bookworm-slim  
FROM $PYTHON_IMAGE as dagster

ENV REGION_NAME=eu-north-1

RUN apt-get update && apt-get upgrade -yqq

ENV DAGSTER_HOME=/opt/dagster/dagster_home/
RUN mkdir -p $DAGSTER_HOME
WORKDIR $DAGSTER_HOME

COPY dagster.yaml workspace.yaml $DAGSTER_HOME

COPY pyproject.toml $DAGSTER_HOME
RUN uv pip install --system .

FROM dagster as webserver

# Install webserver extras
RUN uv pip install --system ".[webserver]"
