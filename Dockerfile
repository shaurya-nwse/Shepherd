FROM python:3.8-slim-buster as base
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential

ENV PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random


FROM base as envsetup
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

ARG POETRY_VERSION="1.1.11"
RUN pip install "poetry==${POETRY_VERSION}"
WORKDIR /app
COPY poetry.lock pyproject.toml ./
RUN poetry install --no-dev
COPY . .
RUN poetry build && pip install dist/*.whl

FROM envsetup as final
WORKDIR /app
CMD ["python", "kafka_kinesis/main.py"]
