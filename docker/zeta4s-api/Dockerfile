# syntax=docker/dockerfile:1.7
# zeta4s-api 이미지. engine 이 아니다.
#
# **Airflow base 위에 얹지 않는다.** engine 이미지 안에 zeta4s 가 들어가면 의존 방향이
# 뒤집힌다. zeta4s-api 는 Airflow 에 REST 로만 붙으므로 airflow package 가 필요 없고,
# 없는 것이 headless 의 최종 증명이다.
ARG PYTHON_BASE_IMAGE=python:3.12-slim

FROM ${PYTHON_BASE_IMAGE} AS zeta4s-wheel-builder

RUN python -m pip install --no-cache-dir "uv>=0.9,<1.0"

COPY pyproject.toml README.md /tmp/zeta4s-src/
COPY src /tmp/zeta4s-src/src
COPY packages /tmp/zeta4s-src/packages
RUN uv build --wheel --out-dir /tmp/zeta4s-dist /tmp/zeta4s-src \
    && uv build --wheel --out-dir /tmp/zeta4s-dist /tmp/zeta4s-src/packages/zeta4s-api

FROM ${PYTHON_BASE_IMAGE}

# ghcr 패키지를 공개 저장소에 연결한다. 연결된 패키지는 저장소 visibility 를 따른다.
LABEL org.opencontainers.image.source="https://github.com/zeta4lab/zeta4s" \
      org.opencontainers.image.licenses="Apache-2.0"

ARG PIP_INDEX_URL=
ARG PIP_TRUSTED_HOST=
# Airflow 이미지의 UID 다. zeta4s-state 볼륨을 airflow worker 와 공유하므로 파일 소유자가
# 맞아야 한다. 볼륨을 나누기 전까지는 이 값이 계약이다.
ARG ZETA4S_API_UID=50000

# oracledb 는 Thin mode 라 Oracle Client 가 필요 없다. dbt 가 git 을 쓴다.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip install --no-cache-dir "uv>=0.9,<1.0"

RUN groupadd -g 0 -o zeta4s 2>/dev/null || true \
    && useradd -u "${ZETA4S_API_UID}" -g 0 -m -d /home/zeta4s -s /bin/bash zeta4s

ENV VIRTUAL_ENV=/opt/zeta4s/.venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"
RUN mkdir -p /opt/zeta4s && chown -R "${ZETA4S_API_UID}:0" /opt/zeta4s

USER ${ZETA4S_API_UID}

COPY --chown=${ZETA4S_API_UID}:0 docker/zeta4s-api/requirements.txt /tmp/requirements.txt
RUN uv venv "${VIRTUAL_ENV}" --python "$(command -v python)" \
    && uv pip install --python "${VIRTUAL_ENV}/bin/python" --no-cache --strict \
        ${PIP_INDEX_URL:+--index-url "$PIP_INDEX_URL"} \
        ${PIP_TRUSTED_HOST:+--trusted-host "$PIP_TRUSTED_HOST"} \
        -r /tmp/requirements.txt \
    && rm -f /tmp/requirements.txt

COPY --from=zeta4s-wheel-builder --chown=${ZETA4S_API_UID}:0 /tmp/zeta4s-dist /tmp/zeta4s-dist
RUN uv pip install --python "${VIRTUAL_ENV}/bin/python" --no-cache --strict --no-deps \
        /tmp/zeta4s-dist/zeta4s-*.whl \
        /tmp/zeta4s-dist/zeta4s_api-*.whl \
    && uv pip check --python "${VIRTUAL_ENV}/bin/python" \
    && rm -rf /tmp/zeta4s-dist

# airflow 가 설치되지 않았음을 이미지가 스스로 증명한다. 누가 의존을 추가하면 build 가
# 깨진다 — 배포된 뒤 조용히 결합이 돌아오는 것보다 낫다.
RUN if "${VIRTUAL_ENV}/bin/python" -c "import airflow" 2>/dev/null; then \
        echo "airflow must not be installed in the zeta4s-api image" >&2; exit 1; \
    fi

WORKDIR /opt/zeta4s
ENTRYPOINT ["zeta4s-api"]
