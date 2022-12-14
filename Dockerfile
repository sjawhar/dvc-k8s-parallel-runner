ARG PYTHON_VERSION=3.8.12

FROM golang:1.18.3-bullseye AS builder-tpi
WORKDIR /source
ARG TPI_REVISION=ff725420db9ff1e7fc3a0ebb1e48c4ef7c72c2d5
RUN git clone https://github.com/sjawhar/terraform-provider-iterative . \
 && git checkout ${TPI_REVISION} \
 && go build


FROM python:${PYTHON_VERSION}-bullseye AS builder-pip
WORKDIR /source
RUN pip install \
    --no-cache-dir \
        poetry==1.2.2

COPY . .
RUN poetry export --without-hashes --with wintermute > requirements.txt
RUN poetry build


FROM python:${PYTHON_VERSION}-bullseye AS base
WORKDIR /source
ARG TERRAFORM_VERSION=1.1.5
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        gnupg \
        software-properties-common \
        wget \
 && rm -rf /var/lib/apt/lists/* \
 && TERRAFORM_GPG_KEY=/usr/share/keyrings/terraform.gpg \
 && TERRAFORM_REPO=https://apt.releases.hashicorp.com \
 && wget -O- "${TERRAFORM_REPO}/gpg" | gpg --dearmor > "${TERRAFORM_GPG_KEY}" \
 && echo "deb [signed-by=${TERRAFORM_GPG_KEY} arch=amd64] ${TERRAFORM_REPO} $(lsb_release -cs) main" > /etc/apt/sources.list.d/terraform.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
        terraform=${TERRAFORM_VERSION} \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder-pip /source/requirements.txt /tmp
RUN pip install \
    --no-cache-dir \
    --requirement /tmp/requirements.txt

ARG UID=1000
ARG GID=1000
ARG USERNAME=neuromancer
ARG APP_DIR=/home/${USERNAME}/app
RUN groupadd -g ${GID} ${USERNAME} \
 && useradd -u ${UID} -g ${USERNAME} -G users -s /bin/bash -m ${USERNAME} \
 && mkdir -p ${APP_DIR} \
 && chown ${USERNAME}:${USERNAME} ${APP_DIR}

COPY --chown=${UID}:${GID} --from=builder-tpi \
    /source/terraform-provider-iterative \
    /usr/local/share/terraform/plugins/github.com/iterative/iterative/0.0.1/linux_amd64/

WORKDIR ${APP_DIR}


FROM base AS prod
RUN --mount=type=bind,from=builder-pip,source=/source/dist,target=/dist \
    pip install \
    --no-cache-dir \
    --no-deps \
        /dist/neuromancer-*.whl
USER ${USERNAME}


FROM base AS dev
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        bash-completion \
        jq \
        less \
        nano \
 && rm -rf /var/lib/apt/lists/*

RUN pip install \
    --no-cache-dir \
        poetry==1.2.2

WORKDIR ${APP_DIR}
COPY --chown=${USERNAME}:${USERNAME} . .
RUN PIP_NO_CACHE_DIR=true \
    POETRY_VIRTUALENVS_CREATE=false \
    poetry install \
    --with wintermute

USER ${USERNAME}
