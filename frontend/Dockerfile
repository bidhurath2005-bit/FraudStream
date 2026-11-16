# FraudStream dashboard: React build served by unprivileged nginx, which also reverse-proxies
# /api to the FastAPI container (same origin: no CORS, refresh cookie stays first-party).
FROM node:22-alpine AS build
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY . .
RUN npm run build

# Phase 15: current stable nginx on Alpine 3.24, pinned by digest (1.27-alpine / Alpine 3.21.3 had 68 HIGH/CRITICAL OS findings).
FROM nginxinc/nginx-unprivileged:1.30.6-alpine3.24@sha256:f5116ed4606f3eb015dd4a8e28cb9b351476dc0ad4bc6e9f93b19cec130cdf03
# Apply the Alpine 3.24 security updates published after the base image was built (same release branch only;
# Trivy found 13 fixable HIGH/CRITICAL OS findings in the unpatched base on 2026-11-16). Back to the image's uid 101.
USER root
RUN apk upgrade --no-cache
USER 101
COPY --from=build /src/dist /usr/share/nginx/html
COPY nginx/default.conf.template /etc/nginx/templates/default.conf.template
# API_UPSTREAM is substituted into the template at container start (official image envsubst).
ENV API_UPSTREAM=api:8000
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=6 CMD wget -q -O /dev/null http://127.0.0.1:8080/healthz || exit 1
