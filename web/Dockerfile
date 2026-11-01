# syntax=docker/dockerfile:1.7
#
# planning_mfe runtime image: a static Module Federation remote served by
# nginx. Kong never serves these bytes -- the fleet's localhost topology puts
# every frontend asset behind the Nginx web gateway on host :80, while Kong
# owns APIs on host :8000 (see warehouse-infra's ADR on the split edges).
#
# @warehouse/ui-kit is a sibling checkout (file:../../warehouse-ui-kit in
# package.json), not a registry package, so it must be supplied as a named
# BuildKit context. Build from this directory:
#
#   docker build --build-context uikit=../../warehouse-ui-kit \
#     -t warehouse/wes-work-planning-frontend:local .

# Builder base: node:22-bookworm-slim, not node:26-alpine. The committed
# package-lock.json carries both darwin and linux-x64-gnu/arm64-gnu entries
# for the platform-specific optional dependencies Vite 8 pulls in
# (@rolldown/binding-linux-*, lightningcss-linux-*, @oxlint/binding-linux-*),
# so the lockfile resolves on every host; Debian/glibc matches the *-gnu
# bindings that are actually published. Both stages below install with
# `npm ci` against that hash-verified, cross-platform lockfile (unlike an
# unpinned `npm install`), the pattern proven in sibling facility-layout's
# web/Dockerfile.
# --- ui-kit build stage (sibling dependency) ---
FROM node:22-bookworm-slim@sha256:4b150b48b4116e2a22f2c39cc27025fbf4f153e2078fd3b14d159fdb5c7279da AS uikit-build
WORKDIR /uikit
COPY --from=uikit . .
# The uikit named context is a sibling CHECKOUT on the developer's machine
# and may carry its own node_modules from a different host platform, so
# strip it before resolving; the checked-in package-lock.json itself is
# reused via `npm ci`, not regenerated. warehouse-ui-kit is gaining its own
# .dockerignore, but this stays so the build does not depend on which
# revision is checked out.
RUN --mount=type=cache,target=/root/.npm \
    rm -rf node_modules && npm ci && npm run build

# --- remote build stage ---
FROM node:22-bookworm-slim@sha256:4b150b48b4116e2a22f2c39cc27025fbf4f153e2078fd3b14d159fdb5c7279da AS build
WORKDIR /workspace/warehouse-ui-kit
COPY --from=uikit-build /uikit/package.json ./package.json
COPY --from=uikit-build /uikit/dist ./dist

WORKDIR /workspace/wes-work-planning/web
COPY package.json package-lock.json ./
RUN --mount=type=cache,target=/root/.npm \
    npm ci
COPY . .
RUN npm run build

# --- runtime stage ---
FROM nginxinc/nginx-unprivileged:1.31-alpine@sha256:b5047b9b76845eac124358373c6852377c52e4b05834a47647e955dd91cb8f1e
# apk upgrade picks up CVE fixes published to the alpine branch since the base
# image was last rebuilt, matching warehouse-console's runtime stage. Root is
# only needed for this one step.
USER root
RUN apk upgrade --no-cache
USER nginx
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build --chown=nginx:nginx /workspace/wes-work-planning/web/dist /usr/share/nginx/html
EXPOSE 8080
