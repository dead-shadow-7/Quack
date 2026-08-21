# Quack search API — the only long-running service, and it also serves the UI.
#
# NOTE: build context is the REPO ROOT, not searchAPI/, because the image needs
# frontend/ as well. From the repo root:
#   docker build -t quack-api -f searchAPI/Dockerfile .
#   docker run --rm -p 3000:3000 -e MONGO_URI=mongodb://mongo:27017/ quack-api
#
# Debian slim rather than alpine: `natural` pulls native/optional deps that are
# fragile against musl, and the size win is not worth debugging that on deploy.
FROM node:22-slim

ENV NODE_ENV=production \
    PORT=3000 \
    FRONTEND_DIR=/frontend

WORKDIR /app

# Manifests first — this layer is cached until dependencies actually change.
# The glob tolerates a missing lock file, which matters here: package-lock.json
# is listed in .gitignore, so a fresh `git clone` has no lock to copy even
# though the working tree does. npm ci needs one; npm install does not.
COPY searchAPI/package.json searchAPI/package-lock.json* ./
RUN if [ -f package-lock.json ]; then \
        npm ci --omit=dev; \
    else \
        echo "WARNING: no package-lock.json — falling back to npm install (unpinned)"; \
        npm install --omit=dev; \
    fi && \
    npm cache clean --force

COPY searchAPI/ ./
COPY frontend/ ${FRONTEND_DIR}/

# The node images ship an unprivileged `node` user (uid 1000); nothing here
# writes to disk, so read-only ownership is fine.
USER node

EXPOSE 3000

# No curl/wget in slim, but node 22 has a global fetch.
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||3000)+'/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

# Direct exec form, no npm wrapper — so SIGTERM reaches node itself and the
# existing graceful-shutdown handler in server.js actually runs.
CMD ["node", "server.js"]
