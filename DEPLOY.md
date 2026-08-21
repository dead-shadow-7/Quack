# Deploying Quack to AWS (single EC2 + docker-compose)

The whole stack runs on one instance: Mongo and Redis as containers with named
volumes, the API as the only long-running service, and the crawler/indexer as
one-off jobs you invoke by hand.

## 1. Instance sizing

| | Recommendation | Why |
|---|---|---|
| Type | **t3.large** (2 vCPU / 8 GB) minimum | The indexer's Phase 1 holds every document's tokens in RAM at once — it is the memory ceiling of the system. `mem_limit: 4g` in compose is a guard, not a fix; an OOM-kill loses the entire run. |
| Storage | **30 GB gp3** | 20k documents at up to 50 000 chars each (`MAX_CONTENT_CHARS`) is ~1 GB of raw text, plus the inverted index, plus Docker images (~1.5 GB). |
| Ports | 22 (your IP only), 80 or 3000 (public) | Mongo (27017) and Redis (6379) are bound to `127.0.0.1` in compose and must **never** be opened — neither has authentication configured. |

The crawler runs 64 threads but is network-bound, not CPU-bound, so vCPU count
matters less than you would expect.

## 2. Bootstrap (Amazon Linux 2023)

```bash
sudo dnf update -y
sudo dnf install -y docker git
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user && newgrp docker   # log out/in if this is unreliable

# compose v2 as a CLI plugin
sudo mkdir -p /usr/libexec/docker/cli-plugins
sudo curl -sSL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o /usr/libexec/docker/cli-plugins/docker-compose
sudo chmod +x /usr/libexec/docker/cli-plugins/docker-compose
docker compose version
```

## 3. Run it

```bash
git clone <your-repo> quack && cd quack
cp .env.example .env          # set API_PORT=80 to serve the UI on the default port

docker compose up -d --build  # mongo + redis + api
curl localhost:3000/health    # {"status":"ok",...}
```

The UI is served by the API container at `http://<public-ip>:3000/` — the
frontend detects it is same-origin and calls `/search` relatively, so there is
nothing to configure.

Then populate the data, in order. Both are long-running; use `screen`/`tmux` or
`-d`, because these are hours, not minutes:

```bash
docker compose run --rm crawler   # fills `documents` — 20 000 pages
docker compose run --rm indexer   # builds `index` from `documents`
```

`git clone` does not bring a `package-lock.json` (it is in `.gitignore`), so the
API image will fall back to `npm install` and print a warning. See "Known
issues" below.

## 4. Operating notes

These are properties of the existing pipeline, not of the Docker setup, but they
bite differently once this is public:

- **Reindexing causes a search outage.** The indexer's Phase 3 calls
  `index.drop()` before repopulating, so `/search` returns empty results for the
  full duration of a reindex. There is no blue/green swap.
- **Stale results for 5 minutes after a reindex.** The API caches by normalized
  query string in-process (`node-cache`, 5 min TTL). Restart the API container
  (`docker compose restart api`) right after indexing to flush it.
- **The crawler's budget counts failures.** Stopping condition is
  `crawled + failed + skipped >= MAX_PAGES`, so a bad network run terminates
  early with far fewer than 20 000 real documents.
- **One slow host can stall the crawler.** `is_allowed()` fetches `robots.txt`
  while holding a global lock, with no timeout on `RobotFileParser.read()`, so
  every first-seen origin serializes all 64 threads behind one request.
- **Back up the volumes, not the containers.** All state lives in the `quack_mongo-data`
  and `quack_redis-data` named volumes. Snapshot the EBS volume, or
  `docker compose exec mongo mongodump`. Redis holds only transient `idx:*` keys
  between indexer phases and is safe to lose while no indexing run is active.

## 5. Known issues to fix before this is really production

- **No authentication on MongoDB or Redis.** Safe only because both are bound to
  loopback. If you ever split these onto separate hosts, enable auth first.
- **`package-lock.json` is gitignored**, so cloned builds are unpinned. Removing
  line 3 of `.gitignore` and committing the lock file makes image builds
  reproducible.
- **No HTTPS.** Put an ALB with an ACM certificate in front, or run caddy/nginx
  as a TLS terminator, and set `API_PORT` back to a loopback-only bind.
- **The stemming mismatch is still present** — query terms are Porter-stemmed in
  Node while the index is Snowball-stemmed in Python, so common words like
  `news`, `status`, `focus` and `quickly` retrieve nothing. Deploying does not
  change this; see CLAUDE.md for the fix and note it requires a full reindex.
