Here’s the whole stack—short and sweet:

# Edge & DNS

* **Cloudflare**: DNS + proxy/WAF/CDN, SSL **Full (strict)**, cache static (`/_app/*`, `/assets/*`), bypass `/api/*` & authed.

# Front door & access

* **Caddy** (HTTP/2/3, HSTS, gzip/br) behind Cloudflare.
* **Tailscale** (SSH + ACLs) for private admin/DB.

# App

* **Svelte 5 + SvelteKit (TS)** (adapter-node)
* **Shadcn UI Svelte + Tailwind**
* **Rust (Clap)** CLI
* **Rust (Axum)** API
* **Rust** Logic, parsers, calculations, timestamp algorithm, etc


# Data & storage

* **Postgres 17** (primary DB)
* **Cloudflare R2** (buckets)

# Auth & secrets

* **Clerk** (SSR verification; local `users` table keyed to Clerk ID)
* **Doppler** (env/secrets: service token on host or CI-materialized `.env`)

# Analytics

* **Umami** (self-host, events + pages)

# Backups & DR

* **WAL-G → R2** (weekly base + continuous WAL; monthly restore drill)
* **Hetzner Backups** (VM snapshots, +20%)
* Store static series of transactions, whole database rebuildable to any point in time from there
* Store and back up versioned data outputs in R2. 
* Minio for local bucket dev stuff

# Observability & ops

* **Uptime-Kuma** (external checks) + **Healthchecks** (cron heartbeats)
* **tracing/logs/metrics**: `tracing` JSON; `node_exporter` 
  
# Containers & CI/CD

* **Docker** everywhere + **Docker Compose** (healthchecks, limits, restart policies) (ubuntu 24 and/or official images for various services preferred over alpine)
* **GitHub Actions** 
* **Just** + **uv** (PEP-723 ops scripts, which are compatible with uv. you can use simple shell commands for simple just recipes, but look into python/uv compatibility with just if you ), **Ruff**, **Astral `ty`** (type checker)

# Infra as Code & hosting

* **Pulumi (Python)** + `pulumi-hcloud` (SSH key, firewall, server w/ cloud-init, Floating IP, Backups)
* **Hetzner Cloud**: CPX11, Ashburn