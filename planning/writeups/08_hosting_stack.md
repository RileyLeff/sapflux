# `docs/08_hosting_stack.md`

## Production Hosting Stack

This document outlines the proposed technology stack for the production deployment of the Sapflux pipeline. The stack is chosen to be robust, cost-effective, and maintainable.

### Infrastructure & Hosting

*   **Cloud Provider**: Hetzner Cloud (Ashburn, VA location). CPX11 instance.
*   **Infrastructure as Code**: Pulumi (with Python) to manage server provisioning, firewalls, and networking.
*   **Edge & DNS**: Cloudflare for DNS, CDN, and as a security proxy (WAF).
*   **Web Server**: Caddy as a reverse proxy on the host machine, handling automatic HTTPS, HTTP/2, and HSTS. We will use Cloudflare's DNS-01 provider (via the `caddy-dns/cloudflare` plugin) to issue certificates so that TLS succeeds even when Cloudflare is proxying the origin. As a fallback, Cloudflare origin certificates can be provisioned during bootstrap.
*   **Admin Access**: Tailscale for secure SSH access and private networking, eliminating the need for a public SSH port.

### Application & API

*   **Backend API**: Rust with the **Axum** web framework.
*   **Frontend GUI**: **Svelte 5 / SvelteKit** with TypeScript.
*   **CLI**: Rust with the **Clap** argument parsing library.

### Data & Storage

*   **Primary Database**: PostgreSQL (Version 17+) with the **PostGIS** extension enabled.
*   **Object Storage**:
    *   **Production**: Cloudflare R2.
    *   **Local Development**: MinIO.
*   **Database Backups**: `WAL-G` for continuous archiving and point-in-time recovery, sending backups directly to an R2 bucket.

### Authentication & Secrets

*   **Authentication**: **Clerk** is the central, authoritative service for all user management, authentication, and session control for the API, GUI, and CLI.
*   **Secrets Management**: Doppler for securely injecting environment variables and application secrets at runtime and in CI/CD.

### Analytics & Monitoring

*   **Web Analytics**: **Self-hosted Umami** for privacy-respecting website analytics. It will be run as another container within the Docker Compose setup.
*   **Uptime Monitoring**: A combination of **Uptime-Kuma** for external endpoint checks and **Healthchecks.io** for cron job monitoring.

### CI/CD & Operations

*   **Containerization**: **Docker** and **Docker Compose** for local development, testing, and production deployment.
*   **CI/CD**: **GitHub Actions** for automated testing, building Docker images, and deploying changes.
*   **Task Runner**: **Just** (`justfile`) for orchestrating common development and operational commands.
