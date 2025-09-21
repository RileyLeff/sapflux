### The `db seed` Command

You're right, this should not be part of the public-facing CLI. The `justfile` provides the perfect abstraction layer. Here's how it will work:

1.  **A Hidden CLI Command**: The Rust application will still contain the logic for seeding. We will implement a command like `sapflux admin db-seed`, but we will use `clap`'s `hidden = true` attribute. This means the command is fully functional but does not show up in the standard `--help` output, keeping the user-facing CLI clean.
2.  **The `justfile` Recipe**: The `justfile` in the project root will contain a simple, memorable recipe for the administrator.

    ```makefile
    # justfile

    # Seed the database with the application's compiled-in components (parsers, pipelines, etc.).
    # Should only be run once on initial setup or after deploying a new version with new components.
    db-seed:
        #!/usr/bin/env bash
        set -euo pipefail
        echo "Seeding database with application components..."
        # Calls the hidden, internal command
        docker compose exec app sapflux admin db-seed
    ```
