# `docs/07_web_gui.md`

## Web GUI

The Sapflux pipeline will include a modern, responsive web-based Graphical User Interface (GUI) to provide a visual and interactive way to explore data, manage metadata, and administer the system.

**Note:** This document outlines the initial architectural and feature plan for the GUI. The specific user interface (UI) and user experience (UX) design will be developed iteratively after the core backend API is stable and well-defined.

### Technology Stack

The web GUI will be built with a modern, performance-oriented technology stack to ensure a fast, reliable, and maintainable application.

*   **Framework**: **Svelte 5 / SvelteKit** with TypeScript for a highly reactive and component-based architecture.
*   **Styling**: **Tailwind CSS** for a utility-first styling approach.
*   **Component Library**: **Shadcn-Svelte**, a collection of accessible and reusable components, will be used to accelerate development and ensure a consistent design.
*   **Authentication**: **Clerk**, via the community-maintained `svelte-clerk` library, will be fully integrated to handle all user sign-up, sign-in, and session management.

### Core Features and Pages

The GUI will be divided into sections based on user authentication status and roles, providing progressively more functionality.

#### 1. Public-Facing Landing Page

This is the view for any unauthenticated visitor. Its purpose is to provide a high-level overview of the project and encourage sign-in.

*   **Mini-Dashboard**: Displays a few key, public-safe statistics (e.g., "Number of Active Sites," "Total Data Points Processed," "Last Output Generated On").
*   **Project Information**: A brief description of the sap flux project's goals.
*   **Authentication**: A prominent "Sign In" button that initiates the Clerk authentication flow.

#### 2. Authenticated User Dashboard

Once a user has signed in, they will be directed to a comprehensive dashboard that provides tools for data exploration and download.

*   **Interactive Map**: A map view (using a library like Leaflet.js) displaying the geographic locations of all `sites`. Site markers could be clustered and clickable to drill down into `zones` and `plots`.
*   **Metadata Tables**: A set of interactive, sortable, and searchable tables for browsing all metadata resources (Projects, Deployments, Plants, Species, etc.).
*   **Data Download**: A user-friendly interface for listing and downloading final data `outputs`. Users will be able to select from different versions (e.g., the `@latest` version) and choose to include the "Reproducibility Cartridge" with their download.
*   **Transaction Log**: A view of the public transaction history, showing commit messages, timestamps, and outcomes.

#### 3. Admin-Level Transaction Management

Users with administrative privileges (as defined in Clerk) will have access to a dedicated section for managing the pipeline's state by creating and submitting transactions.

*   **Transaction Builder**: A guided, form-based interface for constructing a Transaction Manifest. This will include:
    *   Forms for adding or updating any metadata entity (e.g., "Create New Site," "Update Deployment").
    *   A file uploader for adding new raw data files to a transaction.
*   **Dry Run Functionality**: A "Validate" or "Dry Run" button that will submit the transaction to the `POST /transactions?dry_run=true` endpoint and display the resulting receipt to the user, allowing them to verify their changes before committing.
*   **Submission**: A "Commit Transaction" button that submits the final manifest to the API for processing.

### API Interaction

The Web GUI is a primary client of the same API used by the CLI. It will make authenticated requests to the documented endpoints (`/transactions`, `/outputs/{id}/download`, etc.).

To optimize the dashboard experience, a few specific, read-only API endpoints may be created. For example, instead of having the frontend download the entire outputs list to calculate a statistic, a dedicated endpoint like `GET /stats/summary` could provide pre-aggregated data. These endpoints would be guarded with the same Clerk authentication and would be considered an internal implementation detail supporting the GUI.

### Relevant Links

We will use shadcn svelte components. You can find those [here](https://www.shadcn-svelte.com/) The github repo is [here](https://github.com/huntabyte/shadcn-svelte).

For auth, we will use clerk. We will use the community-maintained, clerk-endorsed svelte-clerk repo. The docs are [here](https://svelte-clerk.netlify.app/). The repo is [here](https://github.com/wobsoriano/svelte-clerk)
