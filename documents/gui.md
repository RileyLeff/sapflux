# Sapflux Web UI

I will initialize a svelte 5 + sveltekit project with typescript and tailwind set up.

We will use shadcn svelte components. You can find those [here](https://www.shadcn-svelte.com/) The github repo is [here](https://github.com/huntabyte/shadcn-svelte).

For auth, we will use clerk. We will use the community-maintained, clerk-endorsed svelte-clerk repo. The docs are [here]. The repo is [here](https://github.com/wobsoriano/svelte-clerk)

The web UI will have a simple public landing page with a mini dashboard for viewing basic stats about sap flux (number of rows in last dataset produced, number of active deployments, number of active sites, that kind of stuff), with a sign in button.

Authenticated users with the appropriate privileges will be able to see a much more detailed dashboard, showing the status of all the tables interactively, a map, a data download button with a menu to choose between versions and some hover info.

Authenticated users with admin access will be able to create and send transactions to the database (e.g. add more data, change metadata, that kind of stuff).

I expect to build a dashboard page or two on the web GUI that use M2M tokens to query data from the API. we might need to add a couple dedicated endpoints that can fetch things from the database that are a little more heavily guarded than the other endpoints. I mostly want to keep the API simple on the user-facing side, e.g. just serve versions of the whole dataset instead of trying to be a query engine. Users can do that locally, the whole datasets are not that big. But for the dashboard we might need a few small dedicated endpoints to ask the DB for stuff. 