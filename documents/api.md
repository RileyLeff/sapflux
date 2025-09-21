# Sapflux API

Unified path for the CLI and the web GUI to communicate with the app.

Build with rust, axum.

Provide means to validate transactions, start up the pipeline, and serve data. 

Needs to work with the clerk authentication. Consider how the Web GUI and CLI users will have their tokens stored and configured after sign in. 

People should also be able to curl data directly from the api or send a transaction directly to the api if they have proper auth attached, though I mostly expect that traffic to come from CLI and web GUI. 

I expect to build a dashboard page or two on the web GUI that use M2M tokens to query data from the API. we might need to add a couple dedicated endpoints that can fetch things from the database that are a little more heavily guarded than the other endpoints. I mostly want to keep the API simple on the user-facing side, e.g. just serve versions of the whole dataset instead of trying to be a query engine. Users can do that locally, the whole datasets are not that big. But for the dashboard we might need a few small dedicated endpoints to ask the DB for stuff. 