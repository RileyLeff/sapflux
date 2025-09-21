# Sapflux API

Unified path for the CLI and the web GUI to communicate with the app.

Build with rust, axum.

Provide means to validate transactions, start up the pipeline, and serve data. 

Needs to work with the clerk authentication. Consider how the Web GUI and CLI users will have their tokens stored and configured after sign in. 

People should also be able to curl data directly from the api or send a transaction directly to the api if they have proper auth attached, though I mostly expect that traffic to come from CLI and GUI. 

