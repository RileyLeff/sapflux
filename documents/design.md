transactions - multiple updates hitting the pipeline at once. each transaction produces 1 new run.

need to separate parsed cache from raw data store. need to be able to clear cache easily (e.g. via command, upon changing parsers, upon changing parsing parameters, etc).

don't want to commit new data to final data storage until we've verified that it is attached to a deployment. maybe a flag for whether it's attached to a known deployment that isn't being edited in the current transaction.

need a way to delete data. if we delete deployment, need to clean up associated data left hanging as well. can't let state get out of sync.

in the dashboard, map of sites with hover for deployments overlay, collapse to circle with number in it over region that contains all active deployments

need to be able to mark datasets as deprecated (but they're still ok)

need to figure out what it means to be able to completely reproduce the resulting dataset. what info does someone need to be able to do that? I guess is it possible to include 1.) the code to run the pipeline 2.) the entire internal state of the pipeline, its data and db and stuff, etc as a reproducible single transaction, that will guaranteed reproduce the same outcome, and is at least convertible into a user-inspectable, semantically meaningful format? Maybe we store some heavily-compressed "cartridge" format that locks the entire database post-transaction as if it took 1 transaction to rebuild the whole thing and we put it in R2?

species metadata table -- needs icons! will make the dashboard nice

transactions should have a commit message or a changelog or something in them

run-level config (e.g. turn timestamp unfucker on/off, )


# Sapflux Pipeline

Alright cool. So I'm building a data pipeline for a sap flux project. Here's how I want it to work.


## User Interaction Surface and Auth
There should be a unified API that users can access via a CLI or a web UI. The API should have some auth, privileges, permissioning. For example, maybe public users can only see a subset of stuff, authenticated users can download data on top of the public stuff, and authenticated users designated as admins can 

The CLI should be written in rust using clap.

The web UI should be written in svelte 5 with sveltekit using typescript. We should use shadcn-svelte components. You can supplement with bits-ui and similar styling if we need something not in shadcn-svelte.

The API should be written in rust using axum.

I want to use clerk for authentication. We should use standard web components from the community maintained svelte clerk repo for this on the web. The CLI should also use clerk, it should have some command to open the browser and authenticate with the web ui, you get access/refresh tokens, and you send Authorization: Bearer … to your API. Your API verifies the Clerk token and checks roles/metadata to decide “is this person allowed here?”

We should maintain some ability to scope permissions, e.g. stuff on the api, the web ui, the cli should be marked as public, needs auth (e.g. download data), needs admin role (modify data etc).

The main things we need to be able to do via the api (e.g. the UI and the CLI) are:
- add more data
- delete previous data or mark it as "inactive"
- clear caches
- alter metadata and config
Since these things all interact, any changes to the pipeline need to be grouped into a "transaction". So for example, a user might log on or open up the CLI and start up a new transaction, add a new piece of data, change some metadata, whatever, and then send it all as one big transaction. Even single events (e.g. add more data) should be wrapped in a transaction.

## How The Pipeline Works

### Data Ingest
There should be parsers, written in rust, right now just 2 but maybe more in the future. The parsers either return an error/fail to parse (we should use thiserror 2.0 for library code like this) or a successfully parsed dataset. We store the parsers in some iterable data structure. So we take a new piece of data that a user sent to the program as part of a transaction -> check if it parses in one parser -> if it doesn't, check if it parses in the next, and so on, until we get a success. Then we try the next file, and repeat until we've parsed or rejected all files.

The parsers should extract and structure key information about the files. We need to know 1.) Which datalogger id the file came from 2.) the "logger-level information" stored in the table (e.g. timestamp, record number, battery voltage, that kind of stuff.) 3.) a vector of sensor-level data for some number of sensors attached to the logger. 4.) those sensor-level data should have an sdi-12 address mapped to a vector of ThermistorPairs. Each thermistor pair contains some designation of its depth (usually inner or outer, though we should support more flexible thermistorpair naming and ordering conventions in the future) and the data it produces. For now, one parser format contains a superset of the other parser format's data, so what we should do is store the entire (maximum) set of columns in the data structure, with the additional ones as an Option type.

We should note that we're renaming and cleaning up the column names in each of these for clarity. A file called planning/columnnames.toml has the column name info.

A valid sdi12 address is 0-9, a-z, A-Z, single char. Could be nice to store as a newtype wrapper around ascii:char with a fallible constructor, or similar.

## Metadata 

See some notes about metadata layout in planning/metadata.md.

