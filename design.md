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