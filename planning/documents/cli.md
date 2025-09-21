# Sapflux CLI

Rust, clap.

Auth via clerk by opening up a browser via a dedicated command, store a token somewhere useful.

Priviledged users can e.g. create a new transaction, give it a message, open it, put some stuff in it, close it, send it.

Alternatively they can push a toml describing their transaction. 

Needs a dry run flag. 

Privileged users should also be able to download versions of the data via the CLI. We should be able to use semantics like named version, calver, by hash, download "@latest", that kind of thing.