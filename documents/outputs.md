data outputs should be parquet files

you get one output per successful transaction

data outputs should be stored in r2 and they should have a pointer to their location in r2 in some postgres table that maps the actual data to the info about the run. 