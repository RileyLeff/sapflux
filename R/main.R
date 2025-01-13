source("R/get_constants.R")
source("R/external_file_util.R")

# R/gdrive_util.R
# authenticates to gdrive using a hidden .json key attached to a gcloud service account
# save your key locally at (relative to the project root) .env/key.json
# alternativey, authenticate interactively if you set the environment variable
#   SAP_RUN_SETTING = "interactive"
#   Option 1: Sys.setenv(SAP_RUN_SETTING = "interactive") from within R console
#   Option 2: export SAP_RUN_SETTING="interactive" from your bash-like terminal emulator
# Don't you dare put Sys.setenv(SAP_RUN_SETTING = "interactive") in the project code. 
# That's a feature of the environment the code executes in, not a feature of the code itself. 

# auth_interactive_or_from_path_to_key(
#     path_to_key = constants$path_to_key,
#     env_label = constants$env_label
# )