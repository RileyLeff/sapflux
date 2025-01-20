test_gdrive_auth_and_download <- function(drive_id = "13X7l7a7vGK2J5YwTX-4zjf8KpPsGSD8XWdXbDfU8BXk") {
    # Clear any existing token
    googledrive::drive_deauth()
    message("Starting authentication test...")
    
    # Test 1: Authentication
    tryCatch({
        credentials <- gargle::credentials_service_account(
            path = ".env/key.json",
            scopes = c(
                "https://www.googleapis.com/auth/drive.readonly",
                "https://www.googleapis.com/auth/drive.file",
                "https://www.googleapis.com/auth/drive.metadata.readonly"
            )
        )
        message("✓ Successfully created credentials")
        
        # Print credential details
        message("Token type: ", credentials$token_type)
        message("Expires in: ", credentials$expires_in, " seconds")
        
        googledrive::drive_auth(
            token = credentials,
            path = ".env/key.json",
            cache = FALSE
        )
        message("✓ Successfully set authentication")
    }, error = function(e) {
        stop("Authentication failed: ", e$message)
    })
    
    # Test 2: Check if we can get file metadata
    tryCatch({
        file_info <- googledrive::drive_get(googledrive::as_id(drive_id))
        message("✓ Successfully retrieved file metadata:")
        message("  File name: ", file_info$name)
        message("  File ID: ", file_info$id)
        message("  MIME type: ", file_info$drive_resource[[1]]$mimeType)
    }, error = function(e) {
        stop("Failed to get file metadata: ", e$message)
    })
    
    # Test 3: Try to download
    temp_file <- tempfile()
    tryCatch({
        googledrive::drive_download(
            file = googledrive::as_id(drive_id),
            path = temp_file
        )
        message("✓ Successfully downloaded file to: ", temp_file)
        message("  File size: ", file.size(temp_file), " bytes")
    }, error = function(e) {
        stop("Failed to download file: ", e$message)
    })
    
    return(TRUE)
}

# Run the test
test_gdrive_auth_and_download()
