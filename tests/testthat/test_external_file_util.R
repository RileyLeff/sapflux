# tests/testthat/test-external_file_util.R

test_that("get_remote_driver sets download directory correctly", {
    # Mock chrome options
    test_options <- list(
        chromeOptions = list(
            prefs = list()
        )
    )
    
    # Mock remoteDriver
    mock_driver <- structure(list(
        open = function(...) NULL
    ), class = "remoteDriver")
    
    # Mock RSelenium::remoteDriver
    with_mock(
        "RSelenium::remoteDriver" = function(...) mock_driver,
        {
            result <- get_remote_driver(
                chrome_options = test_options,
                port = 4445,
                save_path = "/test/path"
            )
            
            # Check if download directory was set correctly
            expect_equal(
                test_options$chromeOptions$prefs["download.default_directory"],
                "/test/path"
            )
        }
    )
})

test_that("get_remote_driver handles connection failure", {
    # Mock chrome options
    test_options <- list(
        chromeOptions = list(
            prefs = list()
        )
    )
    
    # Mock failing remoteDriver
    mock_failing_driver <- structure(list(
        open = function(...) stop("Connection failed")
    ), class = "remoteDriver")
    
    # Mock RSelenium::remoteDriver
    with_mock(
        "RSelenium::remoteDriver" = function(...) mock_failing_driver,
        {
            expect_error(
                get_remote_driver(
                    chrome_options = test_options,
                    port = 4445,
                    save_path = "/test/path"
                ),
                "Connection failed"
            )
        }
    )
})

test_that("get_remote_driver uses correct port", {
    # Create a spy to capture arguments
    called_args <- NULL
    mock_remote_driver <- function(...) {
        called_args <<- list(...)
        structure(list(
            open = function(...) NULL
        ), class = "remoteDriver")
    }
    
    with_mock(
        "RSelenium::remoteDriver" = mock_remote_driver,
        {
            get_remote_driver(
                chrome_options = list(chromeOptions = list(prefs = list())),
                port = 4445,
                save_path = "/test/path"
            )
            
            expect_equal(called_args$port, 4445)
            expect_equal(called_args$remoteServerAddr, "localhost")
            expect_equal(called_args$browserName, "chrome")
        }
    )
})

test_that("get_remote_driver passes chrome options correctly", {
    test_options <- list(
        chromeOptions = list(
            args = c('--headless', '--no-sandbox'),
            prefs = list(
                "download.prompt_for_download" = FALSE,
                "download.directory_upgrade" = TRUE
            )
        )
    )
    
    called_args <- NULL
    mock_remote_driver <- function(...) {
        called_args <<- list(...)
        structure(list(
            open = function(...) NULL
        ), class = "remoteDriver")
    }
    
    with_mock(
        "RSelenium::remoteDriver" = mock_remote_driver,
        {
            get_remote_driver(
                chrome_options = test_options,
                port = 4445,
                save_path = "/test/path"
            )
            
            expect_equal(
                called_args$extraCapabilities$chromeOptions$args,
                c('--headless', '--no-sandbox')
            )
            expect_true(
                !called_args$extraCapabilities$chromeOptions$prefs[["download.prompt_for_download"]]
            )
        }
    )
})