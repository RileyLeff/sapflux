source("exec/load_constants.R")

#' Get paths to only the files that contain actionable data.
#' 
#' @param path A character (string) that represents a valid path. This function doesn't perform any of its own validation on the input path, make sure that it's valid and is a folder.
#' @param forbidden_words A list of characters (strings) where, if they appear in a file path, we know we don't want to read in that file.
#' @return A vector of acceptable file paths.
#' @examples
#' # e.g. if the folder "data/raw" contains file1.csv, file2.csv, and file3yowza.csv
#' get_valid_file_paths("data/raw", forbidden_words = c("yowza")) # returns something like c("data/raw/file1.csv", "data/raw/file2.csv")
get_valid_file_paths <- function(
    path, 
    forbidden_words
) {
    
    output <- sapply(
        # gets all files from the folder located at path
        X = list.files(
            path, 
            full.names = TRUE, 
            recursive = TRUE
        ),
        # checks if each path contains any forbidden words, allows "clean" ones to pass through.
        FUN = \(x) {
            if(!any(sapply(forbidden_words, grepl, x))) {
                x
            }
        }
    )

    # cleanup
    output <- unlist(output)
    names(output) <- NULL
    return(output)
}

#' SDI-12 Address Validation
#' 
#' `is_valid_sdi_address` returns whether the input string is a valid SDI-12 address.
#' 
#' This function can handle vectors as inputs.
#' 
#' Addresses may be a single lowercase character, a single uppercase character, or a digit between 0 and 9.
#' 
#' @param x A character (string).
#' @return A logical (boolean) value.
#' @examples
#' is_valid_sdi_address("a") # returns TRUE
#' is_valid_sdi_address("A") # returns TRUE
#' is_valid_sdi_address("9") # returns TRUE
#' is_valid_sdi_address(9) # returns TRUE
#' is_valid_sdi_address(10) # returns FALSE
#' is_valid_sdi_address(c("a", "B", "2", "200")) # returns TRUE, TRUE, TRUE, FALSE
is_valid_sdi_address <- function(x){
    valids <- c(letters, LETTERS, as.character(0:9))
    return(x %in% valids)
}

#' Remove Dataframe Rows With Invalid SDI-12 Addresses
#' 
#' `remove_rows_with_invalid_sdi_addresses` returns a dataframe without the rows that contain invalid sdi-addresses.
#' 
#' @param df A dataframe.
#' @param sdi_col An identifier for the column that contains the sdi address, e.g. 5 (by position) or "sdi" (by name)
#' @return A logical (boolean) value.
#' @examples
#' remove_rows_with_invalid_sdi_addresses(my_df, 1) # sdi address is in first column
#' remove_rows_with_invalid_sdi_addresses(my_df, "my_sdi_column")
remove_rows_with_invalid_sdi_addresses <- function(df, sdi_col) {
    invalid_rows <- which(!is_valid_sdi_address(df[,sdi_col]))
    if(length(invalid_rows > 0)){
        return(df[-invalid_rows,])
    } else {
        return(df)
    }
}

#' Fix common problems with the ID column of implexx sap flux data
#' 
#' `fix_id_column` returns a dataframe with a potentially mutated ID column.
#' 
#' @param df A dataframe.
#' @param sdi_col An identifier for the column that contains the sdi address, e.g. 5 (by position) or "sdi" (by name)
#' @return A dataframe.
#' @examples
#' fix_id_column(my_df, "id", "data/raw/cr200_599_cooldata.csv")
fix_id_column <- function(df, id_col) {
        ids_here <- unique(df$id)
    if(length(ids_here) > 1) {
        finite_members <- which(is.finite(ids_here))
        if(length(finite_members) > 1) {
            stop("More than one finite (e.g. a number) logger ID found within a single file")    
        } else if(length(finite_members == 1)){
            df$id <- ids_here[finite_members[1]]
        } else {
            stop("No logger ID found in file:")
        }
    }
    return(df)
}

#' Turn Implexx/Campbell Missing Data Into NAs
#' 
#' `make_missings_NA` returns a dataframe with potentially mutated missing data.
#' 
#' @param df A dataframe.
#' @param cols An identifier for the columns to operate on, e.g. 5 (by position) or "sdi" (by name)
#' @param missingvals A vector of values that indicate missing data, a default is provided for campbell/implexx.
#' @return A dataframe.
#' @examples
#' make_missings_NA(my_df, c(4,5,6))
make_missings_NA <- function(df, cols, missingvals = c(-99, NaN)) {
    for(this_col in cols) {
        replace_these <- which(df[,this_col] %in% missingvals)
        if(length(replace_these) > 0) {
            df[replace_these,this_col] <- NA
        }
    }
    return(df)
}

#' Limit data to a known timeframe to remove points with known clock errors.
#' 
#' `remove_timepoints_outside_of_collection_window` returns a dataframe with a potentially reduced set of rows.
#' 
#' @param df A dataframe.
#' @param timecol An identifier for the column representing time, e.g. 1 (by position) or "ts" (by name). We assume POSIXct format.
#' @param earliest Earliest timestamp allowed in the collection, inclusive. Use POSIXct and mind the timezone.
#' @param latest Latest timestamp allowed in the collection, inclusive. Use POSIXct and mind the timezone.
#' @return A dataframe.
remove_timepoints_outside_of_collection_window <- function(df, timecol, earliest, latest){
    inds_to_keep <- which(
        (df[,timecol] >= earliest) &
        (df[,timecol] <= latest)
    )
    return(df[inds_to_keep,])
}

#' Merges dataframes that originate from the same logger into a single dataframe.  
#' 
#' @param dfs_by_logger A list of dataframes where each entry represents a datalogger. Each entry itself contains a list of dataframes associated with that datalogger.
#' @return A dataframe read from the file located at path.
#' @examples
#' readin_implexx("data/raw/2024_04_17/CR200Series_601_Table1.dat") # returns a dataframe
combine_dfs_by_logger <- function(dfs_by_logger){
    lapply(
        X = dfs_by_logger,
        FUN = \(set_of_dfs) {
            temp <- unique(do.call(rbind, set_of_dfs))
            return(temp[order(temp$ts),])
        }
    )
}

#' Fix one-off problems with files in the input data.
#' 
#' @param df A dataframe.
#' @param path A character (string) that corresponds to the origin of the dataframe.
#' @return A dataframe with potential mutations due to one-off fixes.
#' @examples
#' fix_special_cases(my_df, "data/raw/cr200_wow_this_file_is_messed_up.csv")
fix_special_cases <- function(df, path){
    if(grepl("601", path) & any(is.nan(df$id))){
        df$id <- 601
    }
    
    # special case for logger 501, which thinks it is logger #1
    if(grepl("501", path) & any(df$id == 1)){
        df$id <- 501
    }
    return(df)
}

#' Reads in data files specifically for implexx-formatted datasets. 
#' 
#' @param path A character (string) that represents a valid path. This function doesn't perform any validation on the input path, make sure that it's valid and points to implexx sap flux data.
#' @return A dataframe read from the file located at path.
#' @examples
#' readin_implexx("data/raw/2024_04_17/CR200Series_601_Table1.dat") # returns a dataframe
readin_implexx <- function(
    path, 
    column_names, 
    possible_timestamp_formats, 
    cols_to_make_numeric
) {

    x <- read.csv(
        path, 
        skip = 4,
        header = FALSE
    )

    colnames(x) <- column_names

    # remove columns that I have deemed unworthy
    x[,c(which(grepl("wack", colnames(x))))] <- NULL

    x$ts <- as.POSIXct(x$ts, tryFormats = possible_timestamp_formats, tz = "UTC")
    x$rn <- as.numeric(x$rn)
    x$sdi <- as.character(x$sdi)
    x$id <- as.numeric(x$id)

    x[cols_to_make_numeric] <- lapply(x[cols_to_make_numeric], as.numeric)

    x <- fix_special_cases(x, path)

    return(x)
}