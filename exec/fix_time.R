get_timezone_lut <- function(path = "data/stupid_tz_usaonly_formatted.csv"){
    timezone_transitions <- read.csv(path)
    timezone_transitions$ts <- as.POSIXct(timezone_transitions$ts, format = "%Y-%m-%d %H:%M:%S")
    timezone_transitions <- timezone_transitions[order(timezone_transitions$ts),]
    row.names(timezone_transitions) <- NULL
    return(timezone_transitions)
}

# determines whether a timestamp was recorded in EST or EDT. This only works in NYC time zone.
get_timezone_for_a_ts <- function(ts, lut){
    if(lut$action[max(which(ts >= lut$ts))] == "start"){
        return("EDT")
    } else {
        return("EST")
    }
}

initial_fix_chunk_tz <- function(chunk, lut, tscol = "ts"){
    
    first_entry <- min(chunk[,"ts"], na.rm = TRUE) # figure out timezone of first entry
    this_chunk_tz <- get_timezone_for_a_ts(first_entry, lut)

    # we read these in as if they were UTC initially. they aren't.
    # we have to use force_tz instead of with_tz because we don't want to change
    # the existing representation of the date.
    chunk[,tscol] <- lubridate::force_tz(chunk[,tscol], this_chunk_tz)

    # now we can use with_tz to put it into UTC where it belongs
    # the engineers that did the clocks at campbell scientific should be arrested
    chunk[,tscol] <- lubridate::with_tz(chunk[,tscol], "UTC")
    return(chunk)
}

merge_rows_by_origin <- function(df, origin_col = "origin", merge_by_cols = c("ts", "rn")) {
    as.data.frame(
        dplyr::distinct(
            dplyr::ungroup(
                dplyr::mutate(
                    dplyr::group_by(df, dplyr::across(dplyr::all_of(merge_by_cols))),
                    origin = paste(sort(unique(!!rlang::sym(origin_col))), collapse = "_")
                )
            )   
        )
    )
}

get_start_and_end_of_chunk <- function(df){
    return(
        list(
            start = min(df$ts), 
            end = max(df$ts)
        )
    )
}