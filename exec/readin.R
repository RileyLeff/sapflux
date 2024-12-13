source("exec/readin_util.R")
source("exec/fix_time.R")
source("exec/calculate.R")
source("exec/deployments.R")
source("exec/sensors.R")

# readin data
list_of_dfs <- unique(
    lapply(
        # for each file path that contains data in "data/raw" (decided by "get_valid_file_paths")
        X = get_valid_file_paths("data/raw", constants$forbidden_words_in_datafile_paths), 
        # we pass it to the function "readin_implexx"
        FUN = \(x){
            readin_implexx(
                path = x,
                column_names = constants$implexx_colnames,
                possible_timestamp_formats = constants$possible_timestamp_formats,
                cols_to_make_numeric = constants$cols_to_make_numeric
            )
        }
    )
)

# cleanup dfs from individual files
list_of_dfs <- lapply(
    list_of_dfs,
    \(df){
        df <- fix_id_column(df, "id")
        df <- make_missings_NA(df, constants$cols_to_make_numeric)
        df <- remove_rows_with_invalid_sdi_addresses(df, "sdi")
        df <- remove_timepoints_outside_of_collection_window(
            df = df, 
            timecol = "ts", 
            earliest = constants$early_bound_timestamp, 
            latest = Sys.time()
        )
        if(nrow(df) == 0){
            return(NULL)
        }
        return(df)
    }
)

# remove dfs that don't contribute any rows
list_of_dfs <- list_of_dfs[!sapply(list_of_dfs, is.null)]

# add unique identifiers to the list names 
names(list_of_dfs) <- 1:length(list_of_dfs)

# attach that identifier to each row in each dataframe
list_of_dfs <- lapply(
    names(list_of_dfs),
    \(df_id){
        list_of_dfs[[df_id]]$origin <- df_id
        return(list_of_dfs[[df_id]])
    }
)

# grab unique logger IDs
logger_ids <- unique(
    unlist(
        sapply(
            X = list_of_dfs,
            FUN = \(x) unique(x$id)
        )
    )
)

# throw an error if we encountered a NAN or #1 (default) logger ID
# nothing will happen if those criteria are not met
if(any(is.nan(logger_ids)) | any(logger_ids == 1)) {
    stop("Invalid Logger ID Found, Check It Out Big Dog.")
}

#  breaks out dataframes by ID. 
# "id_list" is a list where each entry corresponds to a logger ID
#  and each entry itself contains a list of dataframes associated with that logger ID.
# this is probably not the most elegant solution to create that list but it works. 
id_list <- lapply(
    X = logger_ids,
    FUN = \(this_id) {
        # identify dfs that match the current logger ID (this_id)
        matches <- lapply(
            list_of_dfs,
            \(this_df) {
                if(this_df$id[1] == this_id){
                    return(this_df)
                }
            }
        )
        # eliminate the NULLs left behind from non-matching dataframes 
        matches <- matches[!sapply(matches, is.null)]
    }
)

# eliminate valid ids that don't have any actionable data
# these should already be cut from the nrow checker above but this is a cheap operation
# so doesn't hurt to double check
id_list[which(sapply(id_list, length) == 0)] <- NULL

# set the names of the entries in ID list to actually match the logger ID names
names(id_list) <- sapply(id_list, \(x) return(x[[1]]$id[1]))

# merge the dataframes stored under each ID in id_list
# result is a list of logger IDs, each associated with a single big-ass dataframe
id_list <- combine_dfs_by_logger(id_list)

# we call each subset of the cumulative data collected at a given logger ID with a unique set of origins a "chunk"
# each chunk is, barring catastrophe, recorded as if it was in a single timezone, e.g. only EST or only EDT.
# we assume that the time zone was correct against our lookup table at the earliest record in the chunk.
# -- e.g. this assumption breaks only if clocks don't get synced at the first download after a DST switch.

# with merged origins
id_list <- lapply(
    id_list,
    \(df) {
        # get full set of origins for each row
        df <- merge_rows_by_origin(df, origin_col = "origin", merge_by_cols = c("ts", "batt_v"))
    }
)

# split each sublist in id_list into chunks
list_of_chunksets <- lapply(
    id_list,
    \(df) {
        return(split(df, df[,"origin"]))
    }
)

# reads in and cleans up a file that describes EST -> EDT transitions until 2030
tzlut <- get_timezone_lut()

list_of_chunksets <- lapply(
    list_of_chunksets,
    \(chunkset){
        return(
            lapply(
                chunkset,
                initial_fix_chunk_tz,
                tzlut
            )
        )
    }
)

list_of_chunkset_timebounds <- lapply(
    list_of_chunksets,
    \(chunkset){
        return(
            lapply(
                chunkset,
                \(chunk){
                    get_start_and_end_of_chunk(chunk)
                }
            )
        )
    }
)

list_of_dataframes <- lapply(
    list_of_chunksets,
    \(chunkset){
        return(
            do.call(rbind, chunkset)
        )
    }
)

df <- do.call(rbind, list_of_dataframes)

df <- df[, which(colnames(df) != "origin")]

indf <- df[,which(!grepl("out", colnames(df)))]
outdf <- df[,which(!grepl("in", colnames(df)))]
fixed_col_names <- c("alpha", "beta", "tmax_t")
colnames(indf)[(length(indf)-2):length(indf)] <- fixed_col_names
colnames(outdf)[(length(outdf)-2):length(outdf)] <- fixed_col_names
indf$depth_mm <- 20
outdf$depth_mm <- 10
df <- rbind(indf, outdf)
df <- df[order(df$ts),]
df <- df[which(df$tmax_t < 200),]
df <- df[which(df$alpha < 40),]

library(ggplot2)
library(dplyr)


vois <- c(301, 302, 303, 304, 305, 306, 601)
cutdate <- as.POSIXct("2022-12-10 12:00:00", tz = "UTC")
thisyear <- as.POSIXct("2024-01-01 12:00:00", tz = "UTC")
highcut <- as.POSIXct("2024-06-06 12:00:00", tz = "UTC")
lowcut <- as.POSIXct("2022-09-01 12:00:00", tz = "UTC")
value <- 701
ggplot(
    data = df[which(df$id == value),]
    ) +
    aes(x = ts, y = tmax_t, color = interaction(depth_mm, sdi)) +
    geom_point() +
    ggtitle(value)  +
    facet_wrap(vars(id)) + 
    geom_vline(xintercept = cutdate, 
               linetype = "dashed",
               color = "red")

df$deployment_id <- NA
df$sensor_type <- NA
df$site <- NA
df$zone <- NA
df$plot <- NA
df$tree_id <- NA
df$spp <- NA

df$j_cmhr <- NA

deployments <- get_deployments()
sensors <- get_sensors()

for(this_row in 1:nrow(df)){
    print(this_row)
    matches <- which(
        (deployments$sdi == df$sdi[this_row]) & 
        (deployments$logger_id == df$id[this_row]) &
        (deployments$start_ts_utc < df$ts[this_row])
    )
    valid_rows <- matches[deployments$start_ts_utc[matches] < df$ts[this_row]]
    match <- matches[which.max(deployments[matches,"start_ts_utc"])]

    df$deployment_id[this_row] <- match
    df$sensor_type[this_row] <- deployments$sensor_type[match]
    df$site[this_row] <- deployments$site[match]
    df$zone[this_row] <- deployments$zone[match]
    df$plot[this_row] <- deployments$plot[match]
    df$tree_id[this_row] <- deployments$tree_id[match]
    df$spp[this_row] <- deployments$spp[match]

    df$xd[this_row] <- sensors$downstream_probe_distance_cm[
        which(sensors$sensor_id == df$sensor_type[this_row])
    ]

    df$xu[this_row] <- sensors$upstream_probe_distance_cm[
        which(sensors$sensor_id == df$sensor_type[this_row])
    ]
}

# First join df with deployments on matching criteria
library(dplyr)

result <- df %>%
  # Create a cross join with deployments
  left_join(
    deployments,
    by = c("sdi", "id" = "logger_id"),
    relationship = "many-to-many"
  ) %>%
  # Keep only valid deployment times
  filter(start_ts_utc < ts) %>%
  # Group by original rows and keep most recent deployment
  group_by(
    # Include all original columns from df that uniquely identify rows
    ts, sdi, id  # add other identifying columns as needed
  ) %>%
  slice_max(start_ts_utc, n = 1) %>%
  ungroup() %>%
  # Join with sensors table
  left_join(
    select(sensors, 
           sensor_id,
           downstream_probe_distance_cm,
           upstream_probe_distance_cm),
    by = c("sensor_type" = "sensor_id")
  ) %>%
  # Rename distance columns if needed
  rename(
    xd = downstream_probe_distance_cm,
    xu = upstream_probe_distance_cm
  )

df <- readRDS("data/df")



# First match deployments
matches <- merge(df, 
                deployments, 
                by.x = c("sdi", "id"),
                by.y = c("sdi", "logger_id"))

# Filter for valid times
matches <- matches[matches$start_ts_utc < matches$ts,]

# Get most recent deployment for each measurement
matches <- matches[order(matches$start_ts_utc, decreasing = TRUE),]
matches <- matches[!duplicated(matches[c("ts", "rn", "id", "sdi", "depth_mm", "alpha", "beta", "tmax_t")]),]

# Add sensor info
matches <- merge(matches,
                sensors,
                by.x = "sensor_type",
                by.y = "sensor_id",
                all.x = TRUE)

# Reorder columns if needed
df <- matches[, c("ts", "rn", "batt_v", "id", "sdi", "alpha", "beta", "tmax_t", 
                  "depth_mm", "deployment_id", "sensor_type", "site", "zone", 
                  "plot", "tree_id", "spp", "j_cmhr", "xd", "xu")]

matches$j_cmhr <- sap_calculate(
    matches,
    constants$parameters$k$value,
    constants$parameters$hpd$value,
    constants$parameters$t$value,
    constants$parameters$woundcorr$value,
    constants$parameters$sph$value,
    constants$parameters$pd$value,
    constants$parameters$cd$value,
    constants$parameters$mc$value,
    constants$parameters$cw$value,
    constants$parameters$pw$value
)

colnames(matches)[4] <- "ts_utc"
library(ggplot2)
ggplot(matches %>% filter(site == "brnv")) +
    aes(x = ts_utc, y = j_cmhr, color = interaction(tree_id, depth_mm)) +
    geom_point() +
    facet_wrap(vars(interaction(zone, plot)))


ggplot(data = df %>% filter(id == 401)) +
aes(x = ts, y = tmax_t, color = interaction(sdi, depth_mm)) +
geom_point()


write.csv(matches, "still_bad_sapflux.csv")
