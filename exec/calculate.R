#' calculate sapflux rates
#'
#' @param data fix later
#' @param parameters fix later
#' @return A data frame.
#' @examples
#' sap_calculate(parameters)
#' @export
sap_calculate <- function(
    df, 
    k,
    to,
    t,
    b,
    sph,
    pd,
    cd,
    mc,
    cw,
    pw,
    switch_thresh = 1
) {

  vc_hrm <- get_vc_hrm(df$alpha, k, df$downstream_probe_distance_cm, df$upstream_probe_distance_cm, to, t, b, sph)
  vc_tmax <- get_vc_tmax(df$tmax_t, k, df$downstream_probe_distance_cm, to, b, sph)
  dma <- vc_hrm
  #indices_to_switch <- which(df$beta >= switch_thresh)
  #dma[indices_to_switch] <- vc_tmax[indices_to_switch]

  j <- get_j(dma, pd, cd, mc, cw, pw)

  return(j)
}


get_vc_hrm <- function(alpha, k, xd, xu, to, t, b, sph) {
  vc <- (((2 * k) / (xd + xu)) * alpha + ((xd - xu) / (2 * (t - (to / 2))))) * b * sph
  return(vc)
}

get_vc_tmax <- function(tm, k, xd, to, b, sph) {
  vc <- ((sqrt(((4 * k) / to) * (log(1 - (to / tm))) + ((xd^2) / (tm * (tm - to))))) * sph) * b
  return(vc)
}

get_j <- function(data, pd, cd, mc, cw, pw) {
  j <- (data * pd * (cd + (mc * cw))) / (pw * cw)
  return(j)
}