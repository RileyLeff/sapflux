try_with <- function(xpr, value, threshold){
    if(value > threshold){
        return(eval(substitute(xpr)))
    }
}
