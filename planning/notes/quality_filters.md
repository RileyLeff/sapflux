The final output should include a categorical column called quality and another called something like quality_explanation. 

For good rows, these should both be missing data.

For rows that meet certain criteria, we should flag them as quality = SUSPECT and provide an explanation. Here are the cases to consider. The parameters and thresholds for these cases should be stored in the same place and way that the other calculation parameters are stored, supporting hierarchical overrides, and so on.

Minimum time: if a timestamp is earlier than the start date and time of its deployment, mark it as suspect.
Maximum time: if a timestamp is later than the end of its deployment OR at a timestamp that hasn't occurred yet for an active deployment, mark it as suspect.
Time travel: if >2 years pass between adjacent rows (adjacency defined by record number), mark it as suspect.
Maximum flux: If sapflow density exceeds 40 cm/hr, mark it as suspect. 
Minimum flux: if sapflow density is less than -15 cm/hr, mark it as suspect.
