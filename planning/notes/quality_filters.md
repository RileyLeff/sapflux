Quality filtering is the final processing stage. It annotates each row with:

* `quality`: one of `GOOD` (implicit null) or `SUSPECT`.
* `quality_explanation`: a pipe-delimited list of rule identifiers explaining why a row was flagged.

All thresholds are managed through the same parameter cascade used by the calculator. The canonical parameter codes are:

* `quality_max_flux_cm_hr`
* `quality_min_flux_cm_hr`
* `quality_gap_years`
* `quality_deployment_start_grace_minutes`
* `quality_deployment_end_grace_minutes`
* `quality_future_lead_minutes`

Rules:

1. **Deployment window** – Reject timestamps that fall outside the deployment window once the configured grace minutes elapse. This rule expresses as:
   * timestamp < deployment.start - `quality_deployment_start_grace_minutes`
   * timestamp > deployment.end + `quality_deployment_end_grace_minutes`
2. **Future data** – Flag rows whose timestamps occur more than `quality_future_lead_minutes` ahead of the ingestion clock.
3. **Time travel** – After sorting by `record`, compute the elapsed time between adjacent rows. If the gap exceeds `quality_gap_years`, mark the later row suspect.
4. **Physiological bounds** – Flag rows where `sap_flux_density_j_dma_cm_hr` exceeds `quality_max_flux_cm_hr` or is below `quality_min_flux_cm_hr`.

The filter produces deterministic explanations such as `timestamp_before_deployment`, `timestamp_future`, `record_gap_gt_quality_gap_years`, and `sap_flux_density_above_quality_max_flux_cm_hr`. Multiple triggers are joined with `|` so downstream tooling can split the values reliably.
