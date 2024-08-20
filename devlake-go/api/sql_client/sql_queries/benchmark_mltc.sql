with _pr_stats as (
    SELECT
        distinct pr.id,
                 ppm.pr_cycle_time
    FROM
        pull_requests pr
            join project_pr_metrics ppm on ppm.id = pr.id
            join project_mapping pm on pr.base_repo_id = pm.row_id and pm.`table` = 'repos'
		join cicd_deployment_commits cdc on ppm.deployment_commit_id = cdc.id
    WHERE
        pm.project_name in (:project)
      and pr.merged_date is not null
      and ppm.pr_cycle_time is not null
      AND cdc.finished_date BETWEEN FROM_UNIXTIME(:from)
      AND FROM_UNIXTIME(:to)
),

     _median_change_lead_time_ranks as(
         SELECT *, percent_rank() over(order by pr_cycle_time) as ranks
         FROM _pr_stats
     ),

     _median_change_lead_time as(
         SELECT max(pr_cycle_time) as median_change_lead_time
         FROM _median_change_lead_time_ranks
         WHERE ranks <= 0.5
     )

SELECT
    CASE
        WHEN ('2023') = '2023' THEN
            CASE
                WHEN median_change_lead_time < 24 * 60 THEN "elite"
                WHEN median_change_lead_time < 7 * 24 * 60 THEN "high"
                WHEN median_change_lead_time < 30 * 24 * 60 THEN "medium"
                WHEN median_change_lead_time >= 30 * 24 * 60 THEN "low"
                ELSE "N/A. Please check if you have collected deployments/pull_requests."
                END
        WHEN ('2023') = '2021' THEN
            CASE
                WHEN median_change_lead_time < 60 THEN "elite"
                WHEN median_change_lead_time < 7 * 24 * 60 THEN "high"
                WHEN median_change_lead_time < 180 * 24 * 60 THEN "medium"
                WHEN median_change_lead_time >= 180 * 24 * 60 THEN "low"
                ELSE "N/A. Please check if you have collected deployments/pull_requests."
                END
        ELSE 'Invalid dora report'
        END AS data_key,
    CASE
        WHEN ('2023') = '2023' THEN
            CASE
                WHEN median_change_lead_time < 24 * 60 THEN CONCAT(round(median_change_lead_time/60,1), " (elite)")
                WHEN median_change_lead_time < 7 * 24 * 60 THEN CONCAT(round(median_change_lead_time/60,1), " (high)")
                WHEN median_change_lead_time < 30 * 24 * 60 THEN CONCAT(round(median_change_lead_time/60,1), " (medium)")
                WHEN median_change_lead_time >= 30 * 24 * 60 THEN CONCAT(round(median_change_lead_time/60,1), " (low)")
                ELSE "N/A. Please check if you have collected deployments/pull_requests."
                END
        WHEN ('2023') = '2021' THEN
            CASE
                WHEN median_change_lead_time < 60 THEN CONCAT(round(median_change_lead_time/60,1), " (elite)")
                WHEN median_change_lead_time < 7 * 24 * 60 THEN CONCAT(round(median_change_lead_time/60,1), " (high)")
                WHEN median_change_lead_time < 180 * 24 * 60 THEN CONCAT(round(median_change_lead_time/60,1), " (medium)")
                WHEN median_change_lead_time >= 180 * 24 * 60 THEN CONCAT(round(median_change_lead_time/60,1), " (low)")
                ELSE "N/A. Please check if you have collected deployments/pull_requests."
                END
        ELSE 'Invalid dora report'
        END AS data_value
FROM _median_change_lead_time