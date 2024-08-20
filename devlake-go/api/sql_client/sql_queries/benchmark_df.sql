with last_few_calendar_months as(
    SELECT CAST((SYSDATE()-INTERVAL (H+T+U) DAY) AS date) day
    FROM ( SELECT 0 H
           UNION ALL SELECT 100 UNION ALL SELECT 200 UNION ALL SELECT 300
         ) H CROSS JOIN ( SELECT 0 T
                          UNION ALL SELECT  10 UNION ALL SELECT  20 UNION ALL SELECT  30
                          UNION ALL SELECT  40 UNION ALL SELECT  50 UNION ALL SELECT  60
                          UNION ALL SELECT  70 UNION ALL SELECT  80 UNION ALL SELECT  90
    ) T CROSS JOIN ( SELECT 0 U
                     UNION ALL SELECT   1 UNION ALL SELECT   2 UNION ALL SELECT   3
                     UNION ALL SELECT   4 UNION ALL SELECT   5 UNION ALL SELECT   6
                     UNION ALL SELECT   7 UNION ALL SELECT   8 UNION ALL SELECT   9
    ) U
    WHERE
        (SYSDATE() - INTERVAL (H + T + U) DAY) > FROM_UNIXTIME(:from)
      AND (SYSDATE() - INTERVAL (H + T + U) DAY) < FROM_UNIXTIME(:to)
),

     _production_deployment_days as(
         SELECT
             cdc.cicd_deployment_id as deployment_id,
             max(DATE(cdc.finished_date)) as day
         FROM cicd_deployment_commits cdc
                  JOIN project_mapping pm on cdc.cicd_scope_id = pm.row_id and pm.`table` = 'cicd_scopes'
         WHERE
             pm.project_name in (:project)
           and cdc.result = 'SUCCESS'
           and cdc.environment = 'PRODUCTION'
         GROUP BY 1
     ),

     _days_weekly_deploy as(
         SELECT
             date(DATE_ADD(last_few_calendar_months.day, INTERVAL -WEEKDAY(last_few_calendar_months.day) DAY)) as week,
             MAX(if(_production_deployment_days.day is not null, 1, null)) as weeks_deployed,
             COUNT(distinct _production_deployment_days.day) as days_deployed
         FROM
             last_few_calendar_months
                 LEFT JOIN _production_deployment_days ON _production_deployment_days.day = last_few_calendar_months.day
         GROUP BY week
     ),

     _days_monthly_deploy as(
         SELECT
             date(DATE_ADD(last_few_calendar_months.day, INTERVAL -DAY(last_few_calendar_months.day)+1 DAY)) as month,
             MAX(if(_production_deployment_days.day is not null, 1, null)) as months_deployed,
             COUNT(distinct _production_deployment_days.day) as days_deployed
         FROM
             last_few_calendar_months
                 LEFT JOIN _production_deployment_days ON _production_deployment_days.day = last_few_calendar_months.day
         GROUP BY month
     ),

     _days_six_months_deploy AS (
         SELECT
             month,
             SUM(days_deployed) OVER (
                 ORDER BY month
                 ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
                 ) AS days_deployed_per_six_months,
             COUNT(months_deployed) OVER (
                 ORDER BY month
                 ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
                 ) AS months_deployed_count,
             ROW_NUMBER() OVER (
                 PARTITION BY DATE_FORMAT(month, '%Y-%m') DIV 6
      ORDER BY month DESC
                 ) AS rn
         FROM _days_monthly_deploy
     ),

     _median_number_of_deployment_days_per_week_ranks as(
         SELECT *, percent_rank() over(order by days_deployed) as ranks
         FROM _days_weekly_deploy
     ),

     _median_number_of_deployment_days_per_week as(
         SELECT max(days_deployed) as median_number_of_deployment_days_per_week
         FROM _median_number_of_deployment_days_per_week_ranks
         WHERE ranks <= 0.5
     ),

     _median_number_of_deployment_days_per_month_ranks as(
         SELECT *, percent_rank() over(order by days_deployed) as ranks
         FROM _days_monthly_deploy
     ),

     _median_number_of_deployment_days_per_month as(
         SELECT max(days_deployed) as median_number_of_deployment_days_per_month
         FROM _median_number_of_deployment_days_per_month_ranks
         WHERE ranks <= 0.5
     ),

     _days_per_six_months_deploy_by_filter AS (
         SELECT
             month,
             days_deployed_per_six_months,
             months_deployed_count
         FROM _days_six_months_deploy
         WHERE rn%6 = 1
     ),


     _median_number_of_deployment_days_per_six_months_ranks as(
         SELECT *, percent_rank() over(order by days_deployed_per_six_months) as ranks
         FROM _days_per_six_months_deploy_by_filter
     ),

     _median_number_of_deployment_days_per_six_months as(
         SELECT min(days_deployed_per_six_months) as median_number_of_deployment_days_per_six_months, min(months_deployed_count) as is_collected
         FROM _median_number_of_deployment_days_per_six_months_ranks
         WHERE ranks >= 0.5
     )

SELECT
    CASE
        WHEN ('2023') = '2023' THEN
            CASE
                WHEN median_number_of_deployment_days_per_week >= 7 THEN 'elite'
                WHEN median_number_of_deployment_days_per_week >= 1 THEN 'high'
                WHEN median_number_of_deployment_days_per_month >= 1 THEN 'medium'
                WHEN median_number_of_deployment_days_per_month < 1 and is_collected is not null THEN 'low'
                ELSE "N/A. Please check if you have collected deployments." END
        WHEN ('2023') = '2021' THEN
            CASE
                WHEN median_number_of_deployment_days_per_week >= 7 THEN 'elite'
                WHEN median_number_of_deployment_days_per_week >= 1 THEN 'high'
                WHEN median_number_of_deployment_days_per_six_months >= 1 THEN 'medium'
                WHEN median_number_of_deployment_days_per_six_months < 1 and is_collected is not null THEN 'low'
                ELSE "N/A. Please check if you have collected deployments." END
        ELSE 'Invalid dora report'
        END AS data_key,
    CASE
        WHEN ('2023') = '2023' THEN
            CASE
                WHEN median_number_of_deployment_days_per_week >= 7 THEN CONCAT(median_number_of_deployment_days_per_week, ' deployment days per week (elite)')
                WHEN median_number_of_deployment_days_per_week >= 1 THEN CONCAT(median_number_of_deployment_days_per_week, ' deployment days per week (high)')
                WHEN median_number_of_deployment_days_per_month >= 1 THEN CONCAT(median_number_of_deployment_days_per_month, ' deployment days per month (medium)')
                WHEN median_number_of_deployment_days_per_month < 1 and is_collected is not null THEN CONCAT(median_number_of_deployment_days_per_month, ' deployment days per month (low)')
                ELSE "N/A. Please check if you have collected deployments." END
        WHEN ('2023') = '2021' THEN
            CASE
                WHEN median_number_of_deployment_days_per_week >= 7 THEN CONCAT(median_number_of_deployment_days_per_week, ' deployment days per week (elite)')
                WHEN median_number_of_deployment_days_per_month >= 1 THEN CONCAT(median_number_of_deployment_days_per_month, ' deployment days per month (high)')
                WHEN median_number_of_deployment_days_per_six_months >= 1 THEN CONCAT(median_number_of_deployment_days_per_six_months, ' deployment days per six months (medium)')
                WHEN median_number_of_deployment_days_per_six_months < 1 and is_collected is not null THEN CONCAT(median_number_of_deployment_days_per_six_months, ' deployment days per six months (low)')
                ELSE "N/A. Please check if you have collected deployments." END
        ELSE 'Invalid dora report'
        END AS data_value
FROM _median_number_of_deployment_days_per_week, _median_number_of_deployment_days_per_month, _median_number_of_deployment_days_per_six_months
