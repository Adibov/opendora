with last_few_calendar_months as(
    SELECT CAST((FROM_UNIXTIME(:to)-INTERVAL (H+T+U) DAY) AS date) day
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
        (FROM_UNIXTIME(:to)-INTERVAL (H+T+U) DAY) > FROM_UNIXTIME(:from)
),

     _production_deployment_days as(
         SELECT
             cdc.cicd_deployment_id as deployment_id,
             max(DATE(cdc.finished_date)) as day
         FROM cicd_deployment_commits cdc
                  JOIN project_mapping pm on cdc.cicd_scope_id = pm.row_id and pm.`table` = 'cicd_scopes'
         WHERE
             pm.project_name REGEXP (:project)
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

     calendar_weeks as(
         select
             distinct date(DATE_ADD(date(day), INTERVAL -WEEKDAY(date(day)) DAY)) as start_of_week
         FROM last_few_calendar_months
         ORDER BY 1 asc
     )


SELECT
    concat(date_format(cw.start_of_week,'%m/%d'), ' - ', date_format(DATE_ADD(cw.start_of_week, INTERVAL +6 DAY),'%m/%d')) as data_key,
    days_deployed as data_value
from calendar_weeks cw left join _days_weekly_deploy b on cw.start_of_week = b.week