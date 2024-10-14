with _deployments AS (
    SELECT
        cdc.cicd_deployment_id AS deployment_id,
        max(cdc.finished_date) AS deployment_finished_date
    FROM
        cicd_deployment_commits cdc
        JOIN project_mapping pm ON cdc.cicd_scope_id = pm.row_id AND pm.`table` = 'cicd_scopes'
    WHERE
        (
            LOWER(pm.project_name) REGEXP LOWER(:project)
        )
        AND cdc.result = 'SUCCESS'
        AND cdc.environment = 'PRODUCTION'
    GROUP BY 1
),

     _failure_caused_by_deployments AS (
         SELECT
             d.deployment_id,
             d.deployment_finished_date,
             count(
                     distinct case
                                  when i.id is not null then d.deployment_id
                                  else null
                 end
             ) as has_incident
         FROM
             _deployments d
                 left join project_incident_deployment_relationships pim on d.deployment_id = pim.deployment_id
                 left join incidents i on pim.id = i.id
         GROUP BY
             1,
             2
     ),

_change_failure_rate_for_each_month as (
    SELECT
        date_format(deployment_finished_date,'%y/%m') as month,
        case
            when count(deployment_id) is null then null
            else sum(has_incident)/count(deployment_id) end as change_failure_rate
    FROM
        _failure_caused_by_deployments
    GROUP BY 1
)

SELECT
    cm.month as data_key,
    cfr.change_failure_rate as data_value
FROM
    calendar_months cm
    JOIN _change_failure_rate_for_each_month cfr on cm.month = cfr.month
    WHERE cm.month_timestamp BETWEEN FROM_UNIXTIME(:from)
        AND FROM_UNIXTIME(:to)