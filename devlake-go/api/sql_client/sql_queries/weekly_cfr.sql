WITH RECURSIVE calendar_weeks AS (
    SELECT
        STR_TO_DATE(
            CONCAT(YEARWEEK(FROM_UNIXTIME(:from)), ' Sunday'),
            '%X%V %W'
        ) AS week_date
    UNION
    ALL
    SELECT
        DATE_ADD(week_date, INTERVAL 1 WEEK)
    FROM
        calendar_weeks
    WHERE
        week_date < FROM_UNIXTIME(:to)
),

_deployments AS (
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

_change_failure_rate_for_each_week as (
    SELECT
        YEARWEEK(deployment_finished_date) AS week,
        case
            WHEN count(deployment_id) IS NULL THEN NULL
            ELSE sum(has_incident)/count(deployment_id) * 100 END AS change_failure_rate
    FROM
        _failure_caused_by_deployments
    GROUP BY 1
)

    SELECT
        concat(date_format(cw.week_date,'%m/%d'), ' - ', date_format(DATE_ADD(cw.week_date, INTERVAL +6 DAY),'%m/%d')) as data_key,
		cfr.change_failure_rate AS data_value
    FROM
        calendar_weeks cw
        JOIN _change_failure_rate_for_each_week cfr ON YEARWEEK(cw.week_date) = cfr.week
    ORDER BY
        cw.week_date

