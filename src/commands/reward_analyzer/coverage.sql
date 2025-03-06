WITH valid_radios AS (
    SELECT
        hotspot_key,
        count(*)
    FROM
        wifi_heartbeats
    WHERE
        truncated_timestamp >= $1
        AND truncated_timestamp < $2
    GROUP BY
        hotspot_key
    HAVING
        count(*) >= 12
),
latest_uuids AS (
    SELECT DISTINCT ON (hotspot_key)
        hotspot_key,
        coverage_object
    FROM
        wifi_heartbeats wh
    WHERE
        truncated_timestamp >= $1
        AND truncated_timestamp < $2
    ORDER BY
        hotspot_key,
        truncated_timestamp DESC
),
seniorities AS (
    SELECT DISTINCT ON (radio_key)
        radio_key,
        seniority_ts
    FROM
        seniority
    WHERE
        inserted_at <= $2
    ORDER BY
        radio_key,
        inserted_at DESC
)
SELECT
    vr.hotspot_key,
    sn.seniority_ts,
    hc.hex,
    co.indoor,
    hc.signal_level,
    hc.signal_power,
    hc.footfall,
    hc.landtype,
    hc.urbanized,
    hc.service_provider_override
FROM
    valid_radios vr
    INNER JOIN latest_uuids u ON vr.hotspot_key = u.hotspot_key
    INNER JOIN hexes hc ON hc.uuid = u.coverage_object
    INNER JOIN seniorities sn ON sn.radio_key = vr.hotspot_key
    INNER JOIN coverage_objects co ON hc.uuid = co.uuid
