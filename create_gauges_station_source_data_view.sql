CREATE VIEW drf_gauge_station_source_data AS
    SELECT d.obs_id AS obs_id,
    s.source_id AS source_id, 
    g.station_id AS station_id,
    g.station_name AS station_name,
    d.timemark AS timemark,
    d.time AS time,
    d.water_level AS water_level,
    s.units AS units,
    g.tz AS tz,
    g.gauge_owner AS gauge_owner,
    s.data_source AS data_source,
    s.source_name AS source_name,
    s.source_archive AS source_archive,
    g.location_name AS location_name,
    g.country AS country,
    g.state AS state,
    g.county AS county,
    g.geom AS geom 
    FROM drf_gauge_data d 
    INNER JOIN drf_gauge_source s ON s.source_id=d.source_id
    INNER JOIN drf_gauge_station g ON s.station_id=g.station_id;
