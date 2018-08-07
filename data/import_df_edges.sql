\out import_df_edges_ams.log

truncate table osm.df_edges_ams;
\copy osm.df_edges_ams (area, bridge, geometry, fclass, junction, key, lanes, "length", "level", maxspeed, maxweight, name, oneway, osmid, "ref", service, tunnel) from 'df_edges_ams.csv' WITH CSV HEADER DELIMITER AS ',' quote as '"' escape as '\';
