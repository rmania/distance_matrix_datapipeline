\out import_df_nodes_ams.log

truncate table osm.df_nodes_ams;
\copy osm.df_nodes_ams (highway, osmid, ref, x, y, geometry) from 'df_nodes_ams.csv' WITH CSV HEADER DELIMITER AS ',' quote as '"' escape as '\';
