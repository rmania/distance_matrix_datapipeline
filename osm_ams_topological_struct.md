**create a df_edges_ams table**

```
drop table if exists osm.weg_gestruct_merged_final_incl_vot;
CREATE TABLE osm.weg_gestruct_merged_final_incl_vot (
    id int8,
	cat int4,
    osm_id int8 NULL,
	fclass text NULL,
	"name" text NULL,
	"ref" text NULL,
	oneway text NULL,
	maxspeed text NULL,
	layer text NULL,
	bridge text NULL,
	tunnel text NULL,
	linelength numeric null,
    CONSTRAINT stag_osm_topology_pk2 PRIMARY KEY (id)
)
WITH (OIDS=FALSE);
--
SELECT AddGeometryColumn('osm','weg_gestruct_merged_final_incl_vot', 'geom', 28992, 'GEOMETRY', 2);
--SELECT AddGeometryColumn('osm','weg_gestruct_merged_final_incl_vot', 'linelength', 28992, 'GEOMETRY', 2);
------------------

-- create sequence
drop sequence if exists seq_stag_osm_fid;
create sequence seq_stag_osm_fid start 1;	
-- insert 1 from weg_gestruct_merged_final LAYER 0
insert into osm.weg_gestruct_merged_final_incl_vot (
	select
    	nextval('seq_stag_osm_fid'),
		cat,
		cast(osm_id as double precision),
		fclass,
		"name", --as streetname,
		"ref",
		oneway,
		maxspeed::text,
		layer,
		bridge,
		tunnel,
		st_length(geom) as linelength,
     	ST_setSRID(geom, 28992) as geom
from
	osm.weg_gestruct_merged_final where layer = '0');

create index weg_final_incl_vot_GIDX on osm.weg_gestruct_merged_final_incl_vot using GIST(geom);
vacuum analyse osm.weg_gestruct_merged_final_incl_vot;

create index stag_vot_clusters_GIDX on osm.stag_vot_clusters_grass using GIST(cl_geom);
vacuum analyse osm.stag_vot_clusters_grass;

-- add poi1 --> clustered VOT/LPG, connect to street network
insert into osm.weg_gestruct_merged_final_incl_vot
(select distinct on (loc_id)
    nextval('seq_stag_osm_fid'),
	cat,
    osm_id,
	'vot_link' fclass, --clustered vot
	"name",
	"ref",
	oneway,
	maxspeed,
	layer,
	bridge,
	tunnel,
	linelength,
	geomline as geom
--
from (
	SELECT 
		loc.cluster_toewijzing as loc_id,  
		cat,
		osm_id::int8,
      	fclass,
    	"name",
    	"ref",
		oneway,
		maxspeed,
		layer,
		bridge,
		tunnel,
		st_length(ST_ShortestLine(loc.cl_geom, edg.geom)) linelength,
	    ST_ShortestLine(loc.cl_geom, edg.geom) geomline
	FROM osm.stag_vot_clusters_grass loc
	join 
		osm.weg_gestruct_merged_final edg on ST_Dwithin(loc.cl_geom, edg.geom, 100)
	where layer= '0' ) prj
order by loc_id, linelength asc);	

-------------------------------

-- a regular grid of polygons/squares of given size
CREATE OR REPLACE FUNCTION ST_CreateFishnet(
        nrow integer, 
        ncol integer,
        xsize float8, 
        ysize float8,
        x0 float8 DEFAULT 0, 
        y0 float8 DEFAULT 0,
        OUT "row" integer, OUT col integer,
        OUT geom geometry)
    RETURNS SETOF record AS
$$
SELECT i + 1 AS row, j + 1 AS col, ST_Translate(cell, j * $3 + $5, i * $4 + $6) AS geom
FROM generate_series(0, $1 - 1) AS i,
     generate_series(0, $2 - 1) AS j,
(
SELECT ('POLYGON((0 0, 0 '||$4||', '||$3||' '||$4||', '||$3||' 0,0 0))')::geometry AS cell
) AS foo;
$$ LANGUAGE sql IMMUTABLE STRICT;

-- Thanks to Mike T.
-- https://gis.stackexchange.com/questions/16374/creating-regular-polygon-grid-in-postgis
--
-- linksonder: 108000,476000
-- 3 kolommen van 9km, 2 rijen van 9 km
-- breedte DX: 27 km, hoogte DY: 18 km
--
drop table if exists osm.grid_osm_adam;
create table osm.grid_osm_adam as (
SELECT 
    row::varchar || col::varchar id,
    row,
    col,
    ST_setSrid(geom,28992) geom
FROM 
    ST_CreateFishnet(2, 3, 9000, 9000,108000,476000) AS adam_grid);
    
-- join grid on level=0
drop table if exists osm.weg_gestruct_merged_final_clip_mv;
create table osm.weg_gestruct_merged_final_clip_mv as (
select
	grd.id as grid_id,	
	weg.id,
	cat,
	osm_id,
    fclass,
    "name",
    "ref",
	oneway,
	maxspeed,
	layer,
	bridge,
	tunnel,
	linelength,
	ST_Intersection(weg.geom,grd.geom) geom
from
	osm.weg_gestruct_merged_final_incl_vot weg
join
   osm.grid_osm_adam grd
   on ST_intersects(weg.geom,grd.geom)   
where layer = '0' order by grid_id asc);
--ALTER SEQUENCE serial RESTART WITH 0;


-- split base table into separate tables based on grid_id
-- 11,12,13,21,22,23,*/
-- onderstaande moet natuurlijk netjes in een loop...
drop table if exists osm.weg_gestruct_merged_final_clip_mv_11;
drop table if exists osm.weg_gestruct_merged_final_clip_mv_12;
drop table if exists osm.weg_gestruct_merged_final_clip_mv_13;
drop table if exists osm.weg_gestruct_merged_final_clip_mv_21;
drop table if exists osm.weg_gestruct_merged_final_clip_mv_22;
drop table if exists osm.weg_gestruct_merged_final_clip_mv_23;
--
create table osm.weg_gestruct_merged_final_clip_mv_11 as (
	select * from osm.weg_gestruct_merged_final_clip_mv 
	where grid_id='11');
create table osm.weg_gestruct_merged_final_clip_mv_12 as (
	select * from osm.weg_gestruct_merged_final_clip_mv 
	where grid_id='12');
create table osm.weg_gestruct_merged_final_clip_mv_13 as (
	select * from osm.weg_gestruct_merged_final_clip_mv 
	where grid_id='13');
create table osm.weg_gestruct_merged_final_clip_mv_21 as 
	(select * from osm.weg_gestruct_merged_final_clip_mv 
	where grid_id='21');
create table osm.weg_gestruct_merged_final_clip_mv_22 as 
	(select * from osm.weg_gestruct_merged_final_clip_mv 
	where grid_id='22');
create table osm.weg_gestruct_merged_final_clip_mv_23 as 
	(select * from osm.weg_gestruct_merged_final_clip_mv 
	where grid_id='23');

-- TOPOLOGICAL STRUCTURING ON 1 GRID
-- drop topology
SELECT topology.DropTopology('osm_topo');--
-- CreateTopology(varchar topology_schema_name, integer srid, double precision prec, boolean hasz);
select topology.CreateTopology('osm_topo', 28992 ,1 ,false);	

-- build topology for level = 0
-- AddTopoGeometryColumn(varchar topology_name, varchar schema_name, varchar table_name, varchar column_name, varchar feature_type);
select topology.AddTopoGeometryColumn('osm_topo' ,'osm' ,'weg_gestruct_merged_final_clip_mv_11' ,'topo_geom' ,'LINESTRING');


create index weg_gestruct_merged_final_clip_mv_11_GIDX on osm.weg_gestruct_merged_final_clip_mv_11 using GIST(geom);
vacuum analyse osm.weg_gestruct_merged_final_clip_mv_11;
--
--UPDATE osm.weg_gestruct_merged_final_clip_mv_22 SET topo_geom = topology.toTopoGeom(geom, 'osm_topo', 1, 0.5);

--select count(*) from osm.weg_gestruct_merged_final_clip_mv_22;

--exp
drop table if exists osm.log_errors;
create table osm.log_errors (
	id bigint,
	timestamp_error timestamp,
	error_text text,
	id_error_el bigint);
drop sequence if exists seq_log_fid;
create sequence seq_log_fid start 1;

DO $$DECLARE r record;
timestamp_log timestamp;
BEGIN
  FOR r IN SELECT * FROM osm.weg_gestruct_merged_final_clip_mv_11 LOOP
    begin
	  raise notice 'Value: %', r;
	  timestamp_log := now();   
      UPDATE osm.weg_gestruct_merged_final_clip_mv_11 SET topo_geom = topology.toTopoGeom(geom, 'osm_topo', 1, 0.5)
      WHERE id = r.id;
    EXCEPTION
      WHEN OTHERS then
      	insert into osm.log_errors values 
      		(nextval('seq_log_fid'),
      		 timestamp_log, 
      		 'WARNING Loading of record ' || r.id || 'failed: ' || SQLERRM, 
      		 r.id);
        --RAISE WARNING 'Loading of record % failed: %', r.id, SQLERRM;
    END;
  END LOOP;
END$$;
```