**create a df_edges_ams table**

```
drop table if exists osm.df_edges_ams2;
create table osm.df_edges_ams2 (
	area text,
	bridge text,
	geometry text,
	fclass text,
	junction text,
	"key" int4,
	lanes text,
	length numeric,
	"level" int4,
	maxspeed text,
	maxweight text,
	"name" text,
	oneway boolean,
	osmid text,
	"ref" text,
	service text,
	tunnel text
	)


SELECT UpdateGeometrySRID('osm', 'df_edges_ams','geometry',28992);

alter table osm.df_edges_ams
	alter column geometry type geometry(LINESTRING, 28992)
		using ST_setSRID(geometry, 28992)

create index df_edges_ams_GIDX on osm.df_edges_ams using GIST(geometry);
vacuum analyse osm.df_edges_ams;
```

```
-- ============================================================
-- table with layer not 0
-- ============================================================
drop table if exists osm.stag_osm_topology;
CREATE TABLE osm.stag_osm_topology (
    id int8,
	osm_id int8 NULL,
	--code int4 NULL,
	fclass varchar(28) NULL,
	streetname varchar(100) NULL,
	oneway varchar(10) NULL,
	maxspeed text NULL,
	layer int2 NULL,
	bridge varchar(10) NULL,
	tunnel varchar(10) NULL,
	linelength numeric,
    CONSTRAINT stag_osm_topology_pk PRIMARY KEY (id)
)
WITH (OIDS=FALSE);
--
SELECT AddGeometryColumn('osm','stag_osm_topology', 'geom', 28992, 'GEOMETRY', 2);

--
drop sequence if exists seq_stag_osm_fid;
create sequence seq_stag_osm_fid start 1;

insert into osm.stag_osm_topology (
	select
    	nextval('seq_stag_osm_fid'),
		cast(osmid as double precision),
--		code,
		fclass,
		"name" as streetname,
		oneway,
		maxspeed::text,
		"level"::int2 as layer,
		bridge,
		tunnel,
    	st_Length(geometry) "length",
    	ST_setSRID(geometry, 28992) as geom
from
	osm.df_edges_ams
where "level" = '0'
);

```

```
-- add inzamellocaties poi, conenct to street network
insert into osm.stag_osm_topology
(select distinct on (loc_id)
    nextval('seq_stag_osm_fid'),
	osmid,
	--code,
	'inzamelloc_link' fclass,
	streetname,
	oneway,
	maxspeed,
	"level",
	bridge,
	tunnel,
    linelength,
    geomline as geom
--
from (
	SELECT 
		loc.cluster_id loc_id,  
    	--edg.id as id,
	    ST_ShortestLine(loc.geom_inzamelloc, edg.geometry) geomline,
	    st_length(ST_ShortestLine(loc.geom_inzamelloc,edg.geometry)) linelength,
	    osmid::int8,
	    --code,
	    fclass,
	    "name" as streetname,
	    oneway,
	    maxspeed,
	    "level",
	    bridge,
	    tunnel
	FROM service_afvalcontainers.stag_inzamellocaties loc
	join 
		osm.df_edges_ams edg on ST_Dwithin(loc.geom_inzamelloc,edg.geometry,100)
	where level= '0'
	) prj
order by loc_id, linelength asc
);
-- indices and vaccum
create index stag_osm_topology_gidx on osm.stag_osm_topology using GIST(geom);
vacuum analyze osm.stag_osm_topology;
```

```
-- ============================================================
-- table with layer not 0
-- ============================================================
--
drop table if exists osm.stag_osm_topology_lev;
CREATE TABLE osm.stag_osm_topology_lev (
    id int8,
	osm_id int8 NULL,
	--code int4 NULL,
	fclass varchar(28) NULL,
	streetname varchar(100) NULL,
	oneway varchar(10) NULL,
	maxspeed text NULL,
	layer int2 NULL,
	bridge varchar(20) NULL,
	tunnel varchar(20) NULL,
    linelength numeric
)
WITH (
	OIDS=FALSE
) ;
--
SELECT AddGeometryColumn( 'osm','stag_osm_topology_lev', 'geom', 28992, 'GEOMETRY', 2);
--
insert into osm.stag_osm_topology_lev
(select
    nextval('seq_stag_osm_fid'),
	cast(osmid as double precision),
	--code,
	fclass,
	"name" as streetname,
	oneway,
	maxspeed::text,
	"level"::int2 as layer,
	bridge,
	tunnel,
    st_Length(geometry) linelength,
    geometry as geom
from
	osm.df_edges_ams
where "level" != '0'
);
--
--
insert into osm.stag_osm_topology_lev
(select distinct on (loc_id)
    nextval('seq_stag_osm_fid'),
	osmid,
	--code,
	'inzamelloc_link' fclass,
	streetname,
	oneway,
	maxspeed,
	"level",
	bridge,
	tunnel,
    linelength,
    geomline as geom
--
from (
	SELECT 
		loc.cluster_id as loc_id, 
        --edg.id as id,    
	    ST_ShortestLine(loc.geom_inzamelloc,edg.geometry) geomline,
	    st_length(ST_ShortestLine(loc.geom_inzamelloc,edg.geometry)) linelength,
	    osmid::int8,
	    --code,
	    fclass,
	    "name" as streetname,
	    oneway,
	    maxspeed,
	    "level",
	    bridge,
	    tunnel
	FROM 
            service_afvalcontainers.stag_inzamellocaties loc
	join 
            osm.df_edges_ams edg on ST_Dwithin(loc.geom_inzamelloc,edg.geometry,100)
        where "level" !=0
	) prj
order by loc_id, linelength asc
);
-- indices and vaccum
create index stag_osm_topology_lev_gidx on osm.stag_osm_topology_lev using GIST(geom);
vacuum analyze osm.stag_osm_topology_lev;
```

```
-- create topology with postgis topology engine
--- CreateTopology(varchar topology_schema_name, integer srid, double precision prec, boolean hasz);
select topology.CreateTopology('ams_topo', 28992 , 0.5 , false);
--
-- build topology for level = 0
-- AddTopoGeometryColumn(varchar topology_name, varchar schema_name, varchar table_name, varchar column_name, varchar feature_type);
select topology.AddTopoGeometryColumn('ams_topo' ,'osm' ,'stag_osm_topology' ,'topo_geom' ,'LINESTRING');
-- toTopoGeom(geometry geom, varchar toponame, integer layer_id, float8 tolerance);
UPDATE osm.stag_osm_topology SET topo_geom = topology.toTopoGeom(geom, 'ams_topo', 1, 0.25);
```

to see the topology tables type:
```
SELECT * FROM information_schema.tables 
WHERE table_schema = 'ams_topo';
```

error message with precision .25
```
Query execution failed

Reason:
SQL Error [XX000]: ERROR: SQL/MM Spatial exception - geometry crosses edge 48484
  Where: PL/pgSQL function totopogeom(geometry,topogeometry,double precision) line 114 at FOR over SELECT rows
PL/pgSQL function totopogeom(geometry,character varying,integer,double precision) line 89 at assignment
```

error message with precision .5
```
Query execution failed

Reason:
SQL Error [XX000]: ERROR: SQL/MM Spatial exception - curve not simple
  Where: PL/pgSQL function totopogeom(geometry,topogeometry,double precision) line 114 at FOR over SELECT rows
PL/pgSQL function totopogeom(geometry,character varying,integer,double precision) line 89 at assignment
```

```
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
  FOR r IN SELECT * FROM osm.stag_osm_topology LOOP
    begin
	  timestamp_log := now();   
      UPDATE osm.stag_osm_topology SET topo_geom = topology.toTopoGeom(geom, 'ams_topo', 1, 0.5)
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
** stag-ams_cleaned**
```
-- topology fixed for level 0 which is sufficient for most purposes, 
-- now combine the new geometry with the original attributes
drop table if exists osm.stag_ams_cleaned;
create table osm.stag_ams_cleaned as (
select
	top.id,
	top.osm_id,
	--top.code,
	top.fclass,
	top.streetname,
	top.oneway,
	top.maxspeed,
	top.layer,
	top.bridge,
	top.tunnel,
	st_length(edg.geom) linelength,
	edg.geom
from
	ams_topo.edge_data edg   --ipv ams_topo.edge 
join
    ams_topo.relation rel on edg.edge_id = rel.element_id
join 
    osm.stag_osm_topology top on rel.topogeo_id = top.id
)
union all
--
select
	id,
	osm_id,
	--code,
	fclass,
	streetname,
	oneway,
	maxspeed,
	layer,
	bridge,
	tunnel,
	linelength,
	geom
from
	osm.stag_osm_topology_lev;
--
create index stag_osm_cleaned_gidx on osm.stag_ams_cleaned using GIST(geom);
vacuum analyze osm.stag_ams_cleaned;
```