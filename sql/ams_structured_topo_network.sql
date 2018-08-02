
zorg ervoor dat de SRID van de edges table staat gedefinieerd. 
```
alter table osm.df_edges_centrum
	alter column geometry type geometry(LINESTRING, 28992)
		using ST_SetSRID(geometry, 28992);
```

### topologisch structurering

```
--
-- https://docs.pgrouting.org/2.2/en/doc/index.html
-- ST_ShortestLine
-- http://www.postgis.us/downloads/postgis21_topology_cheatsheet.pdf
-- http://blog.mathieu-leplatre.info/use-postgis-topologies-to-clean-up-road-networks.html
-- https://pgrouting.org/
-- ==============================================================================
-- create topology with postgis topology
-- ==============================================================================
--
update bi_afval.osm_west
set layer=0 where layer is null;
--
-- layer 0
drop table if exists bi_afval.stag_osm_west_topology;
CREATE TABLE bi_afval.stag_osm_west_topology (
    id int8,
	osm_id float8 NULL,
	code int4 NULL,
	fclass varchar(28) NULL,
	streetname varchar(100) NULL,
	oneway varchar(1) NULL,
	maxspeed int4 NULL,
	layer float8 NULL,
	bridge varchar(1) NULL,
	tunnel varchar(1) NULL,
        linelength numeric
        CONSTRAINT stag_osm_west_topology_pk PRIMARY KEY (id)
)
WITH (
	OIDS=FALSE
) ;
--
SELECT AddGeometryColumn( 'bi_afval','stag_osm_west_topology', 'geom', 28992, 'GEOMETRY', 2 );
--
drop sequence if exists seq_stag_osm_fid;
create sequence seq_stag_osm_fid start 1;

insert into bi_afval.stag_osm_west_topology
(
select
    nextval('seq_stag_osm_fid'),
	osm_id::float8,
	code,
	fclass,
	"name" as streetname,
	oneway,
	maxspeed,
	layer,
	bridge,
	tunnel,
        st_Length(geom) linelength,
        geom
from
	bi_afval.osm_west
where 
    fclass  not in('motorway','motorway_link','primary','primary_link')
and layer=0
)
;
--
--
insert into bi_afval.stag_osm_west_topology
(select distinct on (loc_id)
    nextval('seq_stag_osm_fid'),
	osm_id,
	code,
	'inzamelloc_link' fclass,
	streetname,
	oneway,
	maxspeed,
	layer,
	bridge,
	tunnel,
        linelength,
        geomline as geom
--
from
	(
	SELECT loc.id as loc_id,  
               osm.id as id,
	       ST_ShortestLine(loc.geom,osm.geom) geomline,
	       st_length(ST_ShortestLine(loc.geom,osm.geom)) linelength,
	       osm_id::float8,
	       code,
	       fclass,
	       "name" as streetname,
	       oneway,
	       maxspeed,
	       layer,
	       bridge,
	       tunnel
	FROM 
            bi_afval.stag_inzamellocaties loc
	join 
            bi_afval.osm_west osm on ST_Dwithin(loc.geom,osm.geom,100)
        where 
            fclass  not in('motorway','motorway_link','primary','primary_link')
        and layer=0
	) prj
order by loc_id, linelength asc
);
--
create index stag_osm_west_topology_gidx on bi_afval.stag_osm_west_topology using GIST(geom);
vacuum analyze bi_afval.stag_osm_west_topology;
--
-- ============================================================
-- table with layer not 0
-- ============================================================
--
drop table if exists bi_afval.stag_osm_west_topology_lev;
CREATE TABLE bi_afval.stag_osm_west_topology_lev (
        id int8,
	--fid int8 NULL,
	osm_id float8 NULL,
	code int4 NULL,
	fclass varchar(28) NULL,
	streetname varchar(100) NULL,
	oneway varchar(1) NULL,
	maxspeed int4 NULL,
	layer float8 NULL,
	bridge varchar(1) NULL,
	tunnel varchar(1) NULL,
        linelength numeric
)
WITH (
	OIDS=FALSE
) ;
--
SELECT AddGeometryColumn( 'bi_afval','stag_osm_west_topology_lev', 'geom', 28992, 'GEOMETRY', 2 );
--
insert into bi_afval.stag_osm_west_topology_lev
(
select
        nextval('seq_stag_osm_fid'),
	--id::int8,
	osm_id::float8,
	code,
	fclass,
	"name" as streetname,
	oneway,
	maxspeed,
	layer,
	bridge,
	tunnel,
        st_Length(geom) linelength,
        geom
from
	bi_afval.osm_west
where 
    fclass  not in('motorway','motorway_link','primary','primary_link')
and layer !=0
)
;
--
--
insert into bi_afval.stag_osm_west_topology_lev
(select distinct on (loc_id)
    nextval('seq_stag_osm_fid'),
	osm_id,
	code,
	'inzamelloc_link' fclass,
	streetname,
	oneway,
	maxspeed,
	layer,
	bridge,
	tunnel,
        linelength,
        geomline as geom
--
from
	(
	SELECT loc.id as loc_id, 
               osm.id as id,    
	       ST_ShortestLine(loc.geom,osm.geom) geomline,
	       st_length(ST_ShortestLine(loc.geom,osm.geom)) linelength,
	       osm_id::float8,
	       code,
	       fclass,
	       "name" as streetname,
	       oneway,
	       maxspeed,
	       layer,
	       bridge,
	       tunnel
	FROM 
            bi_afval.stag_inzamellocaties loc
	join 
            bi_afval.osm_west osm on ST_Dwithin(loc.geom,osm.geom,100)
        where 
            fclass  not in('motorway','motorway_link','primary','primary_link')
        and layer !=0
	) prj
order by loc_id, linelength asc
);
--
--
-- ==================================================================================================================
-- create topology
-- ==================================================================================================================
-- ToDo: build topology for sepearate levels
-- drop topology
SELECT topology.DropTopology('west_topo');
--
-- create topology with postgis topology engine
--
-- CreateTopology(varchar topology_schema_name, integer srid, double precision prec, boolean hasz);
select topology.CreateTopology('west_topo', 28992 ,0.5 ,false);
--
--
-- build topology for level = 0
-- AddTopoGeometryColumn(varchar topology_name, varchar schema_name, varchar table_name, varchar column_name, varchar feature_type);
select topology.AddTopoGeometryColumn('west_topo' ,'bi_afval' ,'stag_osm_west_topology' ,'topo_geom' ,'LINESTRING');
--
UPDATE bi_afval.stag_osm_west_topology SET topo_geom = topology.toTopoGeom(geom, 'west_topo', 1, 0.5);
--
-- topology is fixed for level 0 which is sufficient for most purposes, now combine the new geometry with the original attributes
--
drop table if exists bi_afval.stag_west_cleaned;
create table bi_afval.stag_west_cleaned as (
select
	top.id,
	top.osm_id,
	top.code,
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
	west_topo.edge edg 
join
    west_topo.relation rel on edg.edge_id = rel.element_id
join 
    bi_afval.stag_osm_west_topology top on rel.topogeo_id = top.id
)
--
union all
--
select
	id,
	osm_id,
	code,
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
	bi_afval.stag_osm_west_topology_lev;
--
create index stag_osm_west_cl_gidx on bi_afval.stag_west_cleaned using GIST(geom);
vacuum analyze bi_afval.stag_west_cleaned;
--
--
-- ================================================================================================
-- create network topology using pgrouting
-- ================================================================================================
/*
Network topology

As we have seen before, routing functions take as an argument network of connections in the form of a graph (start node id, end node id), 
so the last thing to do is to convert our actual road network to a graph. Fortunately, pgRouting helps us a lot with this task.

Start with adding two additional columns to our table.
*/
--
ALTER TABLE bi_afval.stag_west_cleaned ADD COLUMN source integer;
ALTER TABLE bi_afval.stag_west_cleaned ADD COLUMN target integer;
CREATE INDEX osm_west_cl_source_idx ON bi_afval.stag_west_cleaned (source);
CREATE INDEX osm_west_cl_target_idx ON bi_afval.stag_west_cleaned (target);
--
--
/*
And then use pgr_createTopology function which builds a network topology based on the geometry information
(it analyses roads geometry and automatically assigns node ids to the source and target columns).
*/
--
SELECT pgr_createTopology('bi_afval.stag_west_cleaned', 0.5, 'geom', 'id', clean := TRUE); 
--
--
SELECT * FROM pgr_dijkstra(
    'SELECT fid AS id,
         source,
         target,
         linelength AS cost
        FROM bi_afval.stag_osm_west',
    8151, 4831,
    directed := false);
--
```