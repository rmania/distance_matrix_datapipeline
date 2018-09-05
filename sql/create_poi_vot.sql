drop table if exists osm.bag_lpnd_pnd_id;
create table osm.bag_lpnd_pnd_id as (
	select distinct id as pnd_id,
		   landelijk_id as landelijk_pnd_id
from bagdatapunt.bag_pand);

SELECT (landelijk_pnd_id)::text, count(*)
FROM osm.bag_lpnd_pnd_id
GROUP BY landelijk_pnd_id
HAVING count(*) > 1


drop table if exists osm.poi_objecten_vot;
create table osm.poi_objecten_vot as(
		select
			pnd.id as pnd_id,
			pnd.landelijk_id as landelijk_pnd_id,
			pnd.geometrie as pnd_geom, -- nodig voor finale kaarten
			vbo.id as vot_id,
			vbo.landelijk_id as landelijk_vot_id,
			vbo.geometrie as obj_geom,
			ST_centroid(vbo.geometrie) as point_geom,
			vbo."_openbare_ruimte_naam" as adres,
    		vbo."_huisnummer" as nummer,
    		vbo."_huisletter" as letter,
    		vbo."_huisnummer_toevoeging" as num_toev,
    		vbo.buurt_id,
    		vbo.eigendomsverhouding_id as eigendom_id,
    		eig.omschrijving as koop_huur,
			sdl.naam as stadsdeel,
		    'vot'::text as "type" 
		from 
			bagdatapunt.bag_verblijfsobject as vbo 
		join 
			bagdatapunt.bag_gebruiksdoel as gbd 
			on vbo.id = gbd.verblijfsobject_id
		join 
			bagdatapunt.bag_verblijfsobjectpandrelatie as vpr 
			on vbo.id = vpr.verblijfsobject_id 
		left join 
			bagdatapunt.bag_eigendomsverhouding as eig 
			on vbo.eigendomsverhouding_id = eig.code
		join 
    		gebieden.gbd_stadsdeel as sdl
    		on ST_Intersects(sdl.geometry, vbo.geometrie)
		join 
			bagdatapunt.bag_pand as pnd 
			on vpr.pand_id = pnd.id -- normale pand_id 
		join	
			osm.bag_lpnd_pnd_id as lpnd
			on pnd.id = lpnd.pnd_id --added landelijk pand id
		where gbd.code = '1000' --1000 woonfunctie
			and vbo.status_id in('20','21') --20 Vot in gebruik (niet ingemeten), 21 vot in gebruik 
			and (gbd.code like '10%' or gbd.code_plus like '10%') 
			and pnd.status_id in ('30', '31') -- 30 Pand in gebruik (niet ingemeten), 31 Pand in gebruik 
			)

create index poi_objecten_vot_gidx on osm.poi_objecten_vot using GIST(obj_geom);
vacuum analyze osm.poi_objecten_vot;


drop table if exists osm.poi_objecten_lgp;
create table osm.poi_objecten_lgp as (
		select
			null as "pnd_id",
			null as "landelijk_pnd_id",
			lig.geometrie as pnd_geom,
			lig.id as vot_id,
			lig.landelijk_id as landelijk_vot_id,
			lig.geometrie as obj_geom,
			ST_centroid(lig.geometrie) as point_geom,
			lig."_openbare_ruimte_naam" as adres,
    		lig."_huisnummer" as nummer,
    		lig."_huisletter" as letter,
    		lig."_huisnummer_toevoeging" as num_toev,
    		lig.buurt_id,
    		null as "eigendom_id",
    		null as "koop_huur",
			sdl.naam as stadsdeel,
		    'lgp'::text as "type" 
		from 
			bagdatapunt.bag_ligplaats as lig
		join 
    		gebieden.gbd_stadsdeel as sdl
    		on ST_Intersects(sdl.geometry, lig.geometrie)
		where lig.status_id = '33'); --33 Plaats aangewezen (stelselpedia)
			
create index poi_objecten_lgp_gidx on osm.poi_objecten_lgp using GIST(obj_geom);
vacuum analyze osm.poi_objecten_lgp;


drop table if exists osm.poi_object_total;
create table osm.poi_object_total as (
	select * from osm.poi_objecten_vot
	union all
	select * from osm.poi_objecten_lgp
	);

-- create DBSCAN spatial clusters
drop table if exists osm.poi_object_total_cluster;
create table osm.poi_object_total_cluster as (
	select 
			pnd_id,
			landelijk_pnd_id,
			pnd_geom,
			vot_id,
			landelijk_vot_id,
			obj_geom,
			point_geom,
			ST_ClusterDBSCAN(point_geom, 2.5, 1) over() as cluster_toewijzing,
			adres,
    		nummer,
    		letter,
    		num_toev,
    		eigendom_id,
    		koop_huur,
    		buurt_id,
			stadsdeel,
		    "type"
from osm.poi_object_total);

create index poi_object_total_cluster_GIDX on osm.poi_object_total_cluster using GIST(point_geom);
vacuum analyse osm.poi_object_total_cluster	


--create centroids per cluster 
drop table if exists osm.poi_cluster_centroids;
create table osm.poi_cluster_centroids as (
	select 
		cluster_toewijzing as cluster_no,
		ST_Centroid(ST_Union(point_geom)) as cl_geom			
from osm.poi_object_total_cluster
group by cluster_toewijzing
);

create index poi_cluster_centroids_GIDX on osm.poi_cluster_centroids using GIST(cl_geom);
vacuum analyse osm.poi_cluster_centroids

-- join poi_object_total_cluster with poi_cluster_centroids
create table osm.stag_vot_cluster as (
	select * from osm.poi_object_total_cluster as pct
	join
		osm.poi_cluster_centroids as clc
		on pct.cluster_toewijzing = clc.cluster_no
	)

-- remove double cluster col
alter table osm.stag_vot_cluster
drop column cluster_no
