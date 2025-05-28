select
	h.name as csid_s,
	co.objectnumber as accessionnumber_s,
	regexp_replace(getdispl(tig.taxon), E'[\\t\\n\\r]+', ' ', 'g') as determination_s,
	regexp_replace(ttg.termformatteddisplayname, E'[\\t\\n\\r]+', ' ', 'g') as termformatteddisplayname_s,
	regexp_replace(getdispl(tnh.family), E'[\\t\\n\\r]+', ' ', 'g') as family_s,
	tnh.taxonbasionym as taxonbasionym_s,
	tu.taxonmajorgroup as majorgroup_s,
	regexp_replace(getdispl(fc.item), E'[\\t\\n\\r]+', ' ', 'g') as collector_ss,
	co.fieldcollectionnumber as collectornumber_s,
	sdg.datedisplaydate as collectiondate_s,
	to_char(sdg.dateearliestscalarvalue, 'YYYY-MM-DD') as earlycollectiondate_dt,
	case
		when (sdg.datelatestscalarvalue::date - sdg.dateearliestscalarvalue::date = 1 
			and nullif(sdg.datelatestday, 0) is null)
		then to_char(sdg.dateearliestscalarvalue, 'YYYY-MM-DD')
		else to_char(sdg.datelatestscalarvalue, 'YYYY-MM-DD')
	end as latecollectiondate_dt,
	regexp_replace(lg.fieldlocverbatim, E'[\\t\\n\\r]+', ' ', 'g') as locality_s,
	getdispl(lg.fieldloccounty) as collcounty_s,
	getdispl(lg.fieldlocstate) as collstate_s,
	getdispl(lg.fieldloccountry) as collcountry_s,
	lg.velevation as elevation_s,
	lg.minelevation as minelevation_s,
	lg.maxelevation as maxelevation_s,
	lg.elevationunit as elevationunit_s,
	regexp_replace(co.fieldcollectionnote, E'[\\t\\n\\r]+', ' ', 'g') as habitat_s,
	lg.decimallatitude as location_0_d,
	lg.decimallongitude as location_1_d,
	lg.decimallatitude || ',' || lg.decimallongitude as latlong_p,
	case when lg.vcoordsys like 'Township%' then lg.vcoordinates end as trscoordinates_s,
	lg.geodeticdatum as datum_s,
	lg.coorduncertainty as coordinateuncertainty_f,
	lg.coorduncertaintyunit as coordinateuncertaintyunit_s,
	lg.localitynote as localitynote_s,
	lg.localitysource as localitysource_s,
	lg.localitysourcedetail as localitysourcedetail_s,
	getdispl(lg.georefsource) as georefsource_s,
	lg.georefremarks as georefremarks_s,
	lg.geoRefencedBy as georeferencedby_s,
	cc.updatedat as updatedat_dt,
	getdispl(conh.labelheader) as labelheader_s,
	getdispl(conh.labelfooter) as labelfooter_s,
	array_to_string(array(
		select
			coalesce(nullif(tig1.qualifier || ' ', ''), '')
			|| case when tig1.taxon not like '%no name%' then (
				coalesce(getdispl(tig1.taxon), '')	
					|| case when tig1.identby not like '%unknown%'
						 then coalesce(', by ' || nullif(getdispl(tig1.identby), ''), '') else '' end
					|| coalesce(', ' || nullif(getdispl(tig1.institution), ''), '')
					|| coalesce(', ' || nullif(trim(prevdetsdg1.datedisplaydate), ''), '')
					|| coalesce(' (' || nullif(tig1.identkind, '') || ')', '')
				) else '' end
			|| coalesce('. ' || nullif(tig1.notes, ''), '')
		from collectionobjects_common c1
		left outer join hierarchy htig1 on (
			c1.id = htig1.parentid
			and htig1.pos > 0
			and htig1.name = 'collectionobjects_naturalhistory:taxonomicIdentGroupList')
		left outer join taxonomicidentgroup tig1 on (tig1.id = htig1.id)
		left outer join hierarchy hprevdet1 on (
			tig1.id = hprevdet1.parentid
			and hprevdet1.name = 'identDateGroup')
		left outer join structureddategroup prevdetsdg1 on (prevdetsdg1.id = hprevdet1.id)
		where c1.id = co.id
		order by htig1.pos), '␥', '') previousdeterminations_ss,
	lng.localname as localname_s,
	nullif(cocbd.item, '') as briefdescription_s,
	lg.vdepth as depth_s,
	lg.mindepth as mindepth_s,
	lg.maxdepth as maxdepth_s,
	lg.depthunit as depthUnit_s,
	array_to_string(array(
		select coalesce(nullif(getdispl(atg2.associatedtaxon), ''), '')
			|| coalesce(' (' || nullif(atg2.interaction, '') || ')', '')
		from collectionobjects_common c2
		left outer join hierarchy hatg2 on (
			c2.id = hatg2.parentid
			and hatg2.name = 'collectionobjects_naturalhistory:associatedTaxaGroupList')
		left outer join associatedtaxagroup atg2 on (hatg2.id = atg2.id)
		where c2.id = co.id
		order by hatg2.pos), '␥', '') as associatedtaxa_ss,
	array_to_string(array(
		select coalesce(nullif(tsg3.typespecimenkind, ''), '') 
			|| coalesce(' (' || nullif(getdispl(tsg3.typespecimenbasionym), '') || ')', '') 
		from collectionobjects_common c3
		left outer join hierarchy htsg3 on (
		   c3.id = htsg3.parentid
		   and htsg3.name = 'collectionobjects_naturalhistory:typeSpecimenGroupList')
		left outer join typespecimengroup tsg3 on (tsg3.id = htsg3.id)
		where c3.id = co.id
		order by htsg3.pos), '␥', '') as typeassertions_ss,
	nullif(conh.cultivated, '') as Cultivated_s,
	nullif(co.sex, '') as sex_s,
	getdispl(co.phase) as phase_s,
	array_to_string(array(
		select coalesce(nullif(ong4.numbervalue, ''), '')
			|| coalesce(' (' || nullif(getdispl(ong4.numbertype), '') || ')', '')
		from collectionobjects_common c4
		left outer join hierarchy hong4 on (
			c4.id = hong4.parentid
			and hong4.name = 'collectionobjects_common:otherNumberList')
		left outer join othernumber ong4 on (ong4.id = hong4.id)
		where c4.id = co.id
		order by hong4.pos), '␥', '') as othernumber_ss,
	'ucbgacccession' as ucbgaccessionnumber_s,
	case
		when tig.identby not like '%unknown%'
		then coalesce(nullif(getdispl(tig.identby), ''), '')
			|| coalesce(', ' || nullif(getdispl(tig.institution), ''), '') 
			|| coalesce(', ' || nullif(trim(detdetailssdg.datedisplaydate), ''), '')
			|| coalesce(' (' || nullif(tig.identkind, '') || ')', '')
			|| coalesce('. ' || nullif(tig.notes, ''), '')
		else '' end AS determinationdetails_s,
	'' as loanstatus_s,
	'' as loannumber_s,
	regexp_replace(getdispl(fc.item),E'[\\t\\n\\r]+', ' ', 'g') as collectorverbatim_s,
	array_to_string(array(
		select
			case when lg5.fieldlocverbatim not like '%unknown%' 
				then coalesce(nullif(getdispl(lg5.fieldlocverbatim), ''), '') 
				else '' end
		from collectionobjects_common c5
		left outer join hierarchy hlg5 on (
			c5.id = hlg5.parentid
			and hlg5.pos > 0
			and hlg5.name = 'collectionobjects_naturalhistory:localityGroupList')
		left outer join localitygroup lg5 on (lg5.id = hlg5.id)
		where c5.id = co.id order by hlg5.pos), '␥', '') as otherlocalities_ss,
	array_to_string(array(
		select
			case when lg6.fieldlocverbatim not like '%unknown%'
				then coalesce(nullif(getdispl(lg6.fieldlocverbatim), ''), '')
				else '' end
		from collectionobjects_common c6
		left outer join hierarchy hlg6 on (
			c6.id = hlg6.parentid
			and hlg6.pos >= 0
			and hlg6.name = 'collectionobjects_naturalhistory:localityGroupList')
		left outer join localityGroup lg6 on (lg6.id = hlg6.id)
		where c6.id = co.id order by hlg6.pos), '␥', '') as alllocalities_ss,
	case when (tsg.typespecimenbasionym is not null and tsg.typespecimenbasionym <>'') 
		 then 'yes' else 'no' end as hastypeassertions_s,
	tig.qualifier as determinationqualifier_s,
	array_to_string(array(
		select com7.item
		from collectionobjects_common_comments com7
		where com7.id = co.id
		and com7.pos is not null
		order by com7.pos), '␥', '') AS comments_ss,
	co.numberofobjects as numberofobjects_s,
	conh.objectcountnumber AS objectcount_s,
	case when (co.numberofobjects > 0 and conh.objectCountNumber > 0)
		then (to_char(conh.objectcountnumber,'FM999') || ' of ' || to_char(co.numberofobjects,'FM999'))
		else '' end as sheet_s,
	cc.createdat as createdat_dt,
	cj.postToPublic as posttopublic_s,
	'' as references_ss
from collectionobjects_common co
join misc on (
	co.id = misc.id
	and misc.lifecyclestate <> 'deleted')
join hierarchy h on (co.id = h.id)
join collectionspace_core cc on (co.id = cc.id)
left outer join collectionobjects_common_fieldCollectors fc on (
	co.id = fc.id
	and fc.pos = 0)
left outer join hierarchy hfcdg on (
	co.id = hfcdg.parentid
	and hfcdg.name = 'collectionobjects_common:fieldCollectionDateGroup')
left outer join structureddategroup sdg on (sdg.id = hfcdg.id)
left outer join hierarchy htig on (
	co.id = htig.parentid
	and htig.pos = 0
	and htig.name = 'collectionobjects_naturalhistory:taxonomicIdentGroupList')
left outer join taxonomicIdentGroup tig on (tig.id = htig.id)
left outer join hierarchy hdetdetailsdate on (
	tig.id = hdetdetailsdate.parentid
	and hdetdetailsdate.name = 'identDateGroup')
left outer join structureddategroup detdetailssdg on (detdetailssdg.id = hdetdetailsdate.id)
left outer join hierarchy hlg on (
	co.id = hlg.parentid and hlg.pos = 0
	and hlg.name = 'collectionobjects_naturalhistory:localityGroupList')
left outer join localitygroup lg on (lg.id = hlg.id)
left outer join taxon_common tc on (tig.taxon = tc.refname)
left outer join hierarchy httg on (
	tc.id = httg.parentid
	and httg.name = 'taxon_common:taxonTermGroupList'
	and httg.pos = 0)
left outer join taxontermgroup ttg on (ttg.id = httg.id)
left outer join hierarchy htsg on (
	co.id = htsg.parentid
	and htsg.pos = 0
	and htsg.name = 'collectionobjects_naturalhistory:typeSpecimenGroupList')
left outer join typespecimengroup tsg on (tsg.id = htsg.id)
left outer join taxon_ucjeps tu on (tc.id = tu.id)
left outer join taxon_naturalhistory tnh on (tc.id = tnh.id)
left outer join collectionobjects_naturalhistory conh on (co.id = conh.id)
left outer join collectionobjects_ucjeps cj on (co.id = cj.id)
left outer join hierarchy hlng on (
	co.id = hlng.parentid
	and hlng.primarytype = 'localNameGroup'
	and hlng.pos = 0)
left outer join localnamegroup lng on (hlng.id = lng.id)
left outer join collectionobjects_common_briefdescriptions cocbd on (
	co.id = cocbd.id
	and cocbd.pos = 0)
where substring(co.objectnumber from '^[A-Z]*') not in ('DHN', 'UCSB', 'UCSC')
and (cj.posttopublic = 'yes' or cj.posttopublic is null)
and co.id in ('939efd96-14ad-40e1-84eb-35c2ad6e5de2', '8bfc4143-c44d-4bf1-9c1a-e9b8986ffd31', '44301be0-2e24-4770-8b6a-ed65840089f3', 'd44365c2-e2e7-4786-afc2-0af430d482dc')

/*
-- left outer join hierarchy hgr on (
--	 co.id = hgr.parentid and hgr.pos = 0 and hgr.name = 'collectionobjects_naturalhistory:localityGroupList')
-- left outer join placegeorefgroup gr on (gr.id = hgr.id)
--
-- test records
-- co.id in ('939efd96-14ad-40e1-84eb-35c2ad6e5de2', '8bfc4143-c44d-4bf1-9c1a-e9b8986ffd31', '44301be0-2e24-4770-8b6a-ed65840089f3', 'd44365c2-e2e7-4786-afc2-0af430d482dc')
-- h.name in ('338075de-821c-49b3-8f34-969cc666a61e', '32328608-467e-46c3-875c-6de0cece0be0', '33803cfe-e6a8-4025-bf53-a3814cf4da82', '0ad96db0-be78-4a0b-8f99-9fb229222ffb')
-- h.name = '338075de-821c-49b3-8f34-969cc666a61e' -- JEPS4687, id = '8bfc4143-c44d-4bf1-9c1a-e9b8986ffd31'
-- h.name = '32328608-467e-46c3-875c-6de0cece0be0' -- UC9999, id = 'd44365c2-e2e7-4786-afc2-0af430d482dc'
-- h.name = '33803cfe-e6a8-4025-bf53-a3814cf4da82'	-- JEPS105623, id = '939efd96-14ad-40e1-84eb-35c2ad6e5de2'
-- h.name = '0ad96db0-be78-4a0b-8f99-9fb229222ffb'	-- JEPS70526, id = '44301be0-2e24-4770-8b6a-ed65840089f3'
*/
