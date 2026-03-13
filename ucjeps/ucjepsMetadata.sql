/* ucjepsMetadata.sql change log:
 * CSW-933: collection dates
   * add 8 hours to scalar dates to fix day offset for hours >= 16
 * CSW-939: collectors
   * collector_ss: change from pos=0 collector to include all collectors in a list
 * CSW-940: single collector
   * collectorverbatim_s: keep as first collector
 * CSW-941: collection number assignor
   * collectors_verbatim_s: replace with collectionobjects_naturalhistory.fieldCollectionNumberAssignor
   * preserving the old solr field name collectors_verbatim_s, as it may affect merrit_archive job
   * update appconfig.py, public_grid.html, ucjepspublicparms.csv to reflect labeling change.
 * CSW-944: number of objects, object counts, sheet counts
   * sheet_s: change from collectionobjects_common.numberofobjects to objectcountgroup.objectcount
 * CSW-948: UCBG accession number
   * ucbgaccessionnumber_s: remove by setting to null
 * CSW-972 / CSW-982: 
   * update query to use temp tables for aggregate fields
*/

----------------------------------------------------------------------------------------------------
-- create temp tables for final select instead of single query with subqueries or CTE
-- reduces need for work_mem
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- objects: object data
-- exclude objectnumber ~ 'DHN' or null, deleted, posttopublic = 'yes' or is null
-- ~ 1.17M rows
----------------------------------------------------------------------------------------------------

create temp table tt_objects as (
  select
    hcc.name as csid,
    cc.id,
    cc.objectnumber as accessionnumber_s,
    cc.fieldcollectionnumber as collectornumber_s,
    getdispl(cnh.fieldcollectionnumberassignor) as collectors_verbatim_s,
    cucj.posttopublic as posttopublic_s,
    regexp_replace(cc.fieldcollectionnote, E'[\\t\\n\\r]+', ' ', 'g') as habitat_s,
    csc.createdat as createdat_dt,
    csc.updatedat as updatedat_dt,
    getdispl(cnh.labelheader) as labelheader_s,
    getdispl(cnh.labelfooter) as labelfooter_s,
    nullif(cnh.cultivated, '') as cultivated_s,
    ocg.objectcount as numberofobjects_s,
    cnh.objectcountnumber as objectcount_s,
    case when (ocg.objectcount > 0 and cnh.objectcountnumber > 0)
      then (to_char(cnh.objectcountnumber, 'FM999') || ' of ' || to_char(ocg.objectcount, 'FM999'))
      else '' end as sheet_s,
    nullif(cc.sex, '') as sex_s,
    getdispl(cc.phase) as phase_s,
    lng.localname as localname_s,
    nullif(ccbd.item, '') as briefdescription_s
  from collectionobjects_common cc
    join misc mcc on cc.id = mcc.id
    join hierarchy hcc on cc.id = hcc.id
    join collectionspace_core csc on cc.id = csc.id
    left outer join collectionobjects_ucjeps cucj on cc.id = cucj.id
    left outer join collectionobjects_naturalhistory cnh on cc.id = cnh.id
    left outer join collectionobjects_common_briefdescriptions ccbd on (
      cc.id = ccbd.id
      and ccbd.pos = 0)
    left outer join hierarchy hocg on (
      cc.id = hocg.parentid
      and hocg.primarytype = 'objectCountGroup'
      and hocg.pos = 0)
    left outer join objectcountgroup ocg on hocg.id = ocg.id
    left outer join hierarchy hlng on (
      cc.id = hlng.parentid
      and hlng.primarytype = 'localNameGroup'
      and hlng.pos = 0)
    left outer join localnamegroup lng on hlng.id = lng.id
  where mcc.lifecyclestate <> 'deleted'
    and substring(cc.objectnumber from '^[A-Z]*') != 'DHN'
    and (cucj.posttopublic = 'yes' or cucj.posttopublic is null)
);

----------------------------------------------------------------------------------------------------
-- comments: aggregate comments
-- ~ 716K rows
----------------------------------------------------------------------------------------------------

create temp table tt_comments as (
  select
    o.id,
    string_agg(ccc.item,
      '␥' order by ccc.pos) as comments_ss
  from tt_objects o
  join collectionobjects_common_comments ccc on o.id = ccc.id
  group by o.id
);

----------------------------------------------------------------------------------------------------
-- determs: aggregate determination data for first/current determination
-- taxonomicidentgroup, pos = 0
-- identdategroup for first/current determination (taxonomicidentgroup)
-- taxon for first/current determination (taxonomicidentgroup)
-- taxontermgroup for first/current determination (taxonomicidentgroup)
-- typespecimengroup, pos = 0
-- ~ 117K rows
----------------------------------------------------------------------------------------------------

create temp table tt_determs as (
  select
    o.id,
    regexp_replace(getdispl(tig.taxon), E'[\\t\\n\\r]+', ' ', 'g') as determination_s,
    regexp_replace(ttg.termformatteddisplayname, E'[\\t\\n\\r]+', ' ', 'g') as termformatteddisplayname_s,
    regexp_replace(getdispl(tnh.family), E'[\\t\\n\\r]+', ' ', 'g') as family_s,
    tnh.taxonbasionym as taxonbasionym_s,
    tu.taxonmajorgroup as majorgroup_s,
    case
      when tig.identby like '%unknown%' then ''
      else regexp_replace(coalesce(nullif(getdispl(tig.identby), ''), '')
        || coalesce(', ' || nullif(getdispl(tig.institution), ''), '')
        || coalesce(', ' || nullif(trim(idg.datedisplaydate), ''), '')
        || coalesce(' (' || nullif(tig.identkind, '') || ')', '')
        || coalesce('. ' || nullif(tig.notes, ''), ''), '^,* *', '')
      end as determinationdetails_s,
    case when nullif(tsg.typespecimenbasionym, '') is null then 'no' else 'yes' end as hastypeassertions_s,
    tig.qualifier as determinationqualifier_s
  from tt_objects o
    join hierarchy htig on (
      o.id = htig.parentid
      and htig.primarytype = 'taxonomicIdentGroup'
      and htig.pos = 0)
    join taxonomicidentgroup tig on htig.id = tig.id
    left outer join hierarchy hidg on (
      tig.id = hidg.parentid
      and hidg.name = 'identDateGroup')
    left outer join structureddategroup idg on hidg.id = idg.id
    left outer join taxon_common tc on tig.taxon = tc.refname
    left outer join taxon_ucjeps tu on tc.id = tu.id
    left outer join taxon_naturalhistory tnh on tc.id = tnh.id
    left outer join hierarchy httg on (
      tc.id = httg.parentid
      and httg.primarytype = 'taxonTermGroup'
      and httg.pos = 0)
    left outer join taxontermgroup ttg on httg.id = ttg.id
    left outer join hierarchy htsg on (
      htig.parentid = htsg.parentid
      and htsg.primarytype = 'typeSpecimenGroup'
      and htsg.pos = 0)
    left outer join typespecimengroup tsg on htsg.id = tsg.id
);

----------------------------------------------------------------------------------------------------
-- prevdeterms: aggregate previous determinations for previousdeterminations_ss
-- concatenation of pos > 0 qualifier, taxon, identby, institution, identdate, identkind, notes
-- pos > 0 to exclude first/current determination
-- ~ 185K rows
----------------------------------------------------------------------------------------------------

create temp table tt_prevdeterms as (
  select
    o.id,
    string_agg(  
      coalesce(nullif(tig.qualifier || ' ', ''), '') 
        || case when tig.taxon like '%no name%' then ''
                else (
                  coalesce(getdispl(tig.taxon), '')  
                    || case when tig.identby like '%unknown%' then ''
                            else coalesce(', by ' || nullif(getdispl(tig.identby), ''), '') end
                    || coalesce(', ' || nullif(getdispl(tig.institution), ''), '') 
                    || coalesce(', ' || nullif(trim(idg.datedisplaydate), ''), '') 
                    || coalesce(' (' || nullif(tig.identkind, '') || ')', '') 
                ) end 
        || coalesce('. ' || nullif(tig.notes, ''), ''),
      '␥' order by htig.pos) as previousdeterminations_ss
  from tt_objects o
    join hierarchy htig on (
      o.id = htig.parentid
      and htig.primarytype = 'taxonomicIdentGroup'
      and htig.pos > 0)
    left outer join taxonomicidentgroup tig on htig.id = tig.id
    left outer join hierarchy hidg on (
      tig.id = hidg.parentid
      and hidg.name = 'identDateGroup')
    left outer join structureddategroup idg on hidg.id = idg.id
  group by o.id
);

----------------------------------------------------------------------------------------------------
-- collectors: aggregate all items from collectionobjects_common_fieldcollectors for collector_ss
-- remove any tab, new line, return chars
-- extract first collector from collector_ss in final select for collectorverbatim_s
-- ~ 780K rows
----------------------------------------------------------------------------------------------------

create temp table tt_collectors as (
  select
    o.id,
    string_agg(regexp_replace(getdispl(ccfc.item), E'[\\t\\n\\r]+', ' ', 'g'),
      '␥' order by ccfc.pos) as collector_ss
  from tt_objects o
    join collectionobjects_common_fieldcollectors ccfc on o.id = ccfc.id
  group by o.id
);

----------------------------------------------------------------------------------------------------
-- assoctaxa: aggregate associated taxa for associatedtaxa_ss
-- concatenation of all associatedtaxon, interaction
-- ~ 343K rows
----------------------------------------------------------------------------------------------------

create temp table tt_assoctaxa as (
  select
    o.id,
    string_agg(
      coalesce(nullif(getdispl(atg.associatedtaxon), ''), '')
        || coalesce(' (' || nullif(atg.interaction, '') || ')', ''),
      '␥' order by hatg.pos) as associatedtaxa_ss
  from tt_objects o
    join hierarchy hatg on (
      o.id = hatg.parentid
      and hatg.primarytype = 'associatedTaxaGroup')
    left outer join associatedtaxagroup atg on hatg.id = atg.id
  group by o.id
);

----------------------------------------------------------------------------------------------------
-- types: aggregate type assertions for typeassertions_ss
-- concatenation of typespecimenkind, typespecimenbasionym
-- ~ 341K rows
----------------------------------------------------------------------------------------------------

create temp table tt_types as (
  select
    o.id,
    string_agg(
      coalesce(nullif(tsg.typespecimenkind, ''), '') 
        || coalesce(' (' || nullif(getdispl(tsg.typespecimenbasionym), '') || ')', ''),
      '␥' order by htsg.pos) as typeassertions_ss
  from tt_objects o
    join hierarchy htsg on (
      o.id = htsg.parentid
      and htsg.primarytype = 'typeSpecimenGroup')
    left outer join typespecimengroup tsg on htsg.id = tsg.id
  group by o.id
);

----------------------------------------------------------------------------------------------------
-- othernums: aggregate other numbers for othernumber_ss
-- concatenation all numbervalue, numbertype
-- ~ 933K rows
----------------------------------------------------------------------------------------------------

create temp table tt_othernums as (
  select
    o.id,
    string_agg(
      case
        when ong.numbervalue is null and ong.numbertype is null then null
        else
          coalesce(nullif(ong.numbervalue, ''), '')
            || coalesce(' (' || nullif(getdispl(ong.numbertype), '') || ')', '') end,
      '␥' order by hong.pos) as othernumber_ss
  from tt_objects o
    join hierarchy hong on (
      o.id = hong.parentid
      and hong.primarytype = 'otherNumber')
    left outer join othernumber ong on hong.id = ong.id
    group by o.id
);

----------------------------------------------------------------------------------------------------
-- colldates: get collection dates
-- name = 'collectionobjects_common:fieldCollectionDateGroup'
-- pos are all NULL
-- add 8 hours to scalar dates to fix day offset for hours >= 16
-- hours are
--   0, 4, 5: date is same as scalar date
--   16, 17, 18, 20: date is scalar date + 1
-- ~ 893K rows
----------------------------------------------------------------------------------------------------

create temp table tt_colldates as (
  select
    o.id,
    cdg.datedisplaydate as collectiondate_s,
    to_char(cdg.dateearliestscalarvalue + interval '8 hours', 'YYYY-MM-DD') as earlycollectiondate_dt,
    case
        when cdg.datelatestscalarvalue::date - cdg.dateearliestscalarvalue::date = 1 
            and nullif(cdg.datelatestday, 0) is null
        then to_char(cdg.dateearliestscalarvalue + interval '8 hours', 'YYYY-MM-DD')
        else to_char(cdg.datelatestscalarvalue, 'YYYY-MM-DD')
    end as latecollectiondate_dt
  from tt_objects o
    join hierarchy hcdg on (
      o.id = hcdg.parentid
      and hcdg.name = 'collectionobjects_common:fieldCollectionDateGroup')
    join structureddategroup cdg on hcdg.id = cdg.id
);

----------------------------------------------------------------------------------------------------
-- localities: get localitygroup data for first locality where pos = 0
-- ~ 998K rows
----------------------------------------------------------------------------------------------------

create temp table tt_localities as (
  select
    o.id,
    regexp_replace(lg.fieldlocverbatim,E'[\\t\\n\\r]+', ' ', 'g') as locality_s,
    getdispl(lg.fieldloccounty) as collcounty_s,
    getdispl(lg.fieldlocstate) as collstate_s,
    getdispl(lg.fieldloccountry) as collcountry_s,
    lg.velevation as elevation_s,
    lg.minelevation as minelevation_s,
    lg.maxelevation as maxelevation_s,
    lg.elevationunit as elevationunit_s,
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
    lg.georefencedby as georeferencedby_s,
    lg.vdepth as depth_s,
    lg.mindepth as mindepth_s,
    lg.maxdepth as maxdepth_s,
    lg.depthunit as depthUnit_s
  from tt_objects o
    join hierarchy hlg on (
      o.id = hlg.parentid
      and hlg.primarytype = 'localityGroup'
      and hlg.pos = 0)
    join localitygroup lg on hlg.id = lg.id
);

----------------------------------------------------------------------------------------------------
-- otherlocalities: get other otherlocalities_ss where pos > 0
-- join with localities to exclude objects without localities
-- exclude fieldlocverbatim like '%unknown%') from aggregate
-- for alllocalities_ss, concatenate locality_s and otherlocalities_ss
-- ~308 rows
----------------------------------------------------------------------------------------------------

create temp table tt_otherlocs as (
  select
    l.id,
    string_agg(
      case
        when vlg.fieldlocverbatim like '%unknown%' then ''
        else coalesce(getdispl(vlg.fieldlocverbatim), '') end,
      '␥' order by hvlg.pos) as otherlocalities_ss
  from tt_localities l
    join hierarchy hvlg on (
      l.id = hvlg.parentid
      and hvlg.primarytype = 'localityGroup'
      and hvlg.pos > 0)
    join localitygroup vlg on hvlg.id = vlg.id
  group by l.id
);


----------------------------------------------------------------------------------------------------
-- final select
-- ~1.17M rows
----------------------------------------------------------------------------------------------------

select
  objects.csid as csid_s,
  objects.accessionnumber_s,
  determs.determination_s,
  determs.termformatteddisplayname_s,
  determs.family_s,
  determs.taxonbasionym_s,
  determs.majorgroup_s,
  collectors.collector_ss,
  objects.collectornumber_s,
  colldates.collectiondate_s,
  colldates.earlycollectiondate_dt,
  colldates.latecollectiondate_dt,
  localities.locality_s,
  localities.collcounty_s,
  localities.collstate_s,
  localities.collcountry_s,
  localities.elevation_s,
  localities.minelevation_s,
  localities.maxelevation_s,
  localities.elevationunit_s,
  objects.habitat_s,
  localities.location_0_d,
  localities.location_1_d,
  localities.latlong_p,
  localities.trscoordinates_s,
  localities.datum_s,
  localities.coordinateuncertainty_f,
  localities.coordinateuncertaintyunit_s,
  localities.localitynote_s,
  localities.localitysource_s,
  localities.localitysourcedetail_s,
  localities.georefsource_s,
  localities.georefremarks_s,
  localities.georeferencedby_s,
  objects.updatedat_dt,
  objects.labelheader_s,
  objects.labelfooter_s,
  prevdeterms.previousdeterminations_ss,
  objects.localname_s,
  objects.briefdescription_s,
  localities.depth_s,
  localities.mindepth_s,
  localities.maxdepth_s,
  localities.depthunit_s,
  assoctaxa.associatedtaxa_ss,
  types.typeassertions_ss,
  objects.cultivated_s,
  objects.sex_s,
  objects.phase_s,
  othernums.othernumber_ss,
  null as ucbgaccessionnumber_s,
  determs.determinationdetails_s,
  '' as loanstatus_s,
  '' as loannumber_s,
  split_part(collectors.collector_ss, '␥', 1) as collectorverbatim_s,
  otherlocs.otherlocalities_ss,
  localities.locality_s || coalesce('␥' || otherlocs.otherlocalities_ss, '') as alllocalities_ss,
  determs.hastypeassertions_s,
  determs.determinationqualifier_s,
  comments.comments_ss,
  objects.numberofobjects_s,
  objects.objectcount_s,
  objects.sheet_s,
  objects.createdat_dt,
  objects.posttopublic_s,
  '' as references_ss,
  objects.collectors_verbatim_s
from tt_objects objects
  left outer join tt_comments comments on objects.id = comments.id
  left outer join tt_determs determs on objects.id = determs.id
  left outer join tt_prevdeterms prevdeterms on objects.id = prevdeterms.id
  left outer join tt_collectors collectors on objects.id = collectors.id
  left outer join tt_assoctaxa assoctaxa on objects.id = assoctaxa.id
  left outer join tt_types types on objects.id = types.id
  left outer join tt_othernums othernums on objects.id = othernums.id
  left outer join tt_colldates colldates on objects.id = colldates.id
  left outer join tt_localities localities on objects.id = localities.id
  left outer join tt_otherlocs otherlocs on objects.id = otherlocs.id;
