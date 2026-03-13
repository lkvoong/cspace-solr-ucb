----------------------------------------------------------------------------------------------------
-- botgardenMetadataV1alive.sql
-- all objects, not deleted, not dead
-- exclude deleted collectionobjects
-- get deadflag = 'false' for not dead, i.e. alive accessions only
-- deadflag is varchar (not boolean) and values include ('false', 'true', 'TRUE', NULL)
-- fruits are boolean
-- flowers are varchar, i.e. can have nulls
-- rare is varchar, not boolean
-- ~15065 rows
----------------------------------------------------------------------------------------------------

with objects as (
  select
    hcc.name as objectcsid,
    cc.id as objectid,
    cc.objectnumber as accessionNumber_s,
    cc.fieldcollectionnumber as collectorNumber_s,
    cc.fieldcollectionnote as habitat_s,
    cc.recordstatus as dataQuality_s,
    nullif(cc.sex, '') as sex_s,
    'no' as deadFlag_s,
    null as deadDate_s,
    cb.flowercolor as flowerColor_s,
    concat_ws('|',
      cb.fruitsjan, cb.fruitsfeb, cb.fruitsmar, cb.fruitsapr, cb.fruitsmay, cb.fruitsjun,
      cb.fruitsjul, cb.fruitsaug, cb.fruitssep, cb.fruitsoct, cb.fruitsnov, cb.fruitsdec) as fruiting_ss,
    concat_ws('|',
      coalesce(cb.flowersjan, ''), coalesce(cb.flowersfeb, ''), coalesce(cb.flowersmar, ''),
      coalesce(cb.flowersapr, ''), coalesce(cb.flowersmay, ''), coalesce(cb.flowersjun, ''),
      coalesce(cb.flowersjul, ''), coalesce(cb.flowersaug, ''), coalesce(cb.flowerssep, ''),
      coalesce(cb.flowersoct, ''), coalesce(cb.flowersnov, ''), coalesce(cb.flowersdec, '')) as flowering_ss,
    case when (cn.rare = 'true') then 'yes' else 'no' end as rare_s,
    cn.provenancetype as provenanceType_s,
    left(cn.provenancetype, 1) as provenanceType_short_s,
    regexp_replace(cn.source , E'[\\t\\n\\r]+', ' ', 'g') as source_s,    -- remove tabs, CRLF
    nullif(ccbd.item, '') as materialType_s,
    regexp_replace(ccc.item, E'[\\t\\n\\r]+', ' ', 'g') as accessionNotes_s, -- remove tabs, CRLF
    getdispl(ccfc.item) as collector_s,
    cdg.datedisplaydate as collectionDate_s,
    to_char(cdg.dateearliestscalarvalue, 'YYYY-MM-DD') as earlyCollectionDate_s,
    to_char(cdg.datelatestscalarvalue, 'YYYY-MM-DD') as lateCollectionDate_s,
    lg.fieldlocverbatim as fcpVerbatim_s,
    lg.fieldloccounty as collCounty_ss,
    lg.fieldlocstate as collState_ss,
    lg.fieldloccountry as collCountry_ss,
    lg.velevation as elevation_s,
    lg.minelevation as minElevation_s,
    lg.maxelevation as maxElevation_s,
    lg.elevationunit as elevationUnit_s,
    lg.decimallatitude as latitude_f,
    lg.decimallongitude as longitude_f,
    coalesce(lg.decimallatitude::text, '') || coalesce(',' || lg.decimallongitude::text, '') as latLong_p,
    case when (lg.vcoordsys like 'Township%') then lg.vcoordinates end as trsCoordinates_s,
    lg.geodeticdatum as datum_s,
    lg.localitysource as coordinateSource_s,
    lg.coorduncertainty as coordinateUncertainty_s,
    lg.coorduncertaintyunit as coordinateUncertaintyUnit_s,
    coalesce(nullif(getdispl(lg.fieldlocplace), ''), 'Geographic range: ' || nullif(lg.taxonomicrange, '')) as locality_s
  from collectionobjects_common cc
    join misc mcc on cc.id = mcc.id
    join collectionobjects_botgarden cb on cc.id = cb.id
    join collectionobjects_naturalhistory cn on cc.id = cn.id
    join hierarchy hcc on cc.id = hcc.id
    left outer join collectionobjects_common_briefdescriptions ccbd on (
      cc.id = ccbd.id
      and ccbd.pos = 0)    -- first description only
    left outer join collectionobjects_common_comments ccc on (
      cc.id = ccc.id
      and ccc.pos = 0)    -- first comment only
    left outer join collectionobjects_common_fieldcollectors ccfc on (
      cc.id = ccfc.id
      and ccfc.pos = 0)    -- first collector only
    left outer join hierarchy hcdg on (
      cc.id = hcdg.parentid
      and hcdg.name = 'collectionobjects_common:fieldCollectionDateGroup')    -- collectionDates have no pos
    left outer join structureddategroup cdg on hcdg.id = cdg.id
    left outer join hierarchy hlg on (
      cc.id = hlg.parentid
      and hlg.name = 'collectionobjects_naturalhistory:localityGroupList'
      and hlg.pos = 0)    -- first locality only
    left outer join localitygroup lg on lg.id = hlg.id
  where mcc.lifecyclestate != 'deleted'
    and cb.deadflag = 'false'
),

----------------------------------------------------------------------------------------------------
-- aggregate distinct group titles from groups_common for objects
-- joins to objects CTE
-- exclude deleted CollectionObject/Group relations and deleted groups
-- get distinct groups because objectcsid:subjectcsid duplicates exist in relations_common
-- e.g. objectcsid = 'b64b2687-1520-4f34-90f7-4d0939952665'
-- ~4294 rows
----------------------------------------------------------------------------------------------------

groups as (
  select
    objects.objectcsid,
    string_agg(distinct nullif(trim(gc.title), ''), '|' order by nullif(trim(gc.title), '')) as grouptitle_ss
  from objects
    join relations_common rcg on (
      objects.objectcsid = rcg.objectcsid
      and rcg.objectdocumenttype = 'CollectionObject'
      and rcg.subjectdocumenttype = 'Group')
    join misc mrcg on rcg.id = mrcg.id
    join hierarchy hgc on (
      rcg.subjectcsid = hgc.name
      and hgc.primarytype = 'Group')
    join groups_common gc on hgc.id = gc.id
    join misc mgc on gc.id = mgc.id
  where mrcg.lifecyclestate != 'deleted'
    and mgc.lifecyclestate != 'deleted'
  group by objects.objectcsid
),

----------------------------------------------------------------------------------------------------
-- aggregate voucher info from loansout_common for objects
-- joins to objects CTE
-- exclude deleted CollectionObject/Loanout relations and deleted loansout
-- ~4929 rows
----------------------------------------------------------------------------------------------------

vouchers as (
  select
    objects.objectcsid,
    string_agg(
      nullif(getdispl(lc.borrower), '') || coalesce(', ' || to_char(lc.loanoutdate, 'YYYY-MM-DD'), ''),
      '|' order by lc.loanoutdate, getdispl(lc.borrower)) as voucherinfo
  from objects
    join relations_common rcl on (
      objects.objectcsid = rcl.objectcsid
      and rcl.objectdocumenttype = 'CollectionObject'
      and rcl.subjectdocumenttype = 'Loanout')
    join misc mrcl on rcl.id = mrcl.id
    join hierarchy hlc on (
      rcl.subjectcsid = hlc.name
      and hlc.primarytype = 'LoanoutTenant35')
    join loansout_common lc on hlc.id = lc.id
    join misc mlc on lc.id = mlc.id
  where mrcl.lifecyclestate != 'deleted'
   and mlc.lifecyclestate != 'deleted'
  group by objects.objectcsid
),

----------------------------------------------------------------------------------------------------
-- current location and reason for move from movements_common for objects
-- joins to objects CTE
-- exclude deleted CollectionObject/Movement relations and deleted movements
-- get isversion is not true for current location
-- more rows than objects because webapps list each alive location as a separate object record
-- ~20173 rows
----------------------------------------------------------------------------------------------------

movements as (
  select
    objects.objectcsid,
    getdispl(mc.currentlocation) as gardenLocation_s,
    getdispl(mc.reasonformove) as reasonForMove_s
  from objects
    join relations_common rmc on (
      objects.objectcsid = rmc.objectcsid
      and rmc.objectdocumenttype = 'CollectionObject'
      and rmc.subjectdocumenttype = 'Movement')
    join misc mrmc on rmc.id = mrmc.id
    join hierarchy hmc on rmc.subjectcsid = hmc.name
    join movements_common mc on hmc.id = mc.id
    join misc mmc on mc.id = mmc.id
  where  mrmc.lifecyclestate != 'deleted'
    and mmc.lifecyclestate != 'deleted'
    and hmc.isversion is not true
),

----------------------------------------------------------------------------------------------------
-- current determination from taxonomicIdentGroup for objects
-- joins to objects CTE
-- pos = 0 to get current taxonomicIdentGroup
-- ~15065 rows
----------------------------------------------------------------------------------------------------

determs as (
  select
    objects.objectid,
    tig.id as tigid,
    case when (tig.hybridflag = 'true') then 'yes' else 'no' end as hybridFlag_s,
    findhybridaffinname(tig.id) as determination_s,
    tig.taxon,
    getdispl(tig.taxon) as determ0
  from objects
  join hierarchy htig on (
    objects.objectid = htig.parentid
    and htig.pos = 0
    and htig.name = 'collectionobjects_naturalhistory:taxonomicIdentGroupList')
  join taxonomicIdentGroup tig on htig.id = tig.id
),

----------------------------------------------------------------------------------------------------
-- aggregate previous determinations from taxonomicIdentGroup and structuredDateGroup for objects
-- joins to determs CTE
-- pos > 0 to get previous determinations
-- removed check for taxon like '%no name%' as value does not exist in data
-- concatenate determs.determ0 with prevDeterms for alldeterminations_ss
-- ~3618 rows in qa
----------------------------------------------------------------------------------------------------

aggdeterms as (
  select
    determs.objectid,
    string_agg(
      coalesce(getdispl(tig.taxon), ''),
      '␥' order by htig.pos) as prevdeterms,
    string_agg(
      coalesce(
        coalesce(nullif(tig.qualifier, '') || ' ', '')
          || coalesce(getdispl(tig.taxon), '')
          || coalesce(', by ' || nullif(getdispl(tig.identby), ''), '')
          || coalesce(', ' || nullif(getdispl(tig.institution), ''), '')
          || coalesce(', ' || nullif(trim(sdg.datedisplaydate), ''), '')
          || coalesce(' (' || nullif(tig.identkind, '') || ')', '')
          || '.',
        '')
        || coalesce(' ' || nullif(tig.notes, ''), ''),
        '␥' order by htig.pos) as previousDeterminations_ss
  from determs
    join hierarchy htig on (
      determs.objectid = htig.parentid
      and htig.pos > 0
      and htig.name = 'collectionobjects_naturalhistory:taxonomicIdentGroupList')
    join taxonomicIdentGroup tig on htig.id = tig.id
    left outer join hierarchy hidg on (
      tig.id = hidg.parentid
      and hidg.name = 'identDateGroup')
    left outer join structureddategroup sdg on hidg.id = sdg.id
  group by determs.objectid
),

----------------------------------------------------------------------------------------------------
-- distinct taxon names from taxonomicIdentGroup for objects
-- joins to determs CTE
-- ~9989 rows
----------------------------------------------------------------------------------------------------

dtaxon as (
  select distinct taxon
  from determs
),

----------------------------------------------------------------------------------------------------
-- determination data from taxon_common, taxon_naturalhistory for taxon
-- joins to dtaxon CTE
-- ~9986 rows
----------------------------------------------------------------------------------------------------

taxon as (
  select
    tc.id as taxonid,
    tc.refname as taxonrefname,
    tn.accessrestrictions as accessrestrictions_s,
    case when (tc.taxonisnamedhybrid = 'true') then 'yes' else 'no' end as taxonIsNamedHybrid_s,
    findparentbyrank(tc.id, 'division') as division_s,
    findparentbyrank(tc.id, 'order') as order_s,
    getdispl(tn.family) as family_s,
    getdispl(cng.naturalhistorycommonname) as commonname_s,
    getdispl(pag.habitat) as habit_s
  from dtaxon
    join taxon_common tc on dtaxon.taxon = tc.refname
    left outer join taxon_naturalhistory tn on tc.id = tn.id
    left outer join hierarchy h on (
      tc.id = h.parentid
      and h.pos = 0    -- first common name only
      and h.name = 'taxon_naturalhistory:naturalHistoryCommonNameGroupList')
    left outer join naturalhistorycommonnamegroup cng on cng.id = h.id
    left outer join hierarchy hpag on (
      tc.id = hpag.parentid
      and hpag.pos = 0    -- first plant attribute only
      and hpag.name = 'taxon_naturalhistory:plantAttributesGroupList')
    left outer join plantattributesgroup pag on pag.id = hpag.id
),

----------------------------------------------------------------------------------------------------
-- aggregate conservation data from plantAttributesGroup for taxon
-- joins to taxon CTE
-- exclude category = 'none' and organization = 'not applicable'
-- ~1500 rows
----------------------------------------------------------------------------------------------------

conservation as (
  select
    taxon.taxonid,
    string_agg(
      nullif(getdispl(pag.conservationcategory), ''),
      '|' order by htc.pos) as conservecat_ss,
    string_agg(
      nullif(getdispl(pag.conservationorganization), '') || ': ' || nullif(getdispl(pag.conservationcategory), ''),
      '|' order by htc.pos) as conservationinfo_ss,
    string_agg(
      nullif(getdispl(pag.conservationorganization), ''),
      '|' order by htc.pos) as conserveorg_ss
  from taxon
    join hierarchy htc on (
      taxon.taxonid = htc.parentid
      and htc.name = 'taxon_naturalhistory:plantAttributesGroupList')
    left outer join plantattributesgroup pag on pag.id = htc.id
  where pag.conservationcategory not like '%none%'
    and pag.conservationorganization not like '%not applicable%'
  group by taxon.taxonid
)

----------------------------------------------------------------------------------------------------
-- final select
----------------------------------------------------------------------------------------------------

select
  objects.objectid as id,
  objects.accessionNumber_s,
  determs.determination_s,
  objects.collector_s,
  objects.collectorNumber_s,
  objects.collectionDate_s,
  objects.earlyCollectionDate_s,
  objects.lateCollectionDate_s,
  objects.fcpVerbatim_s,
  objects.collCounty_ss,
  objects.collState_ss,
  objects.collCountry_ss,
  objects.elevation_s,
  objects.minElevation_s,
  objects.maxElevation_s,
  objects.elevationUnit_s,
  objects.habitat_s,
  objects.latLong_p,
  objects.trsCoordinates_s,
  objects.datum_s,
  objects.coordinateSource_s,
  objects.coordinateUncertainty_s,
  objects.coordinateUncertaintyUnit_s,
  taxon.family_s,
  movements.gardenLocation_s,
  objects.dataQuality_s,
  objects.locality_s,
  objects.objectcsid as csid_s,
  objects.rare_s,
  objects.deadFlag_s,
  objects.flowerColor_s,
  '' as determinationNoAuth_s,
  movements.reasonForMove_s,
  conservation.conservationInfo_ss,
  conservation.conserveOrg_ss,
  conservation.conserveCat_ss,
  case when (nullif(vouchers.voucherinfo, '') is null) then 'no' else 'yes' end as vouchers_s,
  '1' as vouchercount_s,
  nullif(vouchers.voucherinfo, '') as voucherlist_ss,
  objects.fruiting_ss as fruitingVerbatim_ss,
  objects.flowering_ss as floweringVerbatim_ss,
  objects.fruiting_ss,
  objects.flowering_ss,
  objects.provenanceType_s,
  taxon.accessRestrictions_s,
  objects.accessionNotes_s,
  taxon.commonName_s,
  objects.source_s,
  objects.latitude_f,
  objects.longitude_f,
  '' as researcher_s,
  groups.groupTitle_ss,
  determs.hybridFlag_s,
  taxon.taxonIsNamedHybrid_s,
  aggdeterms.previousDeterminations_ss,
  determs.determ0 || coalesce('␥' || aggdeterms.prevdeterms, '') as allDeterminations_ss,
  taxon.habit_s,
  objects.materialType_s,
  objects.sex_s,
  objects.provenanceType_short_s,
  taxon.division_s,
  taxon.order_s,
  objects.deadDate_s

from objects
  left outer join groups on objects.objectcsid = groups.objectcsid
  left outer join vouchers on objects.objectcsid = vouchers.objectcsid
  left outer join movements on objects.objectcsid = movements.objectcsid
  left outer join determs on objects.objectid = determs.objectid
  left outer join aggdeterms on objects.objectid = aggdeterms.objectid
  left outer join taxon on determs.taxon = taxon.taxonrefname
  left outer join conservation on taxon.taxonid = conservation.taxonid;
