-- all objects, not deleted, not dead
with objects as (
  select 
    hcc.name as objectcsid,
    cc.id as objectid,
    cc.objectnumber,
    cc.fieldcollectionnumber,
    cc.fieldcollectionnote,
    cc.recordstatus,
    nullif(cc.sex, '') as sex,
    case when (cb.deadflag = 'true') then 'yes' else 'no' end as deadflag,
    to_char(cb.deaddate, 'YYYY-MM-DD') as deaddate,
    cb.flowercolor,

    concat_ws('|',    -- fruits are boolean
      cb.fruitsjan, cb.fruitsfeb, cb.fruitsmar, cb.fruitsapr, cb.fruitsmay, cb.fruitsjun,
      cb.fruitsjul, cb.fruitsaug, cb.fruitssep, cb.fruitsoct, cb.fruitsnov, cb.fruitsdec) fruiting,

    concat_ws('|',    -- flowers are varchar, i.e. can have nulls
      coalesce(cb.flowersjan, ''), coalesce(cb.flowersfeb, ''), coalesce(cb.flowersmar, ''),
      coalesce(cb.flowersapr, ''), coalesce(cb.flowersmay, ''), coalesce(cb.flowersjun, ''),
      coalesce(cb.flowersjul, ''), coalesce(cb.flowersaug, ''), coalesce(cb.flowerssep, ''),
      coalesce(cb.flowersoct, ''), coalesce(cb.flowersnov, ''), coalesce(cb.flowersdec, '') as flowering,
    case when (cn.rare = 'true') then 'yes' else 'no' end as rareflag,    -- cn.rare is varchar, not boolean
    cn.provenancetype as provenancetype,
    left(cn.provenancetype, 1) as provenancetype_short,
    regexp_replace(cn.source , E'[\\t\\n\\r]+', ' ', 'g') as source,    -- remove tabs, newlines, carriage returns
    nullif(ccbd.item, '') as materialtype,
    regexp_replace(ccc.item, E'[\\t\\n\\r]+', ' ', 'g') as accessionnotes,    -- remove tabs, newlines, carriage returns
    getdispl(ccfc.item) as collector,
    cdg.datedisplaydate as collectiondate,
    to_char(cdg.dateearliestscalarvalue, 'YYYY-MM-DD') as collectionearlydate,
    to_char(cdg.datelatestscalarvalue, 'YYYY-MM-DD') as collectionlatedate,
    lg.fieldlocverbatim,
    lg.fieldloccounty,
    lg.fieldlocstate,
    lg.fieldloccountry,
    lg.velevation,
    lg.minelevation,
    lg.maxelevation,
    lg.elevationunit,
    coalesce(lg.decimallatitude, '') || coalesce(',' || lg.decimallongitude, '') as latlong,
    case when (lg.vcoordsys like 'Township%') then lg.vcoordinates end as trscoordinates,
    lg.geodeticdatum,
    lg.localitysource,
    lg.coorduncertainty,
    lg.coorduncertaintyunit,
    coalesce(nullif(getdispl(lg.fieldlocplace), ''), 'Geographic range: ' || nullif(lg.taxonomicrange, '')) as locality
  from collectionobjects_common cc
  join misc mcc on (
    cc.id = mcc.id
    and mcc.lifecyclestate != 'deleted')    -- exclude deleted collectionobjects
  join collectionobjects_botgarden cb on (
    cc.id = cb.id
    and cb.deadflag = 'false')    -- not dead, i.e. alive accessions only
  join collectionobjects_naturalhistory cn on (cc.id = cn.id)
  join hierarchy hcc on (cc.id = hcc.id)
  left outer join collectionobjects_common_briefdescriptions ccbd on (
    cc.id = ccbd.id
    and ccbd.pos = 0)    -- first description only
  left outer join collectionobjects_common_comments ccc on (
    cc.id = ccc.id
    and cc.pos = 0)    -- first comment only
  left outer join collectionobjects_common_fieldcollectors ccfc on (
    cc.id = ccfc.id
    and ccfc.pos = 0)    -- first collector only
  left outer join hierarchy hcdg on (
    cc.id = hcdg.parentid
    and hcdg.name = 'collectionobjects_common:fieldCollectionDateGroup')    -- collectionDates have no pos
  left outer join structureddategroup cdg on (hcdg.id = cdg.id)
  left outer join hierarchy hlg on (
    cc.id = hlg.parentid
    and hlg.pos = 0    -- first locality only
    and hlg.name = 'collectionobjects_naturalhistory:localityGroupList')
  left outer join localitygroup lg on (lg.id = hlg.id)
),

-- aggregate distinct group titles from groups_common for objects
-- objectcsid:subjectcsid duplicates exist in relations_common
-- objectcsid = 'b64b2687-1520-4f34-90f7-4d0939952665'
-- joins to objects CTE
groups as (
  select
    objects.objectcsid,
    string_agg(distinct nullif(trim(gc.title), ''), '|' order by trim(gc.title)) as grouptitles
  from objects
  join relations_common rgc on (
    objects.objectcsid = rgc.objectcsid
    and rgc.objectdocumenttype = 'CollectionObject'
    and rgc.subjectdocumenttype = 'Group')
  join misc mrgc on (
    rgc.id = mrgc.id
    and mrgc.lifecyclestate != 'deleted')    -- exclude deleted CollectionObject/Group relations
  join hierarchy hgc on (
    rgc.subjectcsid = hgc.name
    and hgc.primarytype = 'Group')
  join groups_common gc on (hgc.id = gc.id)
  join misc mgc on (
    gc.id = mgc.id
    and mgc.lifecyclestate != 'deleted')    -- exclude deleted groups
  group by objects.objectcsid
),

-- aggregate voucher info from loansout_common for objects
-- no objectcsid:subjectcsid duplicates in relations_common
-- objectcsid = '00ddf444-303a-47e4-9e61-016c8c8e59a5'
-- joins to objects CTE
vouchers as (
  select
    objects.objectcsid,
    string_agg(
      nullif(getdispl(loc.borrower), '') || coalesce(', ' || to_char(loc.loanoutdate, 'YYYY-MM-DD'), ''),
      '|' order by loc.loanoutdate, getdispl(loc.borrower)) as voucherinfo
  from objects
  join relations_common rloc on (
    objects.objectcsid = rloc.objectcsid
    and rloc.objectdocumenttype = 'CollectionObject'
    and rloc.subjectdocumenttype = 'Loanout')
  join misc mrloc on (
    rloc.id = mrloc.id
    and mrloc.lifecyclestate != 'deleted')    -- exclude deleted CollectionObject/Loanout relations
  join hierarchy hloc on (
    rloc.subjectcsid = hloc.name
    and hloc.primarytype = 'LoanoutTenant35')
  join loansout_common loc on (hloc.id = loc.id)
  join misc mloc on (loc.id = mloc.id
    and mloc.lifecyclestate != 'deleted')    -- exclude deleted loansout
  group by objects.objectcsid
),

-- current locaiton and reason for move from movements_common for objects
-- joins to objects CTE 
movements as (
  select
    objects.objectcsid,
    getdispl(mc.currentlocation) as gardenlocation,
    getdispl(mc.reasonformove) as reasonformove
  from objects
  join relations_common rmc on (
    objects.objectid = rmc.objectcsid
    and rmc.objectdocumenttype = 'CollectionObject'
    and rmc.subjectdocumenttype = 'Movement')
  join misc mrmc on (
    rmc.id = mrmc.id
    and mrmc.lifecyclestate != 'deleted')    -- exclude deleted CollectionObject/Movement relations
  join hierarchy hmc on (
    rmc.subjectcsid = hmc.name
    and hmc.isversion is not true)    -- get current location
  join movements_common mc on (hmc.id = mc.id)
  join misc mmc on (
    mc.id = mmc.id
    and mmc.lifecyclestate != 'deleted')    -- exclude deleted movements
),

-- current determination from taxonomicIdentGroup for objects
-- joins to objects CTE
determs as (
  select 
    objects.objectid,
    tig.id as tigid,
    htig.pos,
    tig.taxon,
    tig.qualifier,
    tig.identby,
    tig.institution,
    tig.identkind,
    case when (tig.hybridflag = 'true') then 'yes' else 'no' end as hybridflag,
    findhybridaffinname(tig.id) as determination
  from objects
  join hierarchy htig on (
    objects.objectid = htig.parentid
    and htig.pos = 0    -- get current taxonomicIdentGroup
    and htig.name = 'collectionobjects_naturalhistory:taxonomicIdentGroupList')
  join taxonomicIdentGroup tig on (htig.id = tig.id)
),

-- aggregate determinations from taxonomicIdentGroup and structuredDateGroup for objects
-- objectcid = 'ff218a66-b0f3-4fd7-a67a-7d7e7d90f791'
-- joins to determs CTE
aggdeterms as (
  select
    determs.objectid,
    string_agg(
      coalesce(getdispl(determs.taxon), ''),
      '␥' order by determs.pos) as alldeterminations,
    regexp_replace(
      string_agg(
        coalesce(
          coalesce(nullif(determs.qualifier, '') || ' ', '')
            || coalesce(getdispl(determs.taxon), '')
            || coalesce(', by ' || nullif(getdispl(determs.identby), ''), '')
            || coalesce(', ' || nullif(getdispl(determs.institution), ''), '')
            || coalesce(', ' || nullif(trim(sdg.datedisplaydate), ''), '')
            || coalesce(' (' || nullif(determs.identkind, '') || ')', '')
            || '.',
          '')
          || coalesce(' ' || nullif(determs.notes, ''), ''),
        '␥' order by determs.pos),
      '(^[^␥]+␥?)', '') as prevdeterminations    -- regexp_replace to remove lowest pos determination
  from determs
  left outer join hierarchy hidg on (
    determs.tigid = hidg.parentid
    and hidg.name = 'identDateGroup') 
  left outer join structureddategroup sdg on (hidg.id = sdg.id)
  group by objects.objectid
),

-- distinct taxon names from taxonomicIdentGroup for objects
-- joins to determs CTE
dtaxon as (
  select distinct taxon
  from determs
),

-- determination data from taxon_common, taxon_naturalhistory for taxon
-- joins to dtaxon ( CTE
taxon as (
  select
    tc.id as taxonid,
    tc.refname as taxonrefname,
    getdispl(tc.refname) as taxonname,
    tn.accessrestrictions,
    case when (tc.taxonisnamedhybrid = 'true') then 'yes' else 'no' end as taxonisnamedhybrid,
    findparentbyrank(tc.id, 'division') as division,
    findparentbyrank(tc.id, 'order') as order,
    getdispl(tn.family) as family,
    getdispl(cng.naturalhistorycommonname) as commonname,
    getdispl(pag.habitat) as habitat
  from dtaxon
  join taxon_common tc on (dtaxon.taxon = tc.refname)
  left outer join taxon_naturalhistory tn on (tc.id = tn.id)
  left outer join hierarchy h on (
    tc.id = h.parentid
    and h.pos = 0    -- first common name only
    and h.name = 'taxon_naturalhistory:naturalHistoryCommonNameGroupList')
  left outer join naturalhistorycommonnamegroup cng on (cng.id = h.id)
  left outer join hierarchy hpag
        on (tc.id = hpag.parentid
        and hpag.pos = 0    -- first plant attribute only
        and hpag.name = 'taxon_naturalhistory:plantAttributesGroupList')
  left outer join plantattributesgroup pag on (pag.id = hpag.id)
),

-- aggregate conservation data from plantAttributesGroup for taxon
-- joins to taxon CTE
conservation as (
  select
    tc.id as taxonid,
    string_agg(
      nullif((getdispl(pag.conservationcategory), ''),
      '|' order by h.pos) as conservationcat,
    string_agg(
      nullif(getdispl(pag.conservationorganization), '') || ': ' || nullif(getdispl(pag.conservationcategory), ''),
      '|' order by h.pos) as conservationinfo,
    string_agg(
      nullif(getdispl(pag.conservationorganization), ''),
      '|' order by h.pos) as conservationorg
  from taxon
  join hierarchy htc on (
    taxon.taxonid = htc.parentid
    and htc.name = 'taxon_naturalhistory:plantAttributesGroupList')
  left outer join plantattributesgroup pag on (pag.id = htc.id)
  where pag.conservationcategory not like '%none%'    -- exclude category = 'none'
  and pag.conservationorganization not like '%not applicable%'    -- exclude organization = 'not applicable'
  group by tc.id
)

select
  objects.objectid as id,
  objects.objectnumber as AccessionNumber_s,
  determs.determination as Determination_s, 
  objects.collector as Collector_s,
  objects.collectornumber as CollectorNumber_s,
  objects.collectiondate as CollectionDate_s,
  objects.collectionearlydate as EarlyCollectionDate_s,
  objects.collectionlateDate as LateCollectionDate_s,
  objects.fieldlocverbatim as fcpverbatim_s,
  objects.fieldloccounty as CollCounty_ss,
  objects.fieldlocstate as CollState_ss,
  objects.fieldloccountry as CollCountry_ss,
  objects.velevation as Elevation_s,
  objects.minelevation as MinElevation_s,
  objects.maxelevation as MaxElevation_s,
  objects.elevationunit as ElevationUnit_s,
  objects.fieldcollectionnote as Habitat_s,
  objects.latlong as latlong_p,
  objects.trscoordinates as TRSCoordinates_s,
  objects.geodeticdatum as Datum_s,
  objects.localitysource as CoordinateSource_s,
  objects.coorduncertainty as CoordinateUncertainty_s,
  objects.coorduncertaintyunit as CoordinateUncertaintyUnit_s,
  taxon.family as family_s,
  movements.gardenlocation as gardenlocation_s,
  objects.recordstatus dataQuality_s,
  objects.locality as locality_s,
  objects.objectcsid as csid_s,
  objects.rareflag as rare_s,
  objects.deadflag as deadflag_s,
  objects.flowercolor as flowercolor_s,
  '' as determinationNoAuth_s,
  movements.reasonformove as reasonformove_s,
  conservation.conservationinfo as conservationinfo_ss,
  conservation.conservationorg as conserveorg_ss,
  conservation.conservationcat as conservecat_ss,
  case when (nullif(vouchers.voucherinfo, '') is null) then 'no' else 'yes' end as vouchers_s,
  '1' as vouchercount_s,
  nullif(vouchers.voucherinfo, '') as voucherlist_ss,
  objects.fruiting as fruitingverbatim_ss,
  objects.flowering as floweringverbatim_ss,
  objects.fruiting  as fruiting_ss,
  objects.flowering as flowering_ss,
  objects.provenancetype as provenancetype_s,
  objects.accessrestrictions as accessrestrictions_s,
  objects.accessionnotes as accessionnotes_s,
  taxon.commonname as commonname_s,
  objects.source as source_s,
  objects.decimallatitude as latitude_f,
  objects.decimallongitude as longitude_f,
  '' as researcher_s,
  groups.grouptitles as grouptitle_ss,
  determs.hybridflag as hybridflag_s,
  taxon.taxonisnamedhybrid as taxonisnamedhybrid_s,
  aggdeterms.prevdeterminations as previousdeterminations_ss,
  aggdeterms.alldeterminations as previousdeterminations_ss,
  taxon.habitat as habit_s,
  objects.materialtype as materialtype_s,
  objects.sex as sex_s,
  objects.provenancetype_short as provenancetype_short_s,
  objects.division as division_s,
  objects.order as order_s,
  objects.deaddate as deaddate_s

from objects
left outer join groups on (objects.objectcsid = groups.objectcsid)
left outer join vouchers on (objects.objectcsid = vouchers.objectcsid)
left outer join movements on (objects.objectcsid = movements.objectcsid)
left outer join determs on (objects.objectid = determs.objectid)
left outer join aggdeterms on (objects.objectid = aggdeterms.objectid)
left outer join taxon on (determs.taxon = taxon.taxonrefname)
left outer join conservation on (taxon.taxonid = conservation.taxonid)
