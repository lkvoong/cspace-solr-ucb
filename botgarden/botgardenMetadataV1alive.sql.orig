select
    co.id as id,
    co.objectnumber as AccessionNumber_s,
    findhybridaffinname(tig.id) as Determination_s,
    regexp_replace(fc.item, '^.*\)''(.*)''$', '\1') as Collector_s,
    co.fieldcollectionnumber as CollectorNumber_s,
    sdg.datedisplaydate as CollectionDate_s,
    to_char(sdg.dateearliestscalarvalue, 'YYYY-MM-DD') as EarlyCollectionDate_s,
    to_char(sdg.datelatestscalarvalue, 'YYYY-MM-DD') as LateCollectionDate_s,
    lg.fieldlocverbatim as fcpverbatim_s,
    lg.fieldloccounty as CollCounty_ss,
-- adding state and country
    lg.fieldlocstate as CollState_ss,
    lg.fieldloccountry as CollCountry_ss,
    lg.velevation as Elevation_s,
    lg.minelevation as MinElevation_s,
    lg.maxelevation as MaxElevation_s,
    lg.elevationunit as ElevationUnit_s,
    co.fieldcollectionnote as Habitat_s,
    lg.decimallatitude || ',' || lg.decimallongitude as latlong_p,
    case when lg.vcoordsys like 'Township%' then lg.vcoordinates end as TRSCoordinates_s,
    lg.geodeticdatum as Datum_s,
    lg.localitysource as CoordinateSource_s,
    lg.coorduncertainty as CoordinateUncertainty_s,
    lg.coorduncertaintyunit as CoordinateUncertaintyUnit_s,

    regexp_replace(tn.family, '^.*\)''(.*)''$', '\1') as family_s,
    regexp_replace(mc.currentlocation, '^.*\)''(.*)''$', '\1') as gardenlocation_s,
co.recordstatus dataQuality_s,
case when (lg.fieldlocplace is not null and lg.fieldlocplace <> '') then regexp_replace(lg.fieldlocplace, '^.*\)''(.*)''$', '\1')
     when (lg.fieldlocplace is null and lg.taxonomicrange is not null) then 'Geographic range: '||lg.taxonomicrange
end as locality_s,
h1.name as csid_s,
case when (con.rare = 'true') then 'yes' else 'no' end as rare_s,
case when (cob.deadflag = 'true') then 'yes' else 'no' end as deadflag_s,
cob.flowercolor as flowercolor_s,
'' as determinationNoAuth_s,
-- regexp_replace(tig2.taxon, '^.*\)''(.*)''$', '\1') as determinationNoAuth_s,
regexp_replace(mc.reasonformove, '^.*\)''(.*)''$', '\1') as reasonformove_s,

cons.conserveinfo as conservationinfo_ss,
cons.conserveorg as conserveorg_ss,
cons.conservecat as conservecat_ss,

case when (utils.findvoucherinfo(h1.name) is not null)
     then 'yes' else 'no'
end as vouchers_s,
-- vouchercount is set further on in the process
'1' as vouchercount_s,
utils.findvoucherinfo(h1.name) voucherlist_ss,

-- very complicated logic here! these two fields (fruiting and flowering) have different values and one
-- has Nulls and the other doesn't. nevertheless, in both cases we need a pipe-delimited string with 12 values...
-- what to say? "lasciate ogni speranza..."
concat_ws('|', fruitsjan,fruitsfeb,fruitsmar,fruitsapr,fruitsmay,fruitsjun,fruitsjul,fruitsaug,fruitssep,fruitsoct,fruitsnov,fruitsdec) fruitingverbatim_ss,
concat(flowersjan,'|',flowersfeb,'|',flowersmar,'|',flowersapr,'|',flowersmay,'|',flowersjun,'|',flowersjul,'|',flowersaug,'|',flowerssep,'|',flowersoct,'|',flowersnov,'|',flowersdec) as floweringverbatim_ss,

concat_ws('|', fruitsjan,fruitsfeb,fruitsmar,fruitsapr,fruitsmay,fruitsjun,fruitsjul,fruitsaug,fruitssep,fruitsoct,fruitsnov,fruitsdec) fruiting_ss,
concat(flowersjan,'|',flowersfeb,'|',flowersmar,'|',flowersapr,'|',flowersmay,'|',flowersjun,'|',flowersjul,'|',flowersaug,'|',flowerssep,'|',flowersoct,'|',flowersnov,'|',flowersdec) as flowering_ss,

con.provenancetype as provenancetype_s,
tn.accessrestrictions as accessrestrictions_s,
regexp_replace(coc.item, E'[\\t\\n\\r]+', ' ', 'g') as accessionnotes_s,
findcommonname(tig.taxon) as commonname_s,
regexp_replace(con.source , E'[\\t\\n\\r]+', ' ', 'g') as source_s,
lg.decimallatitude as latitude_f,
lg.decimallongitude as longitude_f,
'' as researcher_s,
array_to_string(array
   (SELECT CASE WHEN (gc.title IS NOT NULL AND gc.title <> '') THEN (gc.title) END
    from collectionobjects_common co2
    inner join hierarchy h2int on co2.id = h2int.id
    join relations_common rc ON (h2int.name = rc.subjectcsid AND rc.objectdocumenttype = 'Group')
    join hierarchy h16 ON (rc.objectcsid = h16.name)
    left outer join groups_common gc ON (h16.id = gc.id)
    join misc mm ON (gc.id=mm.id AND mm.lifecyclestate <> 'deleted')
    where h2int.name = h1.name), '|', '') as grouptitle_ss,
case when (tig.hybridflag = 'true') then 'yes' else 'no' end as hybridflag_s,
case when (tc.taxonisnamedhybrid = 'true') then 'yes' else 'no' end as taxonisnamedhybrid_s,

array_to_string(array
      (SELECT
	CASE WHEN (tig2.qualifier IS NOT NULL AND tig2.qualifier <>'') THEN  '' || tig2.qualifier || ' ' ELSE '' END
  ||CASE WHEN (tig2.taxon IS NOT NULL AND tig2.taxon <>'' and tig2.taxon not like '%no name%') THEN (getdispl(tig2.taxon)
	||CASE WHEN (tig2.identby IS NOT NULL AND tig2.identby <>'' and tig2.identby not like '%unknown%') THEN ', by ' || getdispl(tig2.identby) ELSE '' END
	||CASE WHEN (tig2.institution IS NOT NULL AND tig2.institution <>'') THEN ', ' || getdispl(tig2.institution) ELSE '' END
	||CASE WHEN (prevdetsdg.datedisplaydate IS NOT NULL AND prevdetsdg.datedisplaydate <>'' and prevdetsdg.datedisplaydate <>' ') THEN ', ' || prevdetsdg.datedisplaydate ELSE '' END
	||CASE WHEN (tig2.identkind IS NOT NULL AND tig2.identkind <>'') THEN  ' (' || tig2.identkind || ')'ELSE '' END) ELSE '' END
	||CASE WHEN (tig2.notes IS NOT NULL AND tig2.notes <>'') THEN  '. ' || tig2.notes ELSE '' END
       from collectionobjects_common co1
        inner join hierarchy h1int on co1.id = h1int.id
        left outer join hierarchy htig2 on (co1.id = htig2.parentid and htig2.pos > 0
        and htig2.name = 'collectionobjects_naturalhistory:taxonomicIdentGroupList')
        left outer join taxonomicIdentGroup tig2 on (tig2.id = htig2.id)
        left outer join hierarchy hprevdet on (tig2.id = hprevdet.parentid and hprevdet.name = 'identDateGroup')
        left outer join structureddategroup prevdetsdg on (prevdetsdg.id = hprevdet.id)
       where h1int.name=h1.name order by htig2.pos), '␥', '') previousdeterminations_ss,

array_to_string(array
      (SELECT
      CASE WHEN (tig3.taxon IS NOT NULL AND tig3.taxon <>'' and tig3.taxon not like '%no name%') THEN getdispl(tig3.taxon) ELSE '' END
       from collectionobjects_common co2
        inner join hierarchy h2int on co2.id = h2int.id
        left outer join hierarchy htig3 on (co2.id = htig3.parentid
        and htig3.name = 'collectionobjects_naturalhistory:taxonomicIdentGroupList')
        left outer join taxonomicIdentGroup tig3 on (tig3.id = htig3.id)
       where h2int.name=h1.name order by htig3.pos), '␥', '') as alldeterminations_ss,

regexp_replace(pag.habitat, '^.*\)''(.*)''$', '\1') AS habit_s,
case when cocbd.item is null or cocbd.item = '' then null else cocbd.item end as materialtype_s,
case when co.sex is null or co.sex = '' then null else co.sex end as sex_s,
left(con.provenancetype,1) as provenancetype_short_s,

findparentbyrank(tc.id, 'division') as division_s,
findparentbyrank(tc.id, 'order') as order_s,

to_char(cob.deaddate, 'YYYY-MM-DD') as deaddate_s

from collectionobjects_common co
inner join misc on (co.id = misc.id and misc.lifecyclestate <> 'deleted')
left outer join collectionobjects_common_fieldCollectors fc
        on (co.id = fc.id
        and fc.pos = 0)
left outer join hierarchy hfcdg
        on (co.id = hfcdg.parentid
        and hfcdg.name = 'collectionobjects_common:fieldCollectionDateGroup')
left outer join structureddategroup sdg on (sdg.id = hfcdg.id)

left outer join hierarchy htig
        on (co.id = htig.parentid
        and htig.pos = 0
        and htig.name = 'collectionobjects_naturalhistory:taxonomicIdentGroupList')
left outer join taxonomicIdentGroup tig on (tig.id = htig.id)

left outer join taxon_common tc3 on (tig.taxon=tc3.refname)
left outer join hierarchy hpag
        on (tc3.id = hpag.parentid
        and hpag.pos = 0
        and hpag.name = 'taxon_naturalhistory:plantAttributesGroupList')
left outer join plantattributesgroup pag on (pag.id=hpag.id)

left outer join hierarchy hlg
        on (co.id = hlg.parentid
        and hlg.pos = 0
        and hlg.name = 'collectionobjects_naturalhistory:localityGroupList')
left outer join localitygroup lg on (lg.id = hlg.id)

left outer join hierarchy h1 on co.id=h1.id
join relations_common r1 on (h1.name=r1.subjectcsid and objectdocumenttype='Movement')
left outer join hierarchy h2 on (r1.objectcsid=h2.name and h2.isversion is not true)
join movements_common mc on (mc.id=h2.id)
inner join misc misc1 on (misc1.id = mc.id and misc1.lifecyclestate <> 'deleted') -- movement not deleted

left join collectionobjects_naturalhistory con on (co.id = con.id)
left join collectionobjects_botgarden cob on (co.id=cob.id and cob.deadflag='false')
left outer join collectionobjects_common_comments coc  on (co.id = coc.id and coc.pos = 0)

left outer join taxon_common tc on (tig.taxon=tc.refname)
left outer join taxon_naturalhistory tn on (tc.id=tn.id)

left outer join collectionobjects_common_briefdescriptions cocbd on (co.id = cocbd.id and cocbd.pos = 0)

left outer join (
        select tc.id,
          string_agg(getdispl(pag.conservationcategory), '|' order by h.pos) as conservecat,
          string_agg(getdispl(pag.conservationorganization) || ': ' || getdispl(pag.conservationcategory),
            '|' order by h.pos) as conserveinfo,
          string_agg(getdispl(pag.conservationorganization), '|' order by h.pos) as conserveorg
        from taxon_common tc
        join hierarchy h on (tc.id = h.parentid and h.name = 'taxon_naturalhistory:plantAttributesGroupList')
        left outer join plantattributesgroup pag on (pag.id = h.id)
        where pag.conservationcategory not like '%none%'
        and pag.conservationorganization not like '%not applicable%'
        group by tc.id
) cons on (tc.id = cons.id)

