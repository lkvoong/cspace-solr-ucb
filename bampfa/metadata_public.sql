with subjecttheme_rank as (
  select 
    id,
    getdispl(item) as item,
    row_number() over(partition by id order by pos) as pos_index
  from collectionobjects_bampfa_subjectthemes
),

subjectthemes as (
  select 
    id,
    max(case when pos_index = 1 THEN item end) AS SubjectOne,
    max(case when pos_index = 2 THEN item end) AS SubjectTwo,
    max(case when pos_index = 3 THEN item end) AS SubjectThree,
    max(case when pos_index = 4 THEN item end) AS SubjectFour,
    max(case when pos_index = 5 THEN item end) AS SubjectFive
  from subjecttheme_rank
  where pos_index <= 5
  group by id
)

select
   hcc.name as objectCSID,
   cc.objectnumber as idNumber,
   cb.sortableEffectiveObjectNumber as sortObjectNumber,
   con.numbervalue as otherNumber,
   getdispl(cb.itemclass) as itemclass,
   coalesce(nullif(cb.artistdisplayoverride, ''), utils.concat_artists(hcc.name)) as artistCalc,
   --   getdispl(opp.bampfaobjectproductionperson) as artist,
   case
     when nullif(pc.birthplace, '') is null then pcn.item
     when pcn.item = pc.birthplace then pcn.item
     else pcn.item || ', born '|| pc.birthplace end as artistorigin,
   bdg.datedisplaydate as artistbirthdate,
   ddg.datedisplaydate as artistdeathdate,
   pb.datesactive,
   bt.bampfatitle as title,
   '-REDACTED-' as initialvalue,  -- cb.initialvalue,
   '-REDACTED-' as currentvalue,  -- cv.currentvalue,
   cvg.currentvaluesource,
   cvdg.datedisplaydate as currentvaluedate,
   cb.creditline,
   'University of California, Berkeley Art Museum and Pacific Film Archive' || coalesce('; '|| cb.creditline, '') as fullBAMPFAcreditline,
   coalesce(nullif(cb.permissiontoreproduce, ''), pb.permissiontoreproduce) as permissiontoreproduce,
   coalesce(nullif(cb.copyrightCredit, ''), pb.copyrightCredit) as copyrightCredit,
   cb.photoCredit,
   sdg.datedisplaydate as dateMade,
   replace(mpg.dimensionsummary, '-', ' ') as measurement,
   cc.physicaldescription as materials,
   adg.datedisplaydate as dateacquired, -- in future will need case statements to get from intake
   getdispl(cbas.item) as acquisitionsource,
   cb.provenance,
   tig.inscriptioncontent as signature,
   ccc.item as notescomments,
   cg.catalogername as cataloger,
   cg.catalognote as catalognote,
   cg.catalogdate,
   apg.assocplace as site,
   st.SubjectOne,
   st.SubjectTwo,
   st.SubjectThree,
   st.SubjectFour,
   st.SubjectFive,
   -- these values are included here, but eliminated during the loading process
   getdispl(cc.computedcurrentlocation) as currentlocation,
   getdispl(cb.computedcrate) as currentcrate,
   trim(cb.objectProductionDateCentury) || ' ' || getdispl(cb.objectProductionDateEra) as century,
   'not yet available' as grouptitle_ss,
   to_char(sdg.datelatestscalarvalue, 'YYYY-MM-DD') as dateMadeYear_dt
from
   hierarchy hcc
   JOIN collectionobjects_common cc on (hcc.id = cc.id)
   JOIN misc m on (cc.id = m.id and m.lifecyclestate <> 'deleted')
   JOIN collectionobjects_bampfa cb on (cc.id = cb.id)

   left outer join hierarchy h2 on (
      h2.parentid = cc.id 
      and h2.name = 'collectionobjects_common:objectProductionDateGroupList'
      and h2.pos = 0)
   left outer join structuredDateGroup sdg on (h2.id = sdg.id)

   left outer join hierarchy hcon on (
      hcon.parentid = cc.id
      and hcon.name = 'collectionobjects_common:otherNumberList'
      and hcon.pos = 0)
   left outer join othernumber con on (hcon.id = con.id)

   left outer join hierarchy hbt on (
      hbt.parentid = cc.id
      and hbt.name = 'collectionobjects_bampfa:bampfaTitleGroupList'
      and hbt.pos = 0)
   left outer join bampfatitlegroup bt on (hbt.id = bt.id)

   left outer join hierarchy hcvg
      on (hcvg.parentid = cc.id
      and hcvg.name = 'collectionobjects_bampfa:currentValueGroupList'
      and hcvg.pos = 0)
   left outer join currentvaluegroup cvg on (hcvg.id = cvg.id)

   left outer join hierarchy hcvdg
      on (hcvdg.parentid = cvg.id
      and hcvdg.name = 'currentValueDateGroup')
   left outer join structuredDateGroup cvdg on (hcvdg.id = cvdg.id)

   left outer join hierarchy hmpg
      on (hmpg.parentid = cc.id
      and hmpg.name = 'collectionobjects_common:measuredPartGroupList'
      and hmpg.pos = 0)
   left outer join measuredpartgroup mpg on (hmpg.id = mpg.id)

   left outer join hierarchy htig
      on (htig.parentid = cc.id
      and htig.name = 'collectionobjects_common:textualInscriptionGroupList'
      and htig.pos = 0)
   left outer join textualinscriptiongroup tig on (htig.id = tig.id)

   left outer join hierarchy hadg
      on (hadg.parentid = cc.id
      and hadg.name = 'collectionobjects_bampfa:acquisitionDateGroupList'
      and hadg.pos = 0)
   left outer join structuredDateGroup adg on (hadg.id = adg.id)

   left outer join collectionobjects_bampfa_acquisitionsources cbas on (
      cc.id = cbas.id
      and cbas.pos = 0)

   left outer join collectionobjects_common_comments ccc on (
      cc.id = ccc.id
      and ccc.pos = 0)

   left outer join hierarchy hcg
      on (hcg.parentid = cc.id
      and hcg.name = 'collectionobjects_bampfa:catalogerGroupList'
      and hcg.pos = 0)
   left outer join catalogergroup cg on (hcg.id = cg.id)

   left outer join hierarchy hopp
      on (hopp.parentid = cc.id
      and hopp.name = 'collectionobjects_bampfa:bampfaObjectProductionPersonGroupList'
      and hopp.pos = 0)
   left outer join bampfaobjectproductionpersongroup opp on (hopp.id = opp.id)
   left outer join persons_common pc on (opp.bampfaobjectproductionperson = pc.refname)
   left outer join persons_common_nationalities pcn on (
      pc.id = pcn.id
      and pcn.pos = 0)
   left outer join persons_bampfa pb on (pc.id = pb.id)

   left outer join hierarchy hbdg
      on (hbdg.parentid = pc.id
      and hbdg.name = 'persons_common:birthDateGroup')
   left outer join structuredDateGroup bdg on (hbdg.id = bdg.id)

   left outer join hierarchy hddg
      on (hddg.parentid = pc.id
      and hddg.name = 'persons_common:deathDateGroup')
   left outer join structuredDateGroup ddg on (hddg.id = ddg.id)

   left outer join hierarchy hapg
      on (hapg.parentid = cc.id
      and hapg.name = 'collectionobjects_common:assocPlaceGroupList'
      and hapg.pos = 0)
   left outer join assocplacegroup apg on (hapg.id = apg.id)

   left outer join subjectthemes st on (cc.id = st.id)

where getdispl(cb.legalstatus) in ('permanent collection', 'extended loan', 'UCBerkeley dispersed')
