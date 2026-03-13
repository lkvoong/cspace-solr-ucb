----------------------------------------------------------------------------------------------------
-- ucjepsNewMedia.sql
-- get public media, taxon, loc data for collection objects (media_ucjeps.posttopublic != 'no')
-- includes only digital images and slides ('Digital Image','Slide (Photograph)')
-- excludes deleted media
-- does not exclude deleted collection objects
-- removed unreferenced join to collectionobjects_ucjeps
-- ~ 19K rows
----------------------------------------------------------------------------------------------------

select
  hmc.name as id,
  hcc.name as objectid_s,
  cc.objectnumber as objectNumber_s,
  mc.description as description_s,
  bc.name as name_s,
  getdispl(mc.creator) as creator_s,
  mc.creator as creatorRefname_s,
  mc.blobcsid as blob_ss,
  mc.copyrightstatement as copyrightStatement_s,
  mc.identificationnumber as identificationNumber_s,
  getdispl(mc.rightsholder) as rightsholder_s,
  mc.rightsholder as rightsholderRefname_s,
  getdispl(mc.contributor) as contributor_s,
  mc.contributor as contributorrefname_s,
  regexp_replace(getdispl(mu.scientifictaxonomy), E'[\\t\\n\\r]+', '', 'g') as scientificTaxonomy_s,
  regexp_replace(getdispl(tnh.family), E'[\\t\\n\\r]+', '', 'g') as family_s,
  tu.taxonmajorgroup as majorGroup_s,
  mum.item as morphologyCategoryRefname_s,
  getdispl(mum.item) as morphologyCategory_s,
  mu.majorcategory as majorCategoryRefname_s,
  getdispl(mu.majorcategory) as majorCategory_s,
  mct.item as typeofmedia_s,
  regexp_replace(lg.fieldlocverbatim, E'[\\t\\n\\r]+', ' ', 'g') as locality_s,
  dg.datedisplaydate as mediaDate_s,
  mu.posttopublic as postToPublic_s,
  mu.handwritten as handwritten_s,
  mu.collector as collector_s,
  lg.fieldLocState as fieldLocState_s,
  lg.fieldLocCountry as fieldLocCountry_s,
  lg.fieldLocCounty as fieldLocCounty_s

from media_common mc
  join misc mmc on mc.id = mmc.id

  -- get blobs
  join hierarchy hbc on (
    mc.blobcsid = hbc.name
    and hbc.primarytype = 'Blob')
  left outer join blobs_common bc on hbc.id = bc.id

  -- get collection objects
  left outer join hierarchy hmc on mc.id = hmc.id
  left outer join relations_common rc on (
    hmc.name = rc.subjectcsid
    and rc.subjectdocumenttype = 'Media'
    and rc.objectdocumenttype = 'CollectionObject')
  left outer join hierarchy hcc on rc.objectcsid = hcc.name
  left outer join collectionobjects_common cc on hcc.id = cc.id

  -- get media date
  left outer join hierarchy hdg on (
    mc.id = hdg.parentid
    and hdg.name = 'media_common:dateGroupList'
    and hdg.pos = 0)
  left outer join dategroup dg on hdg.id = dg.id

  -- get media type
  left outer join media_common_typelist mct on (
    mc.id = mct.id
    and mct.pos = 0)

  -- get locality
  left outer join hierarchy hlg on (
    mc.id = hlg.parentid
    and hlg.name = 'media_ucjeps:localityGroupList'
    and hlg.pos = 0)
  left outer join localitygroup lg on hlg.id = lg.id

  left outer join media_ucjeps mu on (
    mc.id = mu.id
    and mu.posttopublic != 'no')

  -- get morphology category
  left outer join media_ucjeps_morphologycategories mum on (
    mu.id = mum.id
    and mum.pos = 0)

  -- get taxonomy
  left outer join taxon_common tc on mu.scientifictaxonomy = tc.refname
  left outer join taxon_ucjeps tu on tc.id = tu.id
  left outer join taxon_naturalhistory tnh on tc.id = tnh.id

where mmc.lifecyclestate <> 'deleted'
  and mct.item in ('Digital Image', 'Slide (Photograph)');
