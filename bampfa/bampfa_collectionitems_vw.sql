/* bampfa_collectionitems_vw.sql
 *  View used to get live data in support of several functions, including Inject Metadata
 *  CRH 10/22/2014
 *  CRH 10/27/2014 Measurements subst hyphen with space. Utils schema.
 *  CRH 11/25/2014 Added artistdisplayoverride
 *  CRH 04/25/2015 Added several fields; see Jira BAMPFA-402
 *  CRH 07/30/2015 Added Acquisition Method BAMPFA-446
 *  LKV 09/28/2016 BAMPFA-507: Add call to new function utils.get_first_blobcsid_displevel to
      get first image blob csid (image1blobcsid)
      and website display level (image1displevel) for all images.
 *  LKV 10/27/2016 BAMPFA-512: Added new field 'image_count' using new function utils.get_object_image_count
 *  LKV 05/26/2026 CSW-998:
      update query to match metadata_public.sql
      remove extraneous join to collectionspace_core
      NOTE: site data source is different from Solr metadata queries, ie objectproductionplace vs assocplace
 *  LKV 11/06/2026 CSW-1033:
      re-write to remove get_object_image_count, get_first_blobcsid_displevel, concat_artists
      add exclusions for deleted relations, blobs, persons
*/

DROP VIEW utils.bampfa_collectionitems_vw;

CREATE VIEW utils.bampfa_collectionitems_vw AS
WITH collectionobjects AS (
  SELECT
    cc.id,
    hcc.name AS objectcsid,
    cc.objectnumber AS idnumber,
    cb.sortableeffectiveobjectnumber AS sortobjectnumber,
    cb.itemclass,
    cb.artistdisplayoverride,
    cb.initialvalue,
    cb.creditline,
    cb.permissiontoreproduce,
    cb.copyrightcredit,
    cb.photocredit,
    cc.physicaldescription AS materials,
    cbas.item AS acquisitionsource,
    cb.provenance,
    ccc.item AS notescomments,
    cc.computedcurrentlocation,
    cb.computedcrate,
    cb.legalstatus,
    cb.acquisitionmethod
  FROM collectionobjects_common cc
    JOIN hierarchy hcc ON cc.id = hcc.id
    JOIN misc mcc ON cc.id = mcc.id
    JOIN collectionobjects_bampfa cb ON cc.id = cb.id
    LEFT OUTER JOIN collectionobjects_bampfa_acquisitionsources cbas	-- get first acquisition source
      ON cc.id = cbas.id
      AND cbas.pos = 0
    LEFT OUTER JOIN collectionobjects_common_comments ccc	-- get first collection object comment
      ON cc.id = ccc.id
      AND ccc.pos = 0
  WHERE mcc.lifecyclestate <> 'deleted'	-- exclude deleted objects
),

artists AS (
  SELECT
    hppg.parentid AS objectid,
    STRING_AGG(
      GETDISPL(ppg.bampfaobjectproductionperson) ||
      COALESCE(' (' || NULLIF(GETDISPL(ppg.bampfaobjectproductionpersonqualifier), '') || ')', ''),
      '; ' ORDER BY hppg.pos) AS artistcalc
  FROM hierarchy hppg	-- get all artists
  JOIN bampfaobjectproductionpersongroup ppg ON hppg.id = ppg.id
  JOIN persons_common pc ON ppg.bampfaobjectproductionperson = pc.refname
  JOIN misc mpc ON pc.id = mpc.id
  WHERE mpc.lifecyclestate <> 'deleted'	-- exclude deleted artists
    AND hppg.name = 'collectionobjects_bampfa:bampfaObjectProductionPersonGroupList'
    AND ppg.bampfaobjectproductionperson IS NOT NULL	-- exclude null artists
  GROUP by hppg.parentid
),

firstartists AS (
  SELECT
    hppg.parentid AS objectid,
    CASE
      WHEN NULLIF(pc.birthplace, '') IS NULL THEN pcn.item
      WHEN pcn.item = pc.birthplace THEN pcn.item
      ELSE pcn.item || ', born '|| pc.birthplace END AS artistorigin,
    bdg.datedisplaydate AS artistbirthdate,
    ddg.datedisplaydate AS artistdeathdate,
    pb.datesactive,
    pb.permissiontoreproduce AS permissiontoreproduceartist,
    pb.copyrightcredit AS copyrightcreditartist
  FROM hierarchy hppg        -- get first artist
    JOIN bampfaobjectproductionpersongroup ppg ON hppg.id = ppg.id
    JOIN persons_common pc ON ppg.bampfaobjectproductionperson = pc.refname
    JOIN misc mpc ON pc.id = mpc.id
    LEFT OUTER JOIN persons_common_nationalities pcn      -- get first nationality of first artist
      ON pc.id = pcn.id
      AND pcn.pos = 0
    LEFT OUTER JOIN persons_bampfa pb ON pc.id = pb.id    -- get active dates, permissions, copyright for first artist
    LEFT OUTER JOIN hierarchy hbdg        -- get birth date of first artist
      ON pc.id = hbdg.parentid
      AND hbdg.name = 'persons_common:birthDateGroup'
    LEFT OUTER JOIN structuredDateGroup bdg ON hbdg.id = bdg.id
    LEFT OUTER JOIN hierarchy hddg        -- get death date of first artist
      ON pc.id = hddg.parentid
      AND hddg.name = 'persons_common:deathDateGroup'
    LEFT OUTER JOIN structuredDateGroup ddg ON hddg.id = ddg.id
  WHERE mpc.lifecyclestate <> 'deleted'	-- exclude deleted persons
    AND hppg.name = 'collectionobjects_bampfa:bampfaObjectProductionPersonGroupList'
    AND hppg.pos = 0
),

subjectthemes AS (	-- get first five subjectthemes
  SELECT
    id,
    MAX(CASE WHEN pos = 0 THEN GETDISPL(item) END) AS subjectone,
    MAX(CASE WHEN pos = 1 THEN GETDISPL(item) END) AS subjecttwo,
    MAX(CASE WHEN pos = 2 THEN GETDISPL(item) END) AS subjectthree,
    MAX(CASE WHEN pos = 3 THEN GETDISPL(item) END) AS subjectfour,
    MAX(CASE WHEN pos = 4 THEN GETDISPL(item) END) AS subjectfive
  FROM collectionobjects_bampfa_subjectthemes
  WHERE pos < 5
  GROUP BY id
),

collections AS (	-- get first 3 collections
  SELECT
    id,
    MAX(CASE WHEN pos = 0 THEN GETDISPL(item) END) AS collection1,
    MAX(CASE WHEN pos = 1 THEN GETDISPL(item) END) AS collection2,
    MAX(CASE WHEN pos = 2 THEN GETDISPL(item) END) AS collection3
  FROM collectionobjects_bampfa_bampfacollectionlist
  WHERE pos < 3
  GROUP BY id
),

periodstyles AS (	-- get first 5 periodstyles
  SELECT
    id,
    MAX(CASE WHEN pos = 0 THEN GETDISPL(item) END) AS periodstyle1,
    MAX(CASE WHEN pos = 1 THEN GETDISPL(item) END) AS periodstyle2,
    MAX(CASE WHEN pos = 2 THEN GETDISPL(item) END) AS periodstyle3,
    MAX(CASE WHEN pos = 3 THEN GETDISPL(item) END) AS periodstyle4,
    MAX(CASE WHEN pos = 4 THEN GETDISPL(item) END) AS periodstyle5
  FROM collectionobjects_common_styles
  WHERE pos < 5
  GROUP BY id
),

objectmedia AS (	-- get all object media
  SELECT
    co.objectcsid,
    mc.id as mediaid,
    mc.blobcsid
  FROM collectionobjects co
    JOIN relations_common rc ON co.objectcsid = rc.objectcsid
    JOIN misc mrc ON rc.id = mrc.id
    JOIN hierarchy hmc ON rc.subjectcsid = hmc.name
    JOIN media_common mc ON hmc.id = mc.id
    JOIN misc mmc ON mc.id = mmc.id
    JOIN hierarchy hbc ON mc.blobcsid = hbc.name
    JOIN misc mbc ON hbc.id = mbc.id
  WHERE rc.objectdocumenttype = 'CollectionObject'
    AND rc.subjectdocumenttype = 'Media'
    AND mrc.lifecyclestate <> 'deleted'       -- exclude deleted relations
    AND mmc.lifecyclestate <> 'deleted'       -- exclude deleted media
    AND mbc.lifecyclestate <> 'deleted'       -- exclude deleted blobs
),

objectmediacounts AS (	-- get count of all non-deleted media by objectcsid
  SELECT
    objectcsid,
    COUNT(*) AS imagecount
  FROM objectmedia
  GROUP BY objectcsid
),

-- some objects have > 1 media records with imagenumber = 1
-- some objects have media records, and none have imagenumber = 1
-- media_common.datemodified and media_common.datecreated are all null
-- therefore, must use collectionspace_core.updatedat for media records
-- to get most recent media record with imagenumber = 1

image1 AS (
  SELECT DISTINCT ON (om.objectcsid)
    om.objectcsid,
    om.blobcsid AS image1blobcsid,
    mb.websitedisplaylevel AS image1displevel
  FROM objectmedia om
  JOIN media_bampfa mb on om.mediaid = mb.id
  JOIN collectionspace_core csc ON om.mediaid = csc.id
  WHERE mb.imagenumber = 1
  ORDER BY om.objectcsid, csc.updatedat DESC, om.blobcsid DESC	-- order by updatedat and blobcsid for tiebreaker
)

SELECT
  co.objectcsid,
  co.idnumber,
  co.sortobjectnumber,
  con.numbervalue AS othernumber,
  GETDISPL(co.itemclass) AS itemclass,
  COALESCE(NULLIF(co.artistdisplayoverride, ''), a.artistcalc) as artistcalc,
  fa.artistorigin,
  fa.artistbirthdate,
  fa.artistdeathdate,
  fa.datesactive,
  btg.bampfatitle AS title,
  co.initialvalue,
  cvg.currentvalue,
  cvg.currentvaluesource,
  cvdg.datedisplaydate AS currentvaluedate,
  co.creditline,
  'University of California, Berkeley Art Museum and Pacific Film Archive'
    || COALESCE('; '|| co.creditline, '') AS fullbampfacreditline,
  COALESCE(NULLIF(co.permissiontoreproduce, ''), fa.permissiontoreproduceartist) AS permissiontoreproduce,
  COALESCE(NULLIF(co.copyrightcredit, ''), fa.copyrightcreditartist) AS copyrightcredit,
  co.photocredit,
  pdg.datedisplaydate AS datemade,
  REPLACE(mpg.dimensionsummary, '-', ' ') AS measurement,
  co.materials,
  adg.datedisplaydate AS dateacquired,	-- in future will need case statements to get from intake
  GETDISPL(co.acquisitionsource) AS acquisitionsource,
  co.provenance,
  tig.inscriptioncontent AS signature,
  TRIM(REGEXP_REPLACE(co.notescomments, '[[:space:]]+', ' ', 'g')) AS notescomments,	-- replace CRLF w/space
  cg.catalogername AS cataloger,
  cg.catalognote AS catalognote,
  cg.catalogdate,
  NULLIF(pplg.objectproductionplace, '') AS site,	-- from apg.assocplace in metadata_internal.sql,
  st.subjectone,
  st.subjecttwo,
  st.subjectthree,
  st.subjectfour,
  st.subjectfive,
  GETDISPL(co.computedcurrentlocation) AS currentlocation,
  GETDISPL(co.computedcrate) AS currentcrate,
  c.collection1,
  c.collection2,
  c.collection3,
  ps.periodstyle1,
  ps.periodstyle2,
  ps.periodstyle3,
  ps.periodstyle4,
  ps.periodstyle5,
  GETDISPL(co.legalstatus) AS legalstatus,
  COALESCE(omc.imagecount, 0) as imagecount,
  i1.image1blobcsid,
  i1.image1displevel,
  GETDISPL(co.acquisitionmethod) AS acquisitionmethod

FROM collectionobjects co
  LEFT OUTER JOIN hierarchy hcon	-- get first other number
    ON co.id = hcon.parentid
    AND hcon.name = 'collectionobjects_common:otherNumberList'
    AND hcon.pos = 0
  LEFT OUTER JOIN othernumber con ON hcon.id = con.id
  LEFT OUTER JOIN artists a ON co.id = a.objectid	-- calculate artist
  LEFT OUTER JOIN firstartists fa ON co.id = fa.objectid	-- get first artist data
  LEFT OUTER JOIN hierarchy hbtg	-- get first title
    ON co.id = hbtg.parentid
    AND hbtg.name = 'collectionobjects_bampfa:bampfaTitleGroupList'
    AND hbtg.pos = 0
  LEFT OUTER JOIN bampfatitlegroup btg ON hbtg.id = btg.id
  LEFT OUTER JOIN hierarchy hcvg	-- get first current value
    ON co.id = hcvg.parentid
    AND hcvg.name = 'collectionobjects_bampfa:currentValueGroupList'
    AND hcvg.pos = 0
  LEFT OUTER JOIN currentvaluegroup cvg ON hcvg.id = cvg.id
  LEFT OUTER JOIN hierarchy hcvdg	-- get first current value date
    ON cvg.id = hcvdg.parentid
    AND hcvdg.name = 'currentValueDateGroup'
  LEFT OUTER JOIN structuredDateGroup cvdg ON hcvdg.id = cvdg.id
  LEFT OUTER JOIN hierarchy hpdg	-- get first production date
    ON co.id = hpdg.parentid
    AND hpdg.name = 'collectionobjects_common:objectProductionDateGroupList'
    AND hpdg.pos = 0
  LEFT OUTER JOIN structuredDateGroup pdg ON hpdg.id = pdg.id
  LEFT OUTER JOIN hierarchy hmpg	-- get first measured part
    ON co.id = hmpg.parentid
    AND hmpg.name = 'collectionobjects_common:measuredPartGroupList'
    AND hmpg.pos = 0
  LEFT OUTER JOIN measuredpartgroup mpg ON hmpg.id = mpg.id
  LEFT OUTER JOIN hierarchy hadg	-- get first acquisition date
    ON co.id = hadg.parentid
    AND hadg.name = 'collectionobjects_bampfa:acquisitionDateGroupList'
    AND hadg.pos = 0
  LEFT OUTER JOIN structuredDateGroup adg ON hadg.id = adg.id
  LEFT OUTER JOIN hierarchy htig	-- get first signature
    ON co.id = htig.parentid
    AND htig.name = 'collectionobjects_common:textualInscriptionGroupList'
    AND htig.pos = 0
  LEFT OUTER JOIN textualinscriptiongroup tig ON htig.id = tig.id
  LEFT OUTER JOIN hierarchy hcg	-- get first cataloger
    ON co.id = hcg.parentid
    AND hcg.name = 'collectionobjects_bampfa:catalogerGroupList'
    AND hcg.pos = 0
  LEFT OUTER JOIN catalogergroup cg ON hcg.id = cg.id
  LEFT OUTER JOIN hierarchy hpplg	-- get first production place
    ON co.id = hpplg.parentid
    AND hpplg.name = 'collectionobjects_common:objectProductionPlaceGroupList'
    AND hpplg.pos = 0
  LEFT OUTER JOIN objectproductionplacegroup pplg ON hpplg.id = pplg.id
  LEFT OUTER JOIN subjectthemes st ON co.id = st.id	-- get subjects
  LEFT OUTER JOIN collections c ON co.id = c.id	-- get collections
  LEFT OUTER JOIN periodstyles ps ON co.id = ps.id	-- get periodstyles
  LEFT OUTER JOIN objectmediacounts omc ON co.objectcsid = omc.objectcsid	-- get image count
  LEFT OUTER JOIN image1 i1 ON co.objectcsid = i1.objectcsid	-- get blobcsid, displevel for imagenumber 1
  ORDER BY co.sortobjectnumber;

GRANT SELECT ON utils.bampfa_collectionitems_vw TO reader_bampfa;
GRANT SELECT ON utils.bampfa_collectionitems_vw TO group reporters_bampfa;
