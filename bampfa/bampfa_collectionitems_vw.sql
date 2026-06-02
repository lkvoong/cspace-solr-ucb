/* bampfa_collectionitems_vw.sql
 *  View used to get live data in support of several functions, including Inject Metadata
 *  CRH 10/22/2014
 *  CRH 10/27/2014 Measurements subst hyphen with space. Utils schema.
 *  CRH 11/25/2014 Added artistDisplayOverride
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
*/

-- DROP VIEW utils.bampfa_collectionitems_vw

CREATE OR REPLACE VIEW utils.bampfa_collectionitems_vw AS
WITH subjectthemes AS (	-- get first five subjectthemes
  SELECT
    id,
    MAX(CASE WHEN pos = 0 THEN GETDISPL(item) END) AS SubjectOne,
    MAX(CASE WHEN pos = 1 THEN GETDISPL(item) END) AS SubjectTwo,
    MAX(CASE WHEN pos = 2 THEN GETDISPL(item) END) AS SubjectThree,
    MAX(CASE WHEN pos = 3 THEN GETDISPL(item) END) AS SubjectFour,
    MAX(CASE WHEN pos = 4 THEN GETDISPL(item) END) AS SubjectFive
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
)

SELECT
  hcc.name AS objectCSID,
  cc.objectnumber AS idNumber,
  cb.sortableEffectiveObjectNumber AS sortObjectNumber,
  con.numbervalue AS otherNumber,
  GETDISPL(cb.itemclass) AS itemclass,
  COALESCE(NULLIF(cb.artistdisplayoverride, ''), UTILS.CONCAT_ARTISTS(hcc.name)) AS artistCalc,
  CASE
    WHEN NULLIF(pc.birthplace, '') IS NULL THEN pcn.item
    WHEN pcn.item = pc.birthplace THEN pcn.item
    ELSE pcn.item || ', born '|| pc.birthplace END AS artistorigin,
  bdg.datedisplaydate AS artistbirthdate,
  ddg.datedisplaydate AS artistdeathdate,
  pb.datesactive,
  btg.bampfatitle AS title,
  cb.initialvalue,
  cvg.currentvalue,
  cvg.currentvaluesource,
  cvdg.datedisplaydate AS currentvaluedate,
  cb.creditline,
  'University of California, Berkeley Art Museum and Pacific Film Archive'
    || COALESCE('; '|| cb.creditline, '') AS fullBAMPFAcreditline,
  COALESCE(NULLIF(cb.permissiontoreproduce, ''), pb.permissiontoreproduce) AS permissiontoreproduce,
  COALESCE(NULLIF(cb.copyrightCredit, ''), pb.copyrightCredit) AS copyrightCredit,
  cb.photoCredit,
  pdg.datedisplaydate AS dateMade,
  REPLACE(mpg.dimensionsummary, '-', ' ') AS measurement,
  cc.physicaldescription AS materials,
  adg.datedisplaydate AS dateacquired,	-- in future will need case statements to get from intake
  GETDISPL(cbas.item) AS acquisitionsource,
  cb.provenance,
  tig.inscriptioncontent AS signature,
  TRIM(REGEXP_REPLACE(ccc.item, '[[:space:]]+', ' ', 'g')) AS notescomments,	-- replace CRLF w/space then trim
  cg.catalogername AS cataloger,
  cg.catalognote AS catalognote,
  cg.catalogdate,
  NULLIF(pplg.objectproductionplace, '') AS site,	-- from apg.assocplace in metadata_internal.sql,
  st.SubjectOne,
  st.SubjectTwo,
  st.SubjectThree,
  st.SubjectFour,
  st.SubjectFive,
  GETDISPL(cc.computedcurrentlocation) AS currentlocation,
  GETDISPL(cb.computedcrate) AS currentcrate,
  c.collection1,
  c.collection2,
  c.collection3,
  ps.periodstyle1,
  ps.periodstyle2,
  ps.periodstyle3,
  ps.periodstyle4,
  ps.periodstyle5,
  GETDISPL(cb.legalstatus) AS legalstatus,
  UTILS.GET_OBJECT_IMAGE_COUNT(hcc.name) AS imagecount,
  UTILS.GET_FIRST_BLOBCSID_DISPLEVEL(hcc.name, 'blobcsid') AS image1blobcsid,
  UTILS.GET_FIRST_BLOBCSID_DISPLEVEL(hcc.name, 'displevel') AS image1displevel,
  GETDISPL(cb.acquisitionmethod) AS acquisitionmethod

FROM collectionobjects_common cc
  JOIN hierarchy hcc ON cc.id = hcc.id
  JOIN misc mcc ON cc.id = mcc.id
  JOIN collectionobjects_bampfa cb ON cc.id = cb.id

  LEFT OUTER JOIN hierarchy hpdg	-- get first production date
    ON cc.id = hpdg.parentid
    AND hpdg.name = 'collectionobjects_common:objectProductionDateGroupList'
    AND hpdg.pos = 0
  LEFT OUTER JOIN structuredDateGroup pdg ON hpdg.id = pdg.id

  LEFT OUTER JOIN hierarchy hcon	-- get first other number
    ON cc.id = hcon.parentid
    AND hcon.name = 'collectionobjects_common:otherNumberList'
    AND hcon.pos = 0
  LEFT OUTER JOIN othernumber con ON hcon.id = con.id

  LEFT OUTER JOIN hierarchy hbtg	-- get first title
    ON cc.id = hbtg.parentid
    AND hbtg.name = 'collectionobjects_bampfa:bampfaTitleGroupList'
    AND hbtg.pos = 0
  LEFT OUTER JOIN bampfatitlegroup btg ON hbtg.id = btg.id

  LEFT OUTER JOIN hierarchy hcvg	-- get first current value
    ON cc.id = hcvg.parentid
    AND hcvg.name = 'collectionobjects_bampfa:currentValueGroupList'
    AND hcvg.pos = 0
  LEFT OUTER JOIN currentvaluegroup cvg ON hcvg.id = cvg.id

  LEFT OUTER JOIN hierarchy hcvdg	-- get first current value date
    ON cvg.id = hcvdg.parentid
    AND hcvdg.name = 'currentValueDateGroup'
  LEFT OUTER JOIN structuredDateGroup cvdg ON hcvdg.id = cvdg.id

  LEFT OUTER JOIN hierarchy hmpg	-- get first measured part
    ON cc.id = hmpg.parentid
    AND hmpg.name = 'collectionobjects_common:measuredPartGroupList'
    AND hmpg.pos = 0
  LEFT OUTER JOIN measuredpartgroup mpg ON hmpg.id = mpg.id

  LEFT OUTER JOIN hierarchy htig	-- get first signature
    ON cc.id = htig.parentid
    AND htig.name = 'collectionobjects_common:textualInscriptionGroupList'
    AND htig.pos = 0
  LEFT OUTER JOIN textualinscriptiongroup tig ON htig.id = tig.id

  LEFT OUTER JOIN hierarchy hadg	-- get first acquisition date
    ON cc.id = hadg.parentid
    AND hadg.name = 'collectionobjects_bampfa:acquisitionDateGroupList'
    AND hadg.pos = 0
  LEFT OUTER JOIN structuredDateGroup adg ON hadg.id = adg.id

  LEFT OUTER JOIN collectionobjects_bampfa_acquisitionsources cbas	-- get first acquisition source
    ON cc.id = cbas.id
    AND cbas.pos = 0

  LEFT OUTER JOIN collectionobjects_common_comments ccc	-- get first collection object comment
    ON cc.id = ccc.id
    AND ccc.pos = 0

  LEFT OUTER JOIN hierarchy hcg	-- get first cataloger
    ON cc.id = hcg.parentid
    AND hcg.name = 'collectionobjects_bampfa:catalogerGroupList'
    AND hcg.pos = 0
  LEFT OUTER JOIN catalogergroup cg ON hcg.id = cg.id

  LEFT OUTER JOIN hierarchy hppg	-- get first artist
    ON cc.id = hppg.parentid
    AND hppg.name = 'collectionobjects_bampfa:bampfaObjectProductionPersonGroupList'
    AND hppg.pos = 0
  LEFT OUTER JOIN bampfaobjectproductionpersongroup ppg ON hppg.id = ppg.id
  LEFT OUTER JOIN persons_common pc ON ppg.bampfaobjectproductionperson = pc.refname

  LEFT OUTER JOIN persons_common_nationalities pcn	-- get first nationality of first artist
    ON pc.id = pcn.id
    AND pcn.pos = 0

  LEFT OUTER JOIN persons_bampfa pb ON pc.id = pb.id	-- get active dates, permissions, copyright for first artist

  LEFT OUTER JOIN hierarchy hbdg	-- get birth date of first artist
    ON pc.id = hbdg.parentid
    AND hbdg.name = 'persons_common:birthDateGroup'
  LEFT OUTER JOIN structuredDateGroup bdg ON hbdg.id = bdg.id

  LEFT OUTER JOIN hierarchy hddg	-- get death date of first artist
    ON pc.id = hddg.parentid
    AND hddg.name = 'persons_common:deathDateGroup'
  LEFT OUTER JOIN structuredDateGroup ddg ON hddg.id = ddg.id

  LEFT OUTER JOIN hierarchy hpplg	-- get first production place
    ON cc.id = hpplg.parentid
    AND hpplg.name = 'collectionobjects_common:objectProductionPlaceGroupList'
    AND hpplg.pos = 0
  LEFT OUTER JOIN objectproductionplacegroup pplg ON hpplg.id = pplg.id

  LEFT OUTER JOIN subjectthemes st ON cc.id = st.id	-- get subjects
  LEFT OUTER JOIN collections c ON cc.id = c.id	-- get collections
  LEFT OUTER JOIN periodstyles ps ON cc.id = ps.id	-- get periodstyles

WHERE mcc.lifecyclestate <> 'deleted'	-- exclude deleted objects
ORDER BY cb.sortableEffectiveObjectNumber;


GRANT SELECT ON utils.bampfa_collectionitems_vw TO reader_bampfa;
GRANT SELECT ON utils.bampfa_collectionitems_vw TO group reporters_bampfa;
