/* metadata_public.sql
 *
 * get metadata for BAMPFA object records
 *   Subject*: get first five subjects
 *   grouptitle_ss: set to 'not yet available'
 *   artistCalc: aggregate artists as a semi-colon + space separated list
 *   artistorigin: calculate artist origin from birthplace and nationality
 *   initialvalue, currentvalue: redact value data
 *   fullBAMPFAcreditline: calculated based on collectionobjects_bampfa value
 *   permissiontoreproduce, copyrightCredit: calculate based on collectionobjects_bampfa, persons_bampfa values
 *   measurement: replace hyphen with space
 *   dateacquired: display date from acquisitionDateGroupList, in future will need case to get from intake
 *   notescomments: replace white space, e.g. CRLF etc with space then trim
 *   century: calculate from objectProductionDateCentury and objectProductionDateEra
 *   dateMadeYear_dt: formatted as 'YYYY-MM-Dd from datelatestscalarvalue of first production date
*/

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
)

SELECT
  hcc.name AS objectCSID,
  cc.objectnumber AS idNumber,
  cb.sortableEffectiveObjectNumber AS sortObjectNumber,
  con.numbervalue AS otherNumber,
  GETDISPL(cb.itemclass) AS itemclass,
  COALESCE(NULLIF(cb.artistdisplayoverride, ''), UTILS.CONCAT_ARTISTS(hcc.name)) AS artistCalc,
  --  GETDISPL(ppg.bampfaobjectproductionperson) AS artist,
  CASE
    WHEN NULLIF(pc.birthplace, '') IS NULL THEN pcn.item
    WHEN pcn.item = pc.birthplace THEN pcn.item
    ELSE pcn.item || ', born '|| pc.birthplace END AS artistorigin,
  bdg.datedisplaydate AS artistbirthdate,
  ddg.datedisplaydate AS artistdeathdate,
  pb.datesactive,
  btg.bampfatitle AS title,
  '-REDACTED-' AS initialvalue,	-- cb.initialvalue,
  '-REDACTED-' AS currentvalue,	-- cvg.currentvalue,
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
  apg.assocplace AS site,
  st.SubjectOne,
  st.SubjectTwo,
  st.SubjectThree,
  st.SubjectFour,
  st.SubjectFive,
  -- these values are included here, but eliminated during the loading process
  GETDISPL(cc.computedcurrentlocation) AS currentlocation,
  GETDISPL(cb.computedcrate) AS currentcrate,
  TRIM(cb.objectProductionDateCentury) || ' ' || GETDISPL(cb.objectProductionDateEra) AS century,
  'not yet available' AS grouptitle_ss,
  TO_CHAR(pdg.datelatestscalarvalue, 'YYYY-MM-DD') AS dateMadeYear_dt

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

  LEFT OUTER JOIN hierarchy hapg	-- get first collection site of collection object
    ON cc.id = hapg.parentid
    AND hapg.name = 'collectionobjects_common:assocPlaceGroupList'
    AND hapg.pos = 0
  LEFT OUTER JOIN assocplacegroup apg ON hapg.id = apg.id

  LEFT OUTER JOIN subjectthemes st ON cc.id = st.id	-- get subjects

WHERE mcc.lifecyclestate <> 'deleted'	-- exclude deleted objects
  AND GETDISPL(cb.legalstatus) IN ('permanent collection', 'extended loan', 'UCBerkeley dispersed')

