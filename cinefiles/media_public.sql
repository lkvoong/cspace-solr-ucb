/*
-- remove duplicate join to media_cinefiles
-- remove unused join to collectionobjects_cinefiles
-- exclude deleted collectionobjects and relations
*/

SELECT 
  hcc.name objectcsid,
  cc.objectnumber,
  hmc.name mediacsid,
  mc.description,
  bc.name,
  mc.creator creatorRefname,
  mc.creator creator,
  mc.blobcsid,
  mc.copyrightstatement,
  mc.identificationnumber,
  mc.rightsholder rightsholderRefname,
  mc.rightsholder rightsholder,
  mc.contributor,
  bc.mimetype,
  c.data AS md5
FROM relations_common rc
JOIN misc mrc on (rc.id = mrc.id AND mrc.lifecyclestate != 'deleted')
JOIN hierarchy hmc on (rc.subjectcsid = hmc.name)
JOIN media_common mc on (hmc.id = mc.id)
JOIN misc mmc on (mc.id = mmc.id AND mmc.lifecyclestate != 'deleted')
JOIN hierarchy hcc on (rc.objectcsid = hcc.name)
JOIN collectionobjects_common cc on (hcc.id = cc.id)
JOIN misc mcc on (cc.id = mcc.id AND mcc.lifecyclestate != 'deleted')
JOIN hierarchy hbc ON (mc.blobcsid = hbc.name)
LEFT OUTER JOIN blobs_common bc ON (hbc.id = bc.id)
LEFT OUTER JOIN hierarchy hc ON (
  bc.repositoryid = hc.parentid
  AND hc.primarytype = 'content')
LEFT OUTER JOIN content c ON (hc.id = c.id)
LEFT OUTER JOIN media_cinefiles mcf on (mc.id = mcf.id)
WHERE rc.subjectdocumenttype = 'Media'
AND rc.objectdocumenttype = 'CollectionObject'
ORDER BY mcf.page ASC;
