/*
-- media_internal.sql
--
-- get collection objects with media and blob data, where relations, objects, media, and blobs are not deleted.
*/

SELECT 
  hcc.name AS objectcsid,
  cc.objectnumber,
  hmc.name AS mediacsid,
  mc.description,
  bc.name,
  mc.creator AS creatorRefname,
  mc.creator AS creator,
  mc.blobcsid,
  mc.copyrightstatement,
  mc.identificationnumber,
  mc.rightsholder AS rightsholderRefname,
  mc.rightsholder AS rightsholder,
  mc.contributor,
  mb.imageNumber

FROM relations_common rc
  JOIN misc mrc on rc.id = mrc.id	-- exclude deleted relations

  JOIN hierarchy hcc ON rc.objectcsid = hcc.name
  JOIN collectionobjects_common cc ON hcc.id = cc.id
  JOIN misc mcc on cc.id = mcc.id	-- eclude deleted collection objects

  JOIN hierarchy hmc ON rc.subjectcsid = hmc.name
  JOIN media_common mc ON hmc.id = mc.id
  JOIN misc mmc ON mc.id = mmc.id	-- exclude deleted media

  JOIN hierarchy hbc ON mc.blobcsid = hbc.name
  JOIN blobs_common bc on (hbc.id = bc.id)
  JOIN misc mbc ON bc.id = mbc.id	-- exclude deleted blobs

  LEFT OUTER JOIN media_bampfa mb ON mc.id = mb.id

WHERE mrc.lifecyclestate <> 'deleted'
AND mcc.lifecyclestate <> 'deleted'
AND mmc.lifecyclestate <> 'deleted'
AND mbc.lifecyclestate <> 'deleted'

ORDER BY mb.imageNumber ASC
