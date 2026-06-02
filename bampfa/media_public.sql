/*
-- media_public.sql
--
-- get collection objects with media and blob data, where relations, objects, media, and blobs are not deleted.
-- and not flagged for 'No public display'
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
  JOIN misc mrc on rc.id = mrc.id

  JOIN hierarchy hcc ON rc.objectcsid = hcc.name
  JOIN collectionobjects_common cc ON hcc.id = cc.id
  JOIN misc mcc on cc.id = mcc.id

  JOIN hierarchy hmc ON rc.subjectcsid = hmc.name
  JOIN media_common mc ON hmc.id = mc.id
  JOIN misc mmc ON mc.id = mmc.id

  JOIN hierarchy hbc ON mc.blobcsid = hbc.name
  JOIN blobs_common bc on hbc.id = bc.id
  JOIN misc mbc ON bc.id = mbc.id

  LEFT OUTER JOIN media_bampfa mb ON mc.id = mb.id

WHERE mrc.lifecyclestate <> 'deleted'	-- exclude deleted relations
  AND mcc.lifecyclestate <> 'deleted'	-- exclude deleted collection objects
  AND mmc.lifecyclestate <> 'deleted'	-- exclude deleted media
  AND mbc.lifecyclestate <> 'deleted'	-- exclude deleted blobs
  AND mb.websitedisplaylevel IS DISTINCT FROM 'No public display'	-- include public display media records

ORDER BY mb.imageNumber ASC
