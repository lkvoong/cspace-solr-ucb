-- Revisions
-- keep *_common to *_botgarden loj as there are a few records in *_common that are not in *_botgarden
-- exclude deleted relations, collectionObjects, blobs
-- currently, media to blob is 1:1, but keeping distinct CTE to handle deleted blobs

with objects_media as (
  select
    rc.objectcsid as objectcsid,
    hcc.id as objectid,
    rc.subjectcsid as mediacsid,
    hmc.id as mediaid
  from relations_common rc
  join misc mrc on (
    rc.id = mrc.id 
    and mrc.lifecyclestate != 'deleted')
  join hierarchy hcc on (
    rc.objectcsid = hcc.name
    and rc.objectdocumenttype = 'CollectionObject'
    and hcc.primarytype = 'CollectionObjectTenant35')
  join misc mcc on (
    hcc.id = mcc.id 
    and mcc.lifecyclestate != 'deleted')
  join hierarchy hmc on (
    rc.subjectcsid = hmc.name
    and rc.subjectdocumenttype = 'Media'
    and hmc.primarytype = 'MediaTenant35')
  join misc mmc on (
    hmc.id = mmc.id  
    and mmc.lifecyclestate != 'deleted')
),

objectlist as (
  select distinct objects_media.objectid
  from objects_media
),

objects as (
  select
    objectlist.objectid,
    cc.objectnumber
  from objectlist
  join collectionobjects_common cc on (objectlist.objectid = cc.id)
),

medialist as (
  select distinct objects_media.mediaid
  from objects_media
),

media as (
  select
    medialist.mediaid,
    mc.description,
    mc.creator as creatorRefname,
    getdispl(mc.creator) as creator,
    mc.blobcsid,
    mc.copyrightstatement,
    mc.identificationnumber,
    mc.rightsholder as rightsholderRefname,
    getdispl(mc.rightsholder) as rightsholder,
    mc.contributor,
    mb.imageNumber,
    mb.posttopublic
  from medialist
  join media_common mc on (medialist.mediaid = mc.id)
  left outer join media_botgarden mb on (mc.id = mb.id)
),

bloblist as (
  select distinct media.blobcsid
  from media
),

blobs as (
  select
    hbc.name as blobcsid,
    bc.name
  from bloblist
  join hierarchy hbc on (bloblist.blobcsid = hbc.name)
  join blobs_common bc ON (hbc.id = bc.id)
  join misc mbc on (
    bc.id = mbc.id 
    and mbc.lifecyclestate != 'deleted')
)

select
  objects_media.objectcsid,
  objects.objectnumber,
  objects_media.mediacsid,
  media.description,
  blobs.name,
  media.creatorRefname,
  media.creator,
  media.blobcsid,
  media.copyrightstatement,
  media.identificationnumber,
  media.rightsholderRefname,
  media.rightsholder,
  media.contributor,
  media.posttopublic
from objects_media
left outer join objects on (objects_media.objectid = objects.objectid)
left outer join media on (objects_media.mediaid = media.mediaid)
left outer join blobs on (media.blobcsid = blobs.blobcsid)
order by media.imageNumber asc
;
