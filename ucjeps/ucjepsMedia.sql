----------------------------------------------------------------------------------------------------
-- ucjepsMedia.sql
-- get public media and blob data for collection objects (posttopublic = 'yes')
-- excludes deleted media records (media_common != 'deleted')
-- does not exclude deleted collection object records
-- removed unreferenced join to collectionobjects_ucjeps
-- ~841K rows new query
----------------------------------------------------------------------------------------------------

select
  hcc.name as objectcsid,
  cc.objectnumber,
  hmc.name as mediacsid,
  mc.description,
  bc.name,
  mc.creator as creatorRefname,
  getdispl(mc.creator) as creator,
  mc.blobcsid,
  mc.copyrightstatement,
  mc.identificationnumber,
  mc.rightsholder as rightsholderRefname,
  getdispl(mc.rightsholder) as rightsholder,
  mc.contributor,
  csc.updatedat as updatedat_dt

from media_common mc
  join misc mmc on mc.id = mmc.id

  -- get public media
  left outer join media_ucjeps mu on (
    mc.id = mu.id
    and mu.posttopublic = 'yes')

  -- get blobs
  join hierarchy hbc on (
    mc.blobcsid = hbc.name
    and hbc.primarytype = 'Blob')
  left outer join blobs_common bc on hbc.id = bc.id

  -- get collection objects
  left outer join hierarchy hmc on mc.id = hmc.id
  join relations_common rc on (
    hmc.name = rc.subjectcsid
    and rc.subjectdocumenttype = 'Media'
    and rc.objectdocumenttype = 'CollectionObject')
  left outer join hierarchy hcc on rc.objectcsid = hcc.name
  left outer join collectionobjects_common cc on hcc.id = cc.id
  left outer join collectionspace_core csc on cc.id = csc.id

where mmc.lifecyclestate <> 'deleted';
