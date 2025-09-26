/*
-- Remove the following unused references to 
-- collectionobjects_common.id
-- collectionobjects_common.computedcurrentlocation
-- updated collectionobjects_common.numberofobjects to objectcountgroup.objectcount
-- CineFiles does not use objectcountgroup as repeating group, does not use objectcounttype
*/

SELECT
  hcc.name AS metadata_id,
  ocg.objectcount AS numberofobjects,
  getdispl(cc.collection) AS collection,
  cc.collection AS collection_refname,
  cc.distinguishingfeatures,
  cc.recordstatus,
  cf.hasbiblio,
  getdispl(cf.doctype) AS doctype,
  cf.doctype AS doctype_refname,
  cf.doctitle,
  cf.hasdistco,
  cf.doctitlearticle,
  cf.hasillust,
  cf.hasprodco,
  cf.hasfilmog,
  getdispl(cf.source) AS source,
  cf.source AS source_refname,
  cf.pageinfo,
  cf.hascastcr,
  cf.hascostinfo,
  cf.accesscode,
  cf.hastechcr,
  cf.docdisplayname,
  cf.hasboxinfo,
  cc.objectnumber AS doc_id,
  regexp_replace(cc.contentnote, E'[\\t\\n\\r]+', ' ', 'g') AS canonical_url
FROM collectionobjects_common cc
JOIN misc mcc ON (cc.id = mcc.id AND mcc.lifecyclestate != 'deleted')
JOIN hierarchy hcc ON cc.id = hcc.id
LEFT OUTER JOIN collectionobjects_cinefiles cf ON cc.id = cf.id
LEFT OUTER JOIN hierarchy hocg ON (
  cc.id = hocg.parentid
  AND hocg.pos = 0
  AND hocg.primarytype = 'objectCountGroup')
LEFT OUTER JOIN objectcountgroup ocg ON (hocg.id = ocg.id);
