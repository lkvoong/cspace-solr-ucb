select
  hcc.name as metadata_id,
  ocg.objectcount as numberofobjects,
  getdispl(cc.collection) as collection,
  cc.collection as collection_refname,
  cc.distinguishingfeatures,
  cc.recordstatus,
  cf.hasbiblio,
  getdispl(cf.doctype) as doctype,
  cf.doctype as doctype_refname,
  cf.doctitle,
  cf.hasdistco,
  cf.doctitlearticle,
  cf.hasillust,
  cf.hasprodco,
  cf.hasfilmog,
  getdispl(cf.source) as source,
  cf.source as source_refname,
  cf.pageinfo,
  cf.hascastcr,
  cf.hascostinfo,
  cf.accesscode,
  cf.hastechcr,
  cf.docdisplayname,
  cf.hasboxinfo,
  cc.objectnumber as doc_id,
  regexp_replace(cc.contentnote, E'[\\t\\n\\r]+', ' ', 'g') as canonical_url
from collectionobjects_common cc
join misc mcc on (cc.id = mcc.id and mcc.lifecyclestate != 'deleted')
join hierarchy hcc on (cc.id = hcc.id)
left outer join collectionobjects_cinefiles cf on (cc.id = cf.id)
left outer join hierarchy hocg on (
  cc.id = hocg.parentid
  and hocg.pos = 0
  and hocg.primarytype = 'objectCountGroup')
left outer join objectcountgroup ocg on (hocg.id = ocg.id)
;
