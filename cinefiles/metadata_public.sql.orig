select  distinct
        h1.name as metadata_id,
        /* coc.id as id2, */
        coc.numberofobjects,
        /* coc.computedcurrentlocation, */
        regexp_replace(coc.collection, '^.*\)''(.*)''$', '\1') AS collection,
        coc.collection AS collection_refname,
        coc.distinguishingfeatures,
        coc.recordstatus,
        cocf.hasbiblio,
        regexp_replace(cocf.doctype, '^.*\)''(.*)''$', '\1') AS doctype,
        cocf.doctype AS doctype_refname,
        cocf.doctitle,
        cocf.hasdistco,
        cocf.doctitlearticle,
        cocf.hasillust,
        cocf.hasprodco,
        cocf.hasfilmog,
        regexp_replace(cocf.source, '^.*\)''(.*)''$', '\1') AS source,
        cocf.source as source_refname,
        cocf.pageinfo,
        cocf.hascastcr,
        cocf.hascostinfo,
        cocf.accesscode,
        cocf.hastechcr,
        cocf.docdisplayname,
        cocf.hasboxinfo,
        coc.objectnumber AS doc_id,
        regexp_replace(coc.contentnote, E'[\\t\\n\\r]+', ' ', 'g') AS canonical_url
from collectionobjects_common coc
left outer join hierarchy h1 on (h1.id = coc.id)
left outer join collectionobjects_cinefiles cocf on (coc.id = cocf.id)
left outer join misc m on (coc.id = m.id and m.lifecyclestate != 'deleted')
group by h1.id,coc.id,cocf.id;
