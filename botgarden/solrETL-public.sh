#!/bin/bash -x
date
##############################################################################
# while most of this script is already tenant specific, many of the specific commands
# are shared between the different scripts; having them be as similar as possible
# eases maintainance. ergo, the TENANT parameter
##############################################################################
source ${HOME}/pipeline-config.sh
TENANT=$1
CORE=public
SERVER="${BOTGARDEN_SERVER}"
USERNAME="${BOTGARDEN_USER}"
DATABASE="${TENANT}_domain_${TENANT}"
CONNECTSTRING="host=$SERVER dbname=$DATABASE sslmode=prefer"
CONTACT="${BOTGARDEN_CONTACT}"
##############################################################################
cd ${HOME}/solrdatasources/${TENANT}
##############################################################################
# extract metadata (dead and alive) and media info from CSpace
##############################################################################
time psql -F $'\t' -R"@@" -A -U $USERNAME -d "$CONNECTSTRING" -f botgardenMetadataV1alive.sql -o d1a.csv &
time psql -F $'\t' -R"@@" -A -U $USERNAME -d "$CONNECTSTRING" -f botgardenMetadataV1dead.sql -o d1b.csv &
time psql -F $'\t' -R"@@" -A -U $USERNAME -d "$CONNECTSTRING" -f media.sql  -o i4.csv &
wait
# some fix up required, alas: data from cspace is dirty: contain csv delimiters, newlines, etc. that's why we used @@ as temporary record separator
time perl -pe 's/[\r\n]/ /g;s/\@\@/\n/g' d1b.csv > d2b.csv &
time perl -pe 's/[\r\n]/ /g;s/\@\@/\n/g' d1a.csv > d2a.csv &
time perl -pe 's/[\r\n]/ /g;s/\@\@/\n/g' i4.csv > 4solr.${TENANT}.media.csv &
wait
cat d2b.csv d2a.csv > d2.csv
time perl -ne 'print unless /\(\d+ rows\)/' d2.csv > d3.csv
##############################################################################
# count the number of columns in each row, solr wants them all to be the same
##############################################################################
time python3 ../common/evaluate.py d3.csv d4.csv > ${TENANT}.counts.${CORE}.rawdata.csv
##############################################################################
# check latlongs
##############################################################################
perl -ne '@y=split /\t/;@x=split ",",$y[17];print if  (abs($x[0])<90 && abs($x[1])<180);' d4.csv > d5.csv &
perl -ne '@y=split /\t/;@x=split ",",$y[17];print if !(abs($x[0])<90 && abs($x[1])<180);' d4.csv > ${TENANT}.errors_in_latlong.csv &
wait
##############################################################################
# temporary hack to parse Locality into County/State/Country
##############################################################################
# 1. recover latest version of these 3 files, which were zipped via the previous run of one_job.sh
gunzip -c ${SOLR_CACHE_DIR}/county.csv.gz > county.csv
gunzip -c ${SOLR_CACHE_DIR}/state.csv.gz > state.csv
gunzip -c ${SOLR_CACHE_DIR}/country.csv.gz > country.csv

# parse the extracted field, insert into metadata
perl fixLocalites.pl d5.csv > metadata.csv
# 3. we need to regenerate and save these 3 files for the next run...
cut -f10 metadata.csv | perl -pe 's/\|/\n/g;' | sort | uniq -c | perl -pe 's/^ *(\d+) /\1\t/' > county.csv &
cut -f11 metadata.csv | perl -pe 's/\|/\n/g;' | sort | uniq -c | perl -pe 's/^ *(\d+) /\1\t/' > state.csv &
cut -f12 metadata.csv | perl -pe 's/\|/\n/g;' | sort | uniq -c | perl -pe 's/^ *(\d+) /\1\t/' > country.csv &
rm d3.csv i4.csv
##############################################################################
# make a unique sequence number for id (accession csid is not unique in this extract)
##############################################################################
perl -i -pe '$i++;print $i . "\t"' metadata.csv
##############################################################################
# parse scientific names into parts using GBIF (and cache results in pickle)
##############################################################################
# try to make sure names.pickle is there. it is slow to recreate
if [[ ! -e gbif/names.pickle ]]; then
  echo "names.pickle not found in runtime directory; attempting to retrieve copy from backup"
  cp ${SOLR_CACHE_DIR}/names.pickle gbif/names.pickle
fi
python3 gbif/parseAndInsertGBIFparts.py metadata.csv metadata+parsednames.csv gbif/names.pickle 3
# put the latest and greatest version of names.pickle into the solr cache
cp gbif/names.pickle ${SOLR_CACHE_DIR}/names.pickle
##############################################################################
# we want to recover and use our "special" solr-friendly header, which got buried
##############################################################################
grep -P "^1\tid\t" metadata+parsednames.csv | head -1 > header4Solr.csv
perl -i -pe 's/^1\tid/id\tobjcsid_s/' header4Solr.csv
perl -i -pe 's/\r//;s/$/\tblob_ss/' header4Solr.csv
grep -v -P "^1\tid\t" metadata+parsednames.csv > d7.csv
python3 fixfruits.py d7.csv > ${CORE}.metadata.csv
##############################################################################
# eliminate restricted items from ${CORE} dataset
# nb: at this time we're not doing this, instead we are obfuscating their
# garden locations in a step further below...
##############################################################################
#perl -i -ne '@x = split /\t/;print unless $x[59] =~ /Restricted/' d4.csv
##############################################################################
# up to here, both ${CORE} and internal extracts are the same.
##############################################################################
# obfuscate locations of sensitive accesssions
##############################################################################
perl -ne '@x = split /\t/;if ($x[58] =~ /Restricted/) {@x[38] = "Location Restricted" if  @x[38] ne "" ; @x[22]="Undisclosed"  if  @x[22] ne "";  @x[31]=""}; print join "\t",@x;' ${CORE}.metadata.csv > d8.csv
##############################################################################
# add the blob csids
##############################################################################
time perl mergeObjectsAndMedia.pl 4solr.${TENANT}.media.csv d8.csv ${CORE} > d9.csv
cat header4Solr.csv d9.csv | perl -pe 's/␥/|/g' > d10.csv
##############################################################################
# compute _i values for _dt values (to support BL date range searching)
##############################################################################
time python3 computeTimeIntegers.py d10.csv 4solr.${TENANT}.${CORE}.csv
# shorten this one long org name...
perl -i -pe 's/International Union for Conservation of Nature and Natural Resources/IUCN/g' 4solr.${TENANT}.${CORE}.csv
##############################################################################
# check high water mark for storage use; get rid of intermediate files
##############################################################################
../common/check_space.sh
rm -f d?.csv d??.csv
##############################################################################
# save (hide) files needed for the internal core so that the internal pipeline can find them
##############################################################################
gzip 4solr.${TENANT}.media.csv
gzip ${CORE}.metadata.csv
gzip header4Solr.csv
##############################################################################
# OK, we are good to go! clear out the existing data and reload
##############################################################################
../common/post_to_solr.sh ${TENANT} ${CORE} ${CONTACT}  50000 74
date
