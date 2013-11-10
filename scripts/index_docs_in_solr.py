#!/usr/bin/env python
#
# Read docs from the optimized CouchDB, load them in Solr
# Deletes all documents in Solr before adding them!
#
import requests, json

COUCHDB_SUMMARIES_URL = 'http://localhost:5984/block_summaries2'
COUCHDB_RELATED_URL = 'http://localhost:5984/related_blocks2'
SOLR_URL = 'http://localhost:8983/solr'

COUCHDB_BULK_SIZE = 10000

print "Deleting all docs in Solr"
delete_response = requests.post(SOLR_URL + '/update/json', params={'commit' : 'true'},\
    headers={'Content-Type' : 'application/json'},data=json.dumps({'delete' : {'query' : '*:*'}}))
print "Response was",delete_response.status_code

for (couchdb_url, doc_type) in zip((COUCHDB_SUMMARIES_URL, COUCHDB_RELATED_URL), ('summary', 'related')):
  
  def couch_row_to_solr_doc(row):
    doc = row['doc']
    return {\
        'id' : doc['_id'],\
        'hints' : doc['hints'],\
        'docType' : doc_type,\
        'popularity' : doc['count'] if doc_type == 'related' else (doc['soloCount'] + doc['followingCount'])}
  
  params = {'limit' : COUCHDB_BULK_SIZE,'include_docs' : 'true'}
  
  while True:
    response = requests.get(couchdb_url + '/_all_docs', params=params)
    print "called couch %s, got response code %d" % (couchdb_url, response.status_code)
    rows = response.json()['rows']
    
    if len(rows) == 0:
      break
          
    # filter design documents and hintless documents
    filtered_rows = filter(lambda row : ('hints' in row['doc']) and (len(row['doc']['hints']) > 0), rows)
    
    solr_docs = map(couch_row_to_solr_doc, filtered_rows)
  
    solr_response = requests.post(SOLR_URL + '/update/json', params={'commit' : 'true'},\
        headers={'Content-Type' : 'application/json'},data=json.dumps({'add' : solr_docs}))
    
    print "Posted %d docs to solr, response was %d" % (len(solr_docs), solr_response.status_code)
    print solr_response.text
    
    if len(rows) < COUCHDB_BULK_SIZE: # done reading from couchdb
      break
    
    last_row = rows[-1]
    params.update({'startkey'  : json.dumps(last_row['doc']['_id']), 'skip' : 1})
