#!/usr/bin/env python
#
# Read docs from the optimized CouchDB, load them in Solr
#
import requests, json

COUCHDB_SUMMARIES_URL = 'http://localhost:5984/block_summaries2'
COUCHDB_RELATED_URL = 'http://localhost:5984/related_blocks2'
SOLR_URL = 'http://localhost:8983/solr'

COUCHDB_BULK_SIZE = 10000

for (couchdb_url, doc_type) in zip((COUCHDB_SUMMARIES_URL, COUCHDB_RELATED_URL), ('summary', 'related')):
  params = {limit : COUCHDB_BULK_SIZE}
  while True:
    response = requests.get(couchdb_url + '/_all_docs', params=params)
    print "called couch, got response code ", response.status_code
    rows = response.json()['rows']
    
    if len(rows) < COUCHDB_BULK_SIZE: # done reading from couchdb
      break
      
    solr_docs = map(lambda row : {'id' : row['_id'], 'hints' : row['hints'], 'docType' : doc_type}, rows)
  
    requests.post(SOLR_URL + '/update/json', params={'commit' : 'true'},headers={'Content-Type' : 'application/json'},data=json.dumps(solr_docs))
  
    last_row = rows[-1]
    params.update({'startkey'  : last_row['_id'], 'skip' : 1})