#!/usr/bin/env python
#
# Read docs from the optimized CouchDB, load them in Solr
# Deletes all documents in Solr before adding them!
#
import requests, json

COUCHDB_SUMMARIES_URL = 'http://localhost:5984/block_summaries'
COUCHDB_RELATED_URL = 'http://localhost:5984/related_blocks'
COUCHDB_HINTS_URL = 'http://localhost:5984/block_hints'

SOLR_URL = 'http://localhost:8983/solr'

COUCHDB_BULK_SIZE = 10000
COUCHDB_NUM_KEYS_IN_GETS = 200

related_blocks_already_processed = set()

# there are duplicates, because we store both the reverse and the forward of every two-block sequence,
# so we need to be careful not to insert duplicates
def already_processed_related(related_block):
  if ('_id' not in related_block['doc'] or '~' not in related_block['doc']['_id']): # not a related block
    return False
  
  unique_id = tuple(sorted([int(related_block['doc']['_id'].split('~')[0]),related_block['doc']['block']]))
  
  return unique_id in related_blocks_already_processed
  
def mark_related_as_processed(related_block):
  if ('_id' not in related_block['doc'] or '~' not in related_block['doc']['_id']): # not a related block
    return False
    
  unique_id = tuple(sorted([int(related_block['doc']['_id'].split('~')[0]),related_block['doc']['block']]))
  related_blocks_already_processed.add(unique_id)

def enhance_with_full_hints(rows):
  # fetch hints from the block_hints database if necessary
  keys = map(lambda row : row['doc']['_id'], filter((lambda row : 'hintsRedacted' in row['doc'] and row['doc']['hintsRedacted']),rows))
  
  if (len(keys) == 0): # no hints redacted
    return rows;
  
  # fetch in batches because there are limits to how many keys you can stuff in an http get
  rows = []
  for i in range(0, len(keys), COUCHDB_NUM_KEYS_IN_GETS):
    
    limit = min(len(keys), i + COUCHDB_NUM_KEYS_IN_GETS)
    params = {'keys' : json.dumps(keys[i:limit],separators=(',',':')), 'include_docs' : 'true'}
    
    rows += requests.get(COUCHDB_HINTS_URL + '/_all_docs', params=params).json()['rows']
    
  
  ids_to_hints = dict(map(lambda row : (row['key'], row['doc']['hintMap']), rows))
  
  for row in rows:
    if 'hintsRedacted' in row['doc'] and row['doc']['hintsRedacted']:
      row['doc']['hintMap'] = ids_to_hints[row['doc']['_id']]

  
def main():
  
  print "Deleting all docs in Solr"
  delete_response = requests.post(SOLR_URL + '/update/json', params={'commit' : 'true'},\
      headers={'Content-Type' : 'application/json'},data=json.dumps({'delete' : {'query' : '*:*'}}))
  print "Response was",delete_response.status_code

  for (couchdb_url, doc_type) in zip((COUCHDB_SUMMARIES_URL, COUCHDB_RELATED_URL), ('summary', 'related')):
    params = {'limit' : COUCHDB_BULK_SIZE,'include_docs' : 'true'}
  
    def couch_row_to_solr_doc(row):
      doc = row['doc']

      # repeat the repeated hints so solr can weight them properly
      hints = []
      for (hint, count) in doc['hintMap'].items():
        for i in range(count):
          hints.append(hint)

      return {\
          'id' : doc['_id'],\
          'hints' : hints,\
          'docType' : doc_type,\
          'popularity' : doc['count'] if doc_type == 'related' else (doc['soloHintCount'] + doc['followingHintCount'])}  
  
    while True:
      response = requests.get(couchdb_url + '/_all_docs', params=params)
      print "called couch %s, got response code %d" % (couchdb_url, response.status_code)
      rows = response.json()['rows']
    
      if len(rows) == 0:
        break
          
      # filter design documents and hintless documents
      filtered_rows = filter(lambda row : ('hintMap' in row['doc']) and (len(row['doc']['hintMap'].keys()) > 0) and (not already_processed_related(row)), rows)
    
      for row in filtered_rows:
        mark_related_as_processed(row)
    
      enhanced_rows = enhance_with_full_hints(filtered_rows)
    
      solr_docs = map(couch_row_to_solr_doc, filtered_rows)
  
      solr_response = requests.post(SOLR_URL + '/update/json', params={'commit' : 'true'},\
          headers={'Content-Type' : 'application/json'},data=json.dumps({'add' : solr_docs}))
    
      print "Posted %d docs to solr, response was %d" % (len(solr_docs), solr_response.status_code)
    
      if len(rows) < COUCHDB_BULK_SIZE: # done reading from couchdb
        break
    
      last_row = rows[-1]
      params.update({'startkey'  : json.dumps(last_row['doc']['_id']), 'skip' : 1})
  print "Optimized solr, response was %d" % (requests.get(SOLR_URL + '/update?commit=true&optimize=true').status_code)

if __name__=='__main__':
  main()
