#!/usr/bin/env python
#
# Compute a single CouchDB database that simply contains a mapping of blocks to (new) integer IDs.  All blocks are unique.
#
# This fixes the fact that I can no longer actually use the blocks_to_counts view in the original database as my reducing function,
# because now I have multiple CouchDBs and I can't reduce across all of them.  Fuuuuuuuudge.
# 

import requests, json, sys, base64, re, itertools

COUCHDB_BULK_INSERT_SIZE = 10000

INPUT_COUCHDBS = ['http://localhost:5984/blocks_sharded',\
    'http://localhost:5985/blocks_sharded',\
    'http://localhost:5986/blocks_sharded',\
    'http://admin:password@koholint-wired:5985/blocks_sharded',\
    'http://koholint-wired:5986/blocks_sharded',\
]

OUTPUT_COUCHDB = 'http://localhost:5984/block_ids'

int_id_counter = 0

def convert_row_to_doc(row):
  global int_id_counter
  int_id_counter += 1
  return {'intId' : int_id_counter, '_id' : row['key']}

def main():
  
  # drop and re-create
  
  print 'dropping database %s, response is %s' % (OUTPUT_COUCHDB, requests.delete(OUTPUT_COUCHDB).status_code)
  print 'creating database %s, response is %s' % (OUTPUT_COUCHDB, requests.put(OUTPUT_COUCHDB).status_code)
    
  for input_couchdb in INPUT_COUCHDBS:
    print "working on input couchdb", input_couchdb
    
    params = {'group' : 'true', 'reduce' : 'true', 'limit' : COUCHDB_BULK_INSERT_SIZE}  
    while True:
    
      rows = requests.get(input_couchdb + '/_design/blocks_to_counts/_view/blocks_to_counts',params=params).json()['rows']
  
      print "fetched %d rows" % (len(rows))
  
      if (len(rows) == 0):
        break
  
      docs = map(convert_row_to_doc, rows)
      
      # I don't care if any fail; it will get putted eventually
      response = requests.post(OUTPUT_COUCHDB + '/_bulk_docs',data=json.dumps({docs : docs}),headers={'Content-Type':'application/json'})
      print "tried to put %d docs, response was %d" % (len(docs), response.status_code)
    
      params.update({'skip' : 1, 'startkey' : json.dumps(rows[-1]['key'])})
  
if __name__=='__main__':
  main()