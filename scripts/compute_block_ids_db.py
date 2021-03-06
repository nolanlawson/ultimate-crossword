#!/usr/bin/env python
#
# Compute a single CouchDB database that simply contains a mapping of blocks to (new) integer IDs.  All blocks are unique.
#
# This fixes the fact that I can no longer actually use the blocks_to_counts view in the original database as my reducing function,
# because now I have multiple CouchDBs and I can't reduce across all of them.  Fuuuuuuuudge.
# 

import requests, json, sys, re, itertools
import gevent.monkey
gevent.monkey.patch_socket()
from gevent.pool import Pool

COUCHDB_BULK_INSERT_SIZE = 10000

INPUT_COUCHDBS = ['http://localhost:5984/blocks_sharded',\
    'http://localhost:5985/blocks_sharded',\
    'http://localhost:5986/blocks_sharded',\
    'http://admin:password@koholint-wired:5985/blocks_sharded',\
    'http://koholint-wired:5986/blocks_sharded',\
]

OUTPUT_COUCHDB = 'http://localhost:5984/block_ids'

# using a hash instead of a plain counter because I don't want the ids to get up to over 6 digits; there are less than 1000000
int_ids = {}
int_id_counter = 0

def convert_row_to_doc(row):
  global int_ids
  global int_id_counter
  
  block = row['key']
  try:
    int_id = int_ids[block]
    return None
  except KeyError:
    int_id_counter += 1
    int_ids[block] = int_id_counter
    result = {'intId' : int_id_counter, '_id' : block}
    return result

def process_docs_from_couchdb(input_couchdb):
  print "working on input couchdb", input_couchdb
  
  params = {'group' : 'true', 'reduce' : 'true', 'limit' : COUCHDB_BULK_INSERT_SIZE}  
  while True:
  
    rows = requests.get(input_couchdb + '/_design/blocks_to_counts/_view/blocks_to_counts',params=params).json()['rows']

    print " > %s > fetched %d rows" % (input_couchdb, len(rows))

    if (len(rows) == 0):
      break

    docs = filter((lambda x : x is not None), map(convert_row_to_doc, rows))
    
    # I don't care if any fail; it will get putted eventually
    response = requests.post(OUTPUT_COUCHDB + '/_bulk_docs',data=json.dumps({'docs' : docs}),headers={'Content-Type':'application/json'})
    num_dups = sum([1 for row in filter(lambda row: 'error' in row, response.json())]) if str(response.status_code).startswith('2') else []
    print " > %s > tried to put %d docs, response was %d, found %d duplicates" % (input_couchdb, len(docs), response.status_code, num_dups)
  
    params.update({'skip' : 1, 'startkey' : json.dumps(rows[-1]['key'])})
  print " > %s > Done!" % (input_couchdb)

def main():
  
  # drop and re-create
  
  print 'dropping database %s, response is %s' % (OUTPUT_COUCHDB, requests.delete(OUTPUT_COUCHDB).status_code)
  print 'creating database %s, response is %s' % (OUTPUT_COUCHDB, requests.put(OUTPUT_COUCHDB).status_code)
  
  # one thread for each db we're reading from
  pool = Pool(len(INPUT_COUCHDBS))
  
  for input_couchdb in INPUT_COUCHDBS:
    pool.spawn(process_docs_from_couchdb,input_couchdb)
  pool.join()
    
  
if __name__=='__main__':
  main()