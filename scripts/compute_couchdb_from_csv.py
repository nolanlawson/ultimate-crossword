#!/usr/bin/env python
#
# create a couchdb database from the given cred.csv file in the local directory
#
# 
#

import sys, re, getopt, csv, requests, json
import gevent.monkey
gevent.monkey.patch_socket()
from gevent.pool import Pool

MIN_BLOCK_COUNT = 2
BATCH_SIZE = 1000000
COUCHDB_BULK_INSERT_SIZE = 10000 # per http://dev.svetlyak.ru/couchdb-bulk-inserts-performance-en/
POOL_SIZE = 100
NUM_RETRIES = 10

COUCHDBS = ['http://localhost:5984/blocks_sharded',\
    'http://localhost:5985/blocks_sharded',\
    'http://localhost:5986/blocks_sharded',\
    'http://admin:password@koholint-wired:5985/blocks_sharded',\
    'http://koholint-wired:5986/blocks_sharded',\
]

current_couch_idx = 0

TOTAL_NUM_LINES = 153004874

design_documents = [
{
  '_id'   : '_design/blocks_to_hints',\
  'views' : {
    'blocks_to_hints' : {
      'language' : 'javascript',
      'map'    : '''
      function(doc) {
        if (doc.type === 'block_hint') {
          emit([doc.blocks, false], null);
          if (doc.blocks.length == 2) {
            emit([[doc.blocks[1], doc.blocks[0]], true], null);
          }
        }
      }
      ''',
    }
  }
},
{
   "_id" : "_design/blocks_to_counts",
   "language": "javascript",
   "views": {
       "blocks_to_counts": {
           "map": '''
           function(doc) {
             if (doc.type === 'block_count') {
               emit(doc.block, doc.count);
             }
           }
           ''',
           "reduce": '_sum'
       }
   }
}
]

csvfile = open('cred.csv','rb')
csvreader = csv.reader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)

bogus_hint_pattern = re.compile('^[\s\?]+$') # hints were incorrectly decoded from UTF-8; some are just question marks

def add_blocks(blocks, new_blocks):
  
  for new_block in new_blocks:
    try:
      blocks[new_block] += 1
    except KeyError:
      blocks[new_block] = 1

def add_hints(hints, new_blocks, hint):
  try:
    hints[new_blocks].append(hint)
  except KeyError:
    hints[new_blocks] = [hint]

def analyze_row(row, blocks, hints):
  if (len(row) != 5):
    return
  
  (adobe_id, adobe_username, email, password, hint) = row
  
  if (not hint or not password or bogus_hint_pattern.match(hint)):
    return
    
  password_len = len(password)
  
  # ignore passwords that are longer; I'm not sure how to divide them into blocks, and there
  # aren't very many of them, anyway
  if (password_len != 12 and password_len != 24):
    return
  elif (password_len == 12):
    new_blocks = (password[:11],)
  else: # 24
    new_blocks = (password[:11], password[11:22])
  
  add_blocks(blocks, new_blocks)
  add_hints(hints, new_blocks, hint)
  
  
def create_docs(blocks, hints):
  return [{'type' : 'block_count', 'block' : block[0], 'count' : block[1]} for block in blocks] +\
      [{'type' : 'block_hint', 'blocks' : hint[0], 'hints' : hint[1]} for hint in hints]

def bulk_insert_to_couchdb(docs):
  global current_couch_idx

  def post((couchdb_url, docs)):
    for i in range(NUM_RETRIES):
      try:
        response = requests.post(couchdb_url + '/_bulk_docs',data=json.dumps({'docs' : docs}),headers={"Content-Type":"application/json"})
        print " > posted %d docs to CouchDB %s, response code: %d" % (len(docs), couchdb_url, response.status_code)
        break
      except requests.exceptions.ConnectionError:
        print "Connection error at %s, retrying for %dnth time" % (couchdb_url, i)
  
  urls_and_docs = []
  
  for i in range(0, len(docs), COUCHDB_BULK_INSERT_SIZE):
    
    # round-robin choose a couchdb
    couchdb_url = COUCHDBS[current_couch_idx]
    current_couch_idx += 1
    if (current_couch_idx == len(COUCHDBS)):
      current_couch_idx = 0
    
    limit = min(i + COUCHDB_BULK_INSERT_SIZE, len(docs))
    
    urls_and_docs.append((couchdb_url, docs[i:limit]))
  
  print "we have %d tasks to execute for %d CouchDBs, spawning..." % (len(urls_and_docs), len(COUCHDBS))
  
  pool = Pool(POOL_SIZE)

  for url_and_docs in urls_and_docs:
    pool.spawn(post, url_and_docs);
  pool.join()

  print "Finished posting"

def process_batch(rows):

  # computed statistics about blocks and hints
  blocks = {}
  hints = {}

  for row in rows:
    analyze_row(row, blocks, hints)

  blocks = dict(filter((lambda (block,count) : count >= MIN_BLOCK_COUNT), blocks.items()))
  
  print "found %d blocks and %d hints" % (len(blocks), len(hints))
  
  docs = create_docs(blocks.items(), hints.items())
  bulk_insert_to_couchdb(docs)

def create_database():
  
  # drop and re-create
  for couchdb_url in COUCHDBS:
    print 'dropping database in %s, response is %d' % (couchdb_url, requests.delete(couchdb_url).status_code)
    print 'creating database in %s, response is %d' % (couchdb_url, requests.put(couchdb_url).status_code)
  
    # post design documents to couchdb
    
    for design_doc in design_documents:
      response = requests.put(couchdb_url + '/' + design_doc['_id'],data=json.dumps(design_doc),headers={'Content-Type':'application/json'})
      print 'posted design doc %s to CouchDB %s, got response %d' % (design_doc['_id'], couchdb_url, response.status_code)
  
def main():

  create_database()
    
  numlines = 0
  buffer = []

  for row in csvreader:
    buffer.append(row)
    if len(buffer) == BATCH_SIZE:
      process_batch(buffer)
      del buffer[:]
      numlines += BATCH_SIZE

      print 'processed %d/%d (%.2f%%) lines from cred.csv file so far...' % (numlines, TOTAL_NUM_LINES, (numlines * 100.0 / TOTAL_NUM_LINES))

  if len(buffer) > 0:
    process_batch(buffer)

if __name__=='__main__':
  main()
