#!/usr/bin/env python
#
# Compute an optimized couchdb database from the given CouchDB database (generated from the load_data.py script)
#
# This db should be able to answer simple questions like:
# What are the most popular blocks?
# Given a block, what are the next/previous blocks and their associated hints?
# 

import requests, json, sys, re, itertools
import gevent.monkey
gevent.monkey.patch_socket()
from gevent.pool import Pool

MAX_NUM_HINTS_IN_SUMMARY = 30

COUCHDB_BULK_INSERT_SIZE = 500
COUCHDB_READ_SIZE = 50000

POOL_SIZE = 20
NUM_RETRIES = 10

# if true, sets stale to update_after
DEBUG_MODE = True

INPUT_COUCHDBS = ['http://localhost:5984/blocks_sharded',\
    'http://localhost:5985/blocks_sharded',\
    'http://localhost:5986/blocks_sharded',\
    'http://localhost:5987/blocks_sharded',\
    'http://localhost:5988/blocks_sharded',\
]

INPUT_BLOCK_IDS_DB = 'http://localhost:5984/block_ids'

OUTPUT_URL = 'http://localhost:5984/block_summaries'
OUTPUT_DETAILS_URL = 'http://localhost:5984/related_blocks'
OUTPUT_HINTS_URL = 'http://localhost:5984/block_hints'

design_documents = [
{
  '_id'   : '_design/counts_to_blocks',\
  'views' : {
    'counts_to_blocks' : {
      'language' : 'javascript',
      'map'    : '''
      function(doc) {
        var totalCount = doc.soloHintCount + doc.followingHintCount;
        emit(totalCount, null);
      }
      ''',
    }
  }
}]

blocks_to_ids = {}

def lookup_int_id(block):
  try:
    return str(blocks_to_ids[block])
  except KeyError:
    return None # block wasn't important enough to have its own id in the block_ids db, so skip it

def create_block_document(block, int_id, progress_indicator):

  namespace = {'all_block_hint_rows' : []}
  
  def get_block_hints_rows(input_url):
    
    block_hints_url = input_url + '/_design/blocks_to_hints/_view/blocks_to_hints'
    params = {'include_docs' : 'true', 'startkey' : json.dumps([[block]]), 'endkey' : json.dumps([[block, {}]])}
    if (DEBUG_MODE):
      params['stale'] = 'update_after'
    
    for i in range(NUM_RETRIES):
      try:
        block_hints = requests.get(block_hints_url, params=params).json()
        break
      except requests.exceptions.ConnectionError:
        print "Connection error at %s, retrying for %dnth time" % (block_hints_url, i)

    namespace['all_block_hint_rows'] += block_hints['rows']
  
  # one thread for each db
  pool = Pool(len(INPUT_COUCHDBS))
  for url in INPUT_COUCHDBS:
    pool.spawn(get_block_hints_rows, url)
  pool.join()
  
  result = {'_id' : int_id, 'hints' : [], 'precedingBlocks' : {}, 'followingBlocks' : {}}
  
  for block_hint in namespace['all_block_hint_rows']:
    (key, hints) = (block_hint['key'], block_hint['doc']['hints'])
    
    if len(key[0]) > 1: # has a related block
      (related_block, reverse_keys_and_values) = (key[0][1], key[1])
      
      key = 'precedingBlocks' if reverse_keys_and_values else 'followingBlocks';
      
      int_id = lookup_int_id(related_block)
      
      if int_id is None: # unimportant block
        continue
      
      try:
        result[key][int_id] += hints
      except KeyError:
        result[key][int_id] = hints
    else: # no related block; singleton only
      result['hints'] += hints
  
  progress_indicator['progress'] += 1
  sys.stdout.write(' > %.2f%%\r' % (progress_indicator['progress'] * 100.0 / progress_indicator['total']))
  sys.stdout.flush()
  if progress_indicator['progress'] == progress_indicator['total']:
    sys.stdout.write('\n')
    sys.stdout.flush()
    
  return result
  
  
def create_hint_map(hints):
  result = {}
  for hint in hints:
    try:
      result[hint] += 1
    except KeyError:
      result[hint] = 1
  return result

def reverse_keys_and_values((k,v)):
  return (v,k)

def add_doc_hints_if_necessary(doc, doc_hints):
  # put the hints in a separate db, because they take up too much room
  # also, model the hints as a map of strings to ints rather than a list
  hint_map = create_hint_map(doc['hints'])
  if (len(hint_map.keys()) > MAX_NUM_HINTS_IN_SUMMARY):
    sorted_hint_map = sorted(map(reverse_keys_and_values, hint_map.items()));
    sorted_hint_map.reverse()
    top_hints = dict(map(reverse_keys_and_values, sorted_hint_map[:MAX_NUM_HINTS_IN_SUMMARY]))
    doc['hintMap'] = top_hints
    doc['hintsRedacted'] = sum(map(lambda x:x[0],sorted_hint_map[MAX_NUM_HINTS_IN_SUMMARY:]))
    doc['hintsRedactedUnique'] = (len(hint_map.keys()) - MAX_NUM_HINTS_IN_SUMMARY)
    doc_hints.append({'_id' : doc['_id'], 'hintMap' : hint_map})
  else:
    doc['hintMap'] = hint_map
    doc['hintsRedacted'] = 0
    doc['hintsRedactedUnique'] = 0
  
  del doc['hints']

def split_doc_into_summary_and_details(doc):
  # split docs into an optimized summary/detail format and post
  # to three separate databases
  
  # create separate docs
  related_blocks = []
  for (otherBlocks, preceding) in ((doc['precedingBlocks'], True), (doc['followingBlocks'], False)):
    for (otherBlock, hints) in otherBlocks.items():
      related_blocks.append({'preceding' : preceding, 'hints' : hints, 'count' : len(hints), 'block' : int(otherBlock)})
  
  # sort by count
  related_blocks = sorted(related_blocks, key=lambda related_block : related_block['count'])
  related_blocks.reverse()
  
  
  # I realized only later that this count is inaccurate - it's a lower bound,
  # because I pruned with a minimum count of 2 in the other script
  # So in fact I can just count all the hints and it will give me the more accurate number,
  # since those weren't pruned
  doc['followingBlockCount'] = len(doc['followingBlocks'])
  doc['precedingBlockCount'] = len(doc['precedingBlocks'])
  doc['soloHintCount'] = len(doc['hints'])
  doc['followingHintCount'] = sum(map(lambda x : len(x), doc['followingBlocks'].values()))
  doc['precedingHintCount'] = sum(map(lambda x : len(x), doc['precedingBlocks'].values()))  
  # remove details from original doc
  del doc['precedingBlocks']
  del doc['followingBlocks']
  
  # put the hints in an entirely separate database, because they take up too much room  
  doc_hints = []
  add_doc_hints_if_necessary(doc, doc_hints)
    
  # apply an id to look like this : <blockId>~01, <blockId>~02
  # this means we can sort lexicographically in CouchDB by blockId, then number of hints (descending)
  max_id_len = len(str(len(related_blocks)))
  for i in range(len(related_blocks)):
    related_block = related_blocks[i]
    related_block['_id'] = doc['_id'] + "~" + str(i).zfill(max_id_len)
    add_doc_hints_if_necessary(related_block, doc_hints)
    
  return (doc, related_blocks, doc_hints)

def post_bulk(url, docs):
  
  response = requests.post(url + '/_bulk_docs',data=json.dumps({'docs' : docs}),headers={'Content-Type':'application/json'})
  print " > Posted %d documents to %s, response: %d" % (len(docs), url, response.status_code)
  if (str(response.status_code).startswith('4')): # error
    print " > > Got error", response.json()
  
def post_documents_to_couchdb(docs):
  
  summaries_and_details = map(split_doc_into_summary_and_details, docs);
  
  summary_docs = map(lambda x : x[0], summaries_and_details)
  details_docs = map(lambda x : x[1], summaries_and_details)
  hints_docs = map(lambda x : x[2], summaries_and_details)
  details_docs = list(itertools.chain(*details_docs)) # flatten
  hints_docs = list(itertools.chain(*hints_docs)) # flatten
  
  
  print "Posting %d docs..." % len(summary_docs);
  
  post_bulk(OUTPUT_URL, summary_docs)
  post_bulk(OUTPUT_DETAILS_URL, details_docs)
  post_bulk(OUTPUT_HINTS_URL, hints_docs)
  
def create_block_documents():
  
  num_processed = 0
  blocks_and_ids = blocks_to_ids.items()
  for i in range(0, len(blocks_and_ids), COUCHDB_BULK_INSERT_SIZE * POOL_SIZE):
    
    limit = min(len(blocks_and_ids), i + (COUCHDB_BULK_INSERT_SIZE * POOL_SIZE))
    batches_as_list = blocks_and_ids[i:limit]
    
    # partition into roughly equal sublists
    async_batches = []
    for j in range(0, len(batches_as_list), COUCHDB_BULK_INSERT_SIZE):
      if j > len(batches_as_list):
        break
      limit = min(len(batches_as_list), j + COUCHDB_BULK_INSERT_SIZE)
      async_batches.append(batches_as_list[j:limit])
    
    print "Processing %d docs from CouchDB in %d (%d * %d) concurrent threads" % \
        (len(batches_as_list), POOL_SIZE * len(INPUT_COUCHDBS), POOL_SIZE, len(INPUT_COUCHDBS))
    
    namespace = {'num_docs_batches' : 0}
    progress_indicator = {'progress' : 0, 'total' : len(batches_as_list)}
    
    def process_and_post(batch):  
      docs_batch = map((lambda x : create_block_document(x[0], str(x[1]), progress_indicator)), batch)
      post_documents_to_couchdb(docs_batch)
      namespace['num_docs_batches'] += len(docs_batch)
      
    pool = Pool(POOL_SIZE)
    for async_batch in async_batches:
      pool.spawn(process_and_post, async_batch)
    pool.join()
    
    num_processed += namespace['num_docs_batches']
    print "\nPosted %d/%d (%.2f%%) blocks total." % (num_processed, len(blocks_to_ids), num_processed * 100.0 / len(blocks_to_ids))
    
    if DEBUG_MODE and num_processed > (COUCHDB_BULK_INSERT_SIZE * 20):
      break

def build_blocks_to_ids_map():
  block_ids_url = INPUT_BLOCK_IDS_DB + '/_all_docs'
  params = {'limit' : COUCHDB_READ_SIZE, 'include_docs' : 'true'}
  if DEBUG_MODE:
    params['stale'] = 'update_after'
  
  num_read = 0
  while True:
    block_ids = requests.get(block_ids_url, params=params).json()

    if len(block_ids['rows']) == 0:
      break
    
    num_read += len(block_ids['rows'])
    total = block_ids['total_rows']
    print "Received %d/%d (%.2f%%) docs from CouchDB %s" % (num_read,total,num_read * 100.0/total, INPUT_BLOCK_IDS_DB)
    
    for row in block_ids['rows']:
      if 'intId' in row['doc']: # skip design documents
        blocks_to_ids[row['key']] = row['doc']['intId']
    
    params.update({'startkey' : json.dumps(block_ids['rows'][-1]['key']), 'skip' : 1})
    
    if DEBUG_MODE and num_read > 10000:
      break
    
  print "read in all docs from db %s" % INPUT_BLOCK_IDS_DB
  
def main():
  
  # drop and re-create both output databases
  for url in (OUTPUT_URL, OUTPUT_DETAILS_URL, OUTPUT_HINTS_URL):
    print 'dropping database %s, response is %s' % (url, requests.delete(url).status_code)
    print 'creating database %s, response is %s' % (url, requests.put(url).status_code)
  
  for design_doc in design_documents:
    response = requests.put(OUTPUT_URL + '/' + design_doc['_id'],data=json.dumps(design_doc),headers={'Content-Type':'application/json'})
    print 'posted design doc %s to CouchDB, got response %d' % (design_doc['_id'], response.status_code)
  
  print "\nreading from input CouchDB %s..." % (INPUT_BLOCK_IDS_DB)
  build_blocks_to_ids_map()
  print "\nreading from blocks_to_hints in %s, writing to %s..." % (INPUT_COUCHDBS, (OUTPUT_URL, OUTPUT_DETAILS_URL, OUTPUT_HINTS_URL))
  create_block_documents()
  
if __name__=='__main__':
  main()
