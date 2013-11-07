#!/usr/bin/env python
#
# Compute an optimized couchdb database from the given CouchDB database (generated from the load_data.py script)
#
# This db should be able to answer simple questions like:
# What are the most popular blocks?
# Given a block, what are the next/previous blocks and their associated hints?
# 

import requests, json, sys, MySQLdb as mysqldb

COUCHDB_HOST = '127.0.0.1'
COUCHDB_INPUT_DB = 'blocks'
COUCHDB_OUTPUT_DB = 'block_stats'

STALE = 'update_after' # for testing purposes, use update_after

COUCHDB_BULK_INSERT_SIZE = 1000

couchdb_input_url = 'http://%s:5984/%s' % (COUCHDB_HOST, COUCHDB_INPUT_DB)
couchdb_output_url = 'http://%s:5984/%s' % (COUCHDB_HOST, COUCHDB_OUTPUT_DB)

design_documents = [
{
  '_id'   : '_design/counts_to_blocks',\
  'views' : {
    'counts_to_blocks' : {
      'language' : 'javascript',
      'map'    : '''
      function(doc) {
        emit(doc.count, null);
      }
      ''',
    }
  }
}]

def create_block_document(block, count):
  
  block_hints_url = couchdb_input_url + '/_design/blocks_to_hints/_view/blocks_to_hints'
  params = {'stale' : STALE, 'include_docs' : 'true', 'startkey' : json.dumps([[block]]), 'endkey' : json.dumps([[block, {}]])}
  block_hints = requests.get(block_hints_url, params=params).json()
    
  result = {'_id' : block, 'count' : count, 'hints' : [], 'preceding_blocks' : {}, 'following_blocks' : {}}
  for block_hint in block_hints['rows']:
    (key, hints) = (block_hint['key'], block_hint['doc']['hints'])
    
    if len(key[0]) > 1: # has a related block
      (related_block, reverse_order) = (key[0][1], key[1])
      
      key = 'preceding_blocks' if reverse_order else 'following_blocks';
      try:
        result['following_blocks'][str(related_block)] += hints
      except KeyError:
        result['following_blocks'][str(related_block)] = hints
    else: # no related block; singleton only
      result['hints'] += hints
  return result
  
  
def post_documents_to_couchdb(docs, last_counter, num_docs):
  response = requests.post(couchdb_output_url + '/_bulk_docs',data=json.dumps({'docs' : docs}),headers={'Content-Type':'application/json'})
  print "Posted %d/%d documents (%.2f%%), response: %d" % (last_counter, num_docs, (100.0 * last_counter / num_docs), response.status_code)
  
def create_block_documents():
  
  block_counts_url = couchdb_input_url + '/_design/blocks_to_counts/_view/blocks_to_counts'
  block_counts = requests.get(block_counts_url, params={'group' : 'true', 'reduce' : 'true', 'stale' : STALE}).json()

  counter = 0
  num_docs = len(block_counts['rows'])
  print "Found %d blocks total" % (num_docs)
  
  docs_buffer = []
  for row in block_counts['rows']:
    (block, count) = (row['key'], row['value'])
    doc = create_block_document(block, count)
    counter += 1
    
    docs_buffer.append(doc)
    if (len(docs_buffer) == COUCHDB_BULK_INSERT_SIZE):
      post_documents_to_couchdb(docs_buffer, counter, num_docs)
      del docs_buffer[:]
  
  if (len(docs_buffer) > 0):
    post_documents_to_couchdb(docs_buffer, counter, num_docs)

def main():
  
  # drop and re-create
  print 'dropping database, response is', requests.delete(couchdb_output_url).status_code
  print 'creating database, response is', requests.put(couchdb_output_url).status_code  
  
  for design_doc in design_documents:
    response = requests.put(couchdb_output_url + '/' + design_doc['_id'],data=json.dumps(design_doc),headers={'Content-Type':'application/json'})
    print 'posted design doc %s to CouchDB, got response %d' % (design_doc['_id'], response.status_code)
  
  print "reading from old CouchDB..."
  docs = create_block_documents()
  
if __name__=='__main__':
  main()
