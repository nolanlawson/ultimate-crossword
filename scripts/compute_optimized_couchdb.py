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

couchdb_input_url = 'http://%s:5984/%s' % (COUCHDB_HOST, COUCHDB_INPUT_DB)
couchdb_output_url = 'http://%s:5984/%s' % (COUCHDB_HOST, COUCHDB_OUTPUT_DB)

def create_block_document(block, count, docid):
  
  block_hints_url = couchdb_input_url + '/_design/blocks_to_hints/_view/blocks_to_hints'
  params = {'stale' : STALE, 'include_docs' : 'true', 'startkey' : json.dumps([[block]], 'endkey' : json.dumps([[block, {}]]))}
  block_hints = requests.get(block_hints_url, params=params).json()
  
    
  result = {'_id' : str(docid), 'block' : block, 'count' : count, 'hints' : [], 'preceding_blocks' : {}, 'following_blocks' : {}}
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
  
def create_block_documents():
  
  block_counts_url = couchdb_input_url + '/_design/blocks_to_counts/_view/blocks_to_counts'
  block_counts = requests.get(block_counts_url, params={'group' : 'true', 'reduce' : 'true', 'stale' : STALE}).json()

  docid = 0
  num_docs = len(block_counts['rows'])
  print "Found %d blocks total" % (num_docs)
  
  for row in block_counts['rows']:
    (block, count) = (row['key'], row['value'])
    doc = create_block_document(block, count, docid)
    requests.put(couchdb_output_url + '/' + docid,data=json.dumps(doc),headers={'Content-Type':'application/json'})
    print "Posted document %d/%d (%.2f%%)" % (docid, num_docs, (100.0 * (docid + 1) / num_docs))
    docid += 1

def main():
  
  # drop and re-create
  print 'dropping database, response is', requests.delete(couchdb_output_url).status_code
  print 'creating database, response is', requests.put(couchdb_output_url).status_code  
  
  print "reading from old CouchDB..."
  docs = create_block_documents()
  
if __name__=='__main__':
  main()
