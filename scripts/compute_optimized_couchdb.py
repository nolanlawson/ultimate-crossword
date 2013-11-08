#!/usr/bin/env python
#
# Compute an optimized couchdb database from the given CouchDB database (generated from the load_data.py script)
#
# This db should be able to answer simple questions like:
# What are the most popular blocks?
# Given a block, what are the next/previous blocks and their associated hints?
# 

import requests, json, sys, base64, re, MySQLdb as mysqldb

COUCHDB_HOST = '127.0.0.1'
COUCHDB_INPUT_DB = 'blocks'
COUCHDB_OUTPUT_DB = 'block_stats'

COUCHDB_BULK_INSERT_SIZE = 100

DEBUG_MODE = True # sets stale to update_after

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

anonymizer = {}
anonymous_count = 0

def anonymize(block):
  global anonymous_count
  
  # I don't know if anyone will ever actually reverse-engineer Adobe's encryption, but
  # I can cover my ass by using an integer instead of the string
  try:
    return str(anonymizer[block])
  except KeyError:
    count = anonymous_count
    anonymizer[block] = count
    anonymous_count += 1
    return str(count)

def create_block_document(block, count):
  
  block_hints_url = couchdb_input_url + '/_design/blocks_to_hints/_view/blocks_to_hints'
  params = {'include_docs' : 'true', 'startkey' : json.dumps([[block]]), 'endkey' : json.dumps([[block, {}]])}
  if (DEBUG_MODE):
    params['stale'] = 'update_after'
  block_hints = requests.get(block_hints_url, params=params).json()
    
  result = {'_id' : anonymize(block), 'count' : count, 'hints' : [], 'preceding_blocks' : {}, 'following_blocks' : {}}
  rows = block_hints['rows'] if 'rows' in block_hints else []
  for block_hint in rows:
    (key, hints) = (block_hint['key'], block_hint['doc']['hints'])
    
    if len(key[0]) > 1: # has a related block
      (related_block, reverse_order) = (key[0][1], key[1])
      
      key = 'preceding_blocks' if reverse_order else 'following_blocks';
      try:
        result['following_blocks'][anonymize(related_block)] += hints
      except KeyError:
        result['following_blocks'][anonymize(related_block)] = hints
    else: # no related block; singleton only
      result['hints'] += hints
  return result
  
  
def post_documents_to_couchdb(docs, last_counter):
  response = requests.post(couchdb_output_url + '/_bulk_docs',data=json.dumps({'docs' : docs}),headers={'Content-Type':'application/json'})
  print "Posted %d documents, response: %d" % (last_counter, response.status_code)
  if (str(response.status_code).startswith('4')): # error
    print " > Got error", response.json()
  
def create_block_documents():
  
  block_counts_url = couchdb_input_url + '/_design/blocks_to_counts/_view/blocks_to_counts'
  params = {'group' : 'true', 'reduce' : 'true', 'stale' : 'ok', 'limit' : COUCHDB_BULK_INSERT_SIZE}
  if (DEBUG_MODE):
    params['stale'] = 'update_after'
  
  counter = 0
  while True:
    block_counts = requests.get(block_counts_url, params=params).json()

    if (len(block_counts) == 0):
      break
    
    docs_batch = map((lambda row : create_block_document(row['key'], row['value'])), block_counts['rows'])
    
    counter += len(docs_batch)
    
    post_documents_to_couchdb(docs_batch, counter)
    
    params.update({'startkey' : json.dumps(block_counts['rows'][-1]['key']), 'skip' : 1})
    
    if (DEBUG_MODE and counter > (COUCHDB_BULK_INSERT_SIZE * 20)):
      break

def main():
  
  # drop and re-create
  print 'dropping database, response is', requests.delete(couchdb_output_url).status_code
  print 'creating database, response is', requests.put(couchdb_output_url).status_code  
  
  for design_doc in design_documents:
    response = requests.put(couchdb_output_url + '/' + design_doc['_id'],data=json.dumps(design_doc),headers={'Content-Type':'application/json'})
    print 'posted design doc %s to CouchDB, got response %d' % (design_doc['_id'], response.status_code)
  
  print "reading from old CouchDB..."
  docs = create_block_documents()
  
  # just in case I need this later
  fileout = open('block_mappings.json', 'wb')
  print >>fileout,json.dumps(anonymizer)
  fileout.close()
  
if __name__=='__main__':
  main()
