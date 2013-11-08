#!/usr/bin/env python
#
# Compute an optimized couchdb database from the given CouchDB database (generated from the load_data.py script)
#
# This db should be able to answer simple questions like:
# What are the most popular blocks?
# Given a block, what are the next/previous blocks and their associated hints?
# 

import requests, json, sys, base64, re, itertools

COUCHDB_BULK_INSERT_SIZE = 100

DEBUG_MODE = True # sets stale to update_after

INPUT_URL = 'http://localhost:5984/blocks'
OUTPUT_URL = 'http://koholint-wired:5985/block_stats'
OUTPUT_DETAILS_URL = 'http://koholint-wired:5985/block_details'


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
  
  block_hints_url = INPUT_URL + '/_design/blocks_to_hints/_view/blocks_to_hints'
  params = {'include_docs' : 'true', 'startkey' : json.dumps([[block]]), 'endkey' : json.dumps([[block, {}]])}
  if (DEBUG_MODE):
    params['stale'] = 'update_after'
  block_hints = requests.get(block_hints_url, params=params).json()
    
  result = {'_id' : anonymize(block), 'count' : count, 'hints' : [], 'precedingBlocks' : {}, 'followingBlocks' : {}}
  rows = block_hints['rows'] if 'rows' in block_hints else []
  for block_hint in rows:
    (key, hints) = (block_hint['key'], block_hint['doc']['hints'])
    
    if len(key[0]) > 1: # has a related block
      (related_block, reverse_order) = (key[0][1], key[1])
      
      key = 'precedingBlocks' if reverse_order else 'followingBlocks';
      try:
        result['followingBlocks'][anonymize(related_block)] += hints
      except KeyError:
        result['followingBlocks'][anonymize(related_block)] = hints
    else: # no related block; singleton only
      result['hints'] += hints
  return result
  

def split_doc_into_summary_and_details(doc):
  # split docs into an optimized summary/detail format and post
  # to two separate databases
  
  # create separate docs
  related_blocks = []
  for (otherBlocks, preceding) in ((doc['precedingBlocks'], True), (doc['followingBlocks'], False)):
    for (otherBlock, hints) in otherBlocks.items():
      related_blocks.append({'preceding' : preceding, 'hints' : hints, 'count' : len(hints), 'block' : int(otherBlock)})
  
  # sort by count
  related_blocks = sorted(related_blocks, key=lambda related_block : related_block['count'])
  related_blocks.reverse()
  
  # remove details from original doc
  del doc['precedingBlocks']
  del doc['followingBlocks']
  
  # apply an id to look like this : <blockId>~01, <blockId>~02
  # this means we can sort lexicographically in CouchDB by blockId, then number of hints (descending)
  max_id_len = len(str(len(related_blocks)))
  for i in range(len(related_blocks)):
    related_block = related_blocks[i]
    related_block['_id'] = doc['_id'] + "~" + str(i).zfill(max_id_len)
  
  return (doc, related_blocks)

def post_bulk(url, docs):
  
  response = requests.post(url + '/_bulk_docs',data=json.dumps({'docs' : docs}),headers={'Content-Type':'application/json'})
  print "Posted %d documents to %s, response: %d" % (len(docs), url, response.status_code)
  if (str(response.status_code).startswith('4')): # error
    print " > Got error", response.json()
  
def post_documents_to_couchdb(docs, last_counter):
  
  summaries_and_details = map(split_doc_into_summary_and_details, docs);
  
  summary_docs = map(lambda x : x[0], summaries_and_details)
  details_docs = map(lambda x : x[1], summaries_and_details)
  details_docs = list(itertools.chain(*details_docs)) # flatten
    
  post_bulk(OUTPUT_URL, summary_docs)
  post_bulk(OUTPUT_DETAILS_URL, details_docs)
  
  print "Posted %d blocks total..." % (last_counter)
  
def create_block_documents():
  
  block_counts_url = INPUT_URL + '/_design/blocks_to_counts/_view/blocks_to_counts'
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
  
  # drop and re-create both output databases
  for url in (OUTPUT_URL, OUTPUT_DETAILS_URL):
    print 'dropping database %s, response is %s' % (url, requests.delete(url).status_code)
    print 'creating database %s, response is %s' % (url, requests.put(url).status_code)
  
  for design_doc in design_documents:
    response = requests.put(OUTPUT_URL + '/' + design_doc['_id'],data=json.dumps(design_doc),headers={'Content-Type':'application/json'})
    print 'posted design doc %s to CouchDB, got response %d' % (design_doc['_id'], response.status_code)
  
  print "reading from old CouchDB..."
  create_block_documents()
  
  # just in case I need this later
  fileout = open('block_mappings.json', 'wb')
  print >>fileout,json.dumps(anonymizer)
  fileout.close()
  
if __name__=='__main__':
  main()
