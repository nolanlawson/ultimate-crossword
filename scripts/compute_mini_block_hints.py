#!/usr/bin/env python
#
# create a new block hints, that actually allows us to sort properly and limit the number of hints
# we get back, in cases where there are just too many damn hints
#

import requests, json, sys, itertools
import gevent.monkey
gevent.monkey.patch_socket()
from gevent.pool import Pool

INPUT_DB = 'http://localhost:5984/block_hints'
OUTPUT_DB = 'http://localhost:5984/mini_block_hints'

POOL_SIZE = 1
ROUGH_BATCH_SIZE = 1000
NUM_RETRIES = 10

def create_new_docs(input_doc, progress_indicator):
  result = []
  # sort by counts descending, ,hints ascending
  max_value = 0
  countsToHints = {}
  for (hint, count) in input_doc['hintMap'].items():
    max_value += 1
    try:
      countsToHints[count].append(hint)
    except KeyError:
      countsToHints[count] = [hint]
  
  sortedCounts = sorted(countsToHints.keys())
  sortedCounts.reverse()
  
  counter = 0
  max_num_digits = len(str(max_value))
  
  for count in sortedCounts:
    hints = countsToHints[count];
    hints.sort();
    for hint in hints:
      docId = input_doc['_id'] + '-' + (str(counter).zfill(max_num_digits))
      result.append({'_id' : docId, 'hint' : hint, 'count' : count});
      counter += 1;
  
  progress_indicator['progress'] += 1
  sys.stdout.write(' > %.6f%%\r' % (progress_indicator['progress'] * 100.0 / progress_indicator['total']))
  sys.stdout.flush()
  if progress_indicator['progress'] == progress_indicator['total']:
    sys.stdout.write('\n')
    sys.stdout.flush()
  return result

def post_docs(docs):
  for i in range(NUM_RETRIES):
    try:
      print "About to post %d docs to %s" % (len(docs), OUTPUT_DB)
      response = requests.post(OUTPUT_DB + '/_bulk_docs',data=json.dumps({'docs' : docs}),headers={'Content-Type':'application/json'})
      
      print "posted %d docs, response was %d" % (len(docs), response.status_code)
      if str(response.status_code).startswith('4'):
        sys.exit("got an error " + response.json())
      break
    except requests.exceptions.ConnectionError:
      print "Connection error at %s, retrying for %dth time" % (OUTPUT_DB, i)



def transfer_docs():
  rows = requests.get(INPUT_DB + '/_all_docs', params={'include_docs' : 'true'}).json()['rows']
  
  progress_indicator = {'progress' : 0, 'total' : len(rows)}
  def process_batch(batch):
    
    docs_to_post = []
    for row in batch:
      docs_to_post += create_new_docs(row['doc'], progress_indicator)
    
    post_docs(docs_to_post)
  
  pool = Pool(POOL_SIZE)
  for i in range(0, len(rows), ROUGH_BATCH_SIZE):
    limit = min(len(rows), i + ROUGH_BATCH_SIZE)
    pool.spawn(process_batch, rows[i:limit])
    
  pool.join()

def main():
  
  # drop and re-create output database
  
  print 'dropping database %s, response is %s' % (OUTPUT_DB, requests.delete(OUTPUT_DB).status_code)
  print 'creating database %s, response is %s' % (OUTPUT_DB, requests.put(OUTPUT_DB).status_code)
  
  transfer_docs();

if __name__=='__main__':
  main()