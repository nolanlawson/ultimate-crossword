#!/usr/bin/env python
#
# Compute an optimized couchdb database from the given MySQL database (generated from the load_data.py script)
#
# This db should be able to answer simple questions like:
# What are the most popular blocks?
# Given a block, what are the next/previous blocks and their associated hints?
# 

import requests, json, sys, MySQLdb as mysqldb

COUCHDB_HOST = '127.0.0.1'
COUCHDB_DB = 'blocks'

MYSQL_HOST = 'koholint'
MYSQL_USER = 'adobe_leaks'
MYSQL_PASSWORD = 'adobe_leaks'
MYSQL_DB = 'adobe_leaks'

MAX_NUM_BLOCKS = 10
MIN_BLOCK_POPULARITY = 10

conn = mysqldb.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB)

def fetch_users_to_related_docs(users_and_hints, block_id):
  if len(users_and_hints) == 0:
    return {}

  # fetch related blocks, i.e. preceding or following blocks
  all_user_ids = ','.join(set(map(lambda x : str(int(x[0])), users_and_hints)))
  sql = '''select user_id, block_id, block_location
    from user_blocks
    where user_id in (''' + all_user_ids + ''') and block_id != %s
  '''
  #print "executing sql:",sql
  cur = conn.cursor()
  cur.execute(sql, (block_id))

  result = dict(map((lambda x : [x[0], x[1:]]), cur.fetchall()))
  cur.close()
  return result

def create_block_document(block_id, count):
  cur = conn.cursor()
  # fetch preliminary list of users and hints
  cur.execute('''select u.id, hint
    from users u, user_blocks ub
    where ub.block_id=%s and u.id=ub.user_id
    and hint != '' and hint is not null''', (block_id))

  users_and_hints = cur.fetchall()
  cur.close()

  users_to_related_blocks = fetch_users_to_related_docs(users_and_hints, block_id);
  
  result = {'_id' : str(block_id), 'count' : count, 'hints' : [], 'preceding_blocks' : {}, 'following_blocks' : {}}
  for [user_id, hint] in users_and_hints:
    if user_id in users_to_related_blocks: # has a related block
      (other_block_id, other_block_location) = tuple(users_to_related_blocks[user_id])
      key = 'following_blocks' if other_block_location == 1 else 'preceding_blocks';
      try:
        result['following_blocks'][str(other_block_id)] += [hint]
      except KeyError:
        result['following_blocks'][str(other_block_id)] = [hint]
    else: # no related block; singleton only
      result['hints'] += [hint]
  return result
  
def create_block_documents():
  cur = conn.cursor()
  docs = []
  cur.execute('''select bc.count, bc.block_id 
    from block_counts bc
    where bc.count > %s order by bc.count desc
    limit %s''', (MIN_BLOCK_POPULARITY, MAX_NUM_BLOCKS))
  for row in cur.fetchall():
    (count, block_id) = tuple(row)
    doc = create_block_document(block_id, count)
    #print "created doc:", doc
    print ".",
    sys.stdout.flush()
    docs += [doc];
  print
  cur.close()
  return docs
  

def main():
  print "reading from MySQL..."
  docs = create_block_documents()
  print "posting to CouchDB..."

  # post the documents to couchdb
  couchdb_url = 'http://%s:5984/%s/_bulk_docs' % (COUCHDB_HOST, COUCHDB_DB)
  response = requests.post(couchdb_url,data=json.dumps({'docs' : docs}),headers={'Content-Type':'application/json'})
  print response.status_code
  print response.json()
  print "done."
  
if __name__=='__main__':
  main()