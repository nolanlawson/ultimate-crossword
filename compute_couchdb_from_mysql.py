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

MAX_NUM_BLOCKS = 1000
MIN_BLOCK_POPULARITY = 10

def create_block_document(block_id, count):
  cur = conn.cursor()
  # fetch preliminary list of users and hints
  cur.execute('''select id, hint
    from users u, user_blocks ub
    where ub.block_id=%d and u.id=ub.user_id
    and hint != '' and hint is not null''', (block_id))

  users_and_hints = cur.fetchall()

  # fetch related blocks, i.e. preceding or following blocks
  all_user_ids = str(tuple(set(map(lambda x : x[0], users_to_hints))))
  cur.execute('''select user_id, block_id, block_position
    from user_blocks
    where user_id = in %s and block_id != %d
  ''', (all_user_ids, block_id))

  users_to_related_blocks = dict(map((lambda x : [x[0], x[1:2]]), cur.fetchall()))
  cur.close()
  
  result = {'_id' : block_id, 'count' : count, 'hints' : [], 'preceding_blocks' : {}, 'following_blocks' : {}}
  for [user_id, hint] in users_and_hints:
    if user_id in users_to_related_blocks: # has a related block
      (other_block_id, other_block_position) = users_to_related_blocks[user_id]
      key = 'following_blocks' if other_block_position == 1 else 'preceding_blocks';
      try:
        result['following_blocks'][other_block_id] += [hint]
      except KeyError:
        result['following_blocks'][other_block_id] = [hint]
    else: # no related block; singleton only
      result['hints'] += [hint]
  return result
  
def create_block_documents():
  conn = mysqldb.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB)
  cur = conn.cursor()
  docs = []
  cur.execute('''select count(*) as counter, block_id 
    from user_blocks
    group by block_id having counter > %s order by counter desc
    limit %s''', (MIN_BLOCK_POPULARITY, MAX_NUM_BLOCKS))
  for row in cur.fetchall():
    (count, block_id) = tuple(row)
    docs += [create_block_document(block_id, count)];
  cur.close()
  return docs
  

def main():
  docs = create_block_documents()

  # post the documents to couchdb
  couchdb_url = 'http://%s:5984/%s/_bulk_docs' % (COUCHDB_HOST, COUCHDB_DB)
  requests.post(couchdb_url,data=json.dumps({'docs' : docs}),headers={'Content-Type':'application/json'})
  
if __name__=='__main__':
  main()