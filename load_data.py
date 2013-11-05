#!/usr/bin/env python
# create a sqlite3/couchdb database from the given cred file in the local directory
# 
# ./load_data.py --db [sqlite/couchdb]
#

import sys, re, getopt

BATCH_SIZE = 5000000
TOTAL_NUM_LINES = 153004874

args = {'--db' : 'sqlite'}
args.update(dict(getopt.getopt(sys.argv[1:], '', ['db='])[0]))

dbtype = args['--db']
if dbtype == 'couchdb':
  import requests, json
elif dbtype == 'mysql':
  import MySQLdb as mysqldb;
  conn = mysqldb.connect('koholint','adobe_leaks','adobe_leaks','adobe_leaks')
  conn.autocommit(True)
  cur = conn.cursor()
  cur.execute('drop table if exists users')
  cur.execute('''
    create table users(id int, adobe_username varchar(1023), 
    email varchar(1023), password varchar(255), hint longtext);''')
else:
  import sqlite3
  conn = sqlite3.connect('cred.db')
  # create DB and clear if it has any lines in it
  conn.execute('drop table if exists users;')
  conn.execute('''
  create table users (
    id int, 
    adobe_username text, 
    email text, 
    password text, 
    hint text);''')

  conn.execute('create index users_id_idx on users (id);');
  conn.execute('create index users_email_idx on users(email);');
  conn.execute('create index users_password_idx on users(password);');

# batch process the file for better performance
def load_parsed_lines(lines):
  if dbtype == 'couchdb':
    docs = {'docs' : map((lambda line : dict(zip(['id','adobe_username','email','password','hint'],line))), lines)}
    requests.post('http://127.0.0.1:5984/adobe_leaks/_bulk_docs',data=json.dumps(docs),headers={'Content-Type':'application/json'})
  elif dbtype == 'mysql':
    for line in lines:
      cur.execute('insert into users values (%s,%s,%s,%s,%s);', line)
  else:
    conn.executemany('insert into users values (?,?,?,?,?);', lines)

def parse_line(line):
  line = line.strip()
  if line.endswith('|--'):
    line = line[:-3]
  return line.split('-|-') 

numlines = 0
credfile = open('cred','rb')
lines_batch = credfile.readlines(BATCH_SIZE)

while lines_batch:
  parsed_lines = [parse_line(line) for line in lines_batch]

  parsed_lines = filter((lambda line: len(line) >= 5), parsed_lines) # empty lines

  for line in parsed_lines:
    if len(line) > 5:
      print "found a line with too many fields, ignoring extra fields:",line
      line = line[:5]

  load_parsed_lines(parsed_lines)
  numlines += len(parsed_lines)
  print 'processed %d/%d (%.2f%%) lines from cred file so far...' % (numlines, TOTAL_NUM_LINES, (numlines * 100.0 / TOTAL_NUM_LINES))
