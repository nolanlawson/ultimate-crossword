#!/usr/bin/env python
# create a sqlite3 database from the given cred file in the local directory

import re, sqlite3

BATCH_SIZE = 100000
TOTAL_NUM_LINES = 153004874

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
buffer = []

def load_from_buffer():
  conn.executemany('insert into users values (?,?,?,?,?);', buffer)

def parse_line(line):
  line = line.strip()
  if line.endswith('|--'):
    line = line[:-3]
  return line.split('-|-') 

numlines = 0
for line in open('cred','rb'):
  line = parse_line(line)
  
  if len(line) < 5:
    continue # empty line
  elif len(line) > 5:
    print "found a line with too many fields, ignoring extra fields:",line
    line = line[:5]
  buffer.append(line)
  numlines += 1
  
  if len(buffer) == BATCH_SIZE:
    load_from_buffer()
    del buffer[:] # clear
    print 'processed %d/%d (%.4f%%) lines from cred file so far...' % (numlines, TOTAL_NUM_LINES, (numlines * 1.0 / TOTAL_NUM_LINES))

if len(buffer) > 0:
  load_from_buffer()
  
