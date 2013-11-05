#!/usr/bin/env python
# create a sqlite3 database from the given cred file in the local directory

import re, sqlite3

BATCH_SIZE = 5000000
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
def load_parsed_lines(lines):
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
