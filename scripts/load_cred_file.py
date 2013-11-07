#!/usr/bin/env python
# create a csv/mysql database from the given cred file in the local directory
# 
# ./load_cred_file.py --db [csv/mysql]
#

import sys, re, getopt

BATCH_SIZE = 50000
TOTAL_NUM_LINES = 153004874

args = {'--db' : 'csv'}
args.update(dict(getopt.getopt(sys.argv[1:], '', ['db='])[0]))

dbtype = args['--db']
if dbtype == 'mysql':
  import MySQLdb as mysqldb;
  conn = mysqldb.connect('koholint','adobe_leaks','adobe_leaks','adobe_leaks')
  conn.autocommit(True)
  cur = conn.cursor()
  sql_commands = [\
    'set storage_engine=InnoDB;',\
    'drop trigger if exists block_after_insert;',\
    'drop trigger if exists block_counts_create',\
    'drop trigger if exists block_counts_update',\
    'drop table if exists users;',\
    'drop table if exists user_blocks;',\
    'drop table if exists blocks;',\
    'drop table if exists block_counts;',\
    '''
      create table users(
      id int primary key auto_increment, adobe_id int, adobe_username varchar(1023), 
      email varchar(1023), password varchar(255), hint longtext);
    ''',\
    'create table blocks (id int primary key auto_increment, value varchar(255) unique not null);',\
    'create table user_blocks (id int primary key auto_increment, user_id int not null, block_id int not null, block_location int not null);',\
    'create table block_counts (block_id int primary key, count int not null default 0)',\
    'create unique index user_blocks_primary_idx on user_blocks (user_id, block_id, block_location);',\
    'create unique index user_blocks_secondary_idx on user_blocks (block_id, block_location, user_id);',\
    'create index users_adobe_id on users(adobe_id);',\
    'create index users_email_idx on users(email(16));',\
    'create index users_hint_idx on users(hint(16));',\
    'create index block_counts_counts_idx on block_counts(count)',\
  	'''
	create trigger `block_after_insert` 
		after insert on `users`
		     FOR EACH ROW BEGIN
		     declare passwordlength int;
		     declare block1 varchar(255);
		     declare block2 varchar(255);
	         set passwordlength = length(NEW.password);
	         IF (passwordlength = 12 OR passwordlength = 24) THEN
	           set block1 = substring(NEW.password, 1, 11);
	           insert ignore into blocks (value) values (block1);
	           insert into user_blocks (user_id, block_id, block_location) values (NEW.id, (select id from blocks where value = block1), 0);
		     END IF;
	         IF (passwordlength = 24) THEN
	           set block2 = substring(NEW.password, 11, 11);
	           insert ignore into blocks (value) values (block2);
	           insert into user_blocks (user_id, block_id, block_location) values (NEW.id, (select id from blocks where value = block2), 1);
	         END IF;
	END
  ''',\
  '''
  create trigger `block_counts_create`
    after insert on blocks
      for each row begin
      insert into block_counts(block_id, count) values (NEW.id, 0);
    END
  ''',\
  '''
  create trigger `block_counts_update`
    after insert on user_blocks
      for each row begin
      update block_counts set count = count + 1 where block_id = NEW.block_id;
  END
  ''',\
  ]
  for sql_command in sql_commands:
	cur.execute(sql_command)
else: # csv
  import csv
  csvfile = open('cred.csv','wb')
  csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)

# batch process the file for better performance
def load_parsed_lines(lines):
  if dbtype == 'mysql':
    for line in lines:
      cur.execute('insert into users(adobe_id,adobe_username,email,password,hint) values (%s,%s,%s,%s,%s);', line)
  else: # csv
    csvwriter.writerows(lines)

def parse_line(line):
  line = line.strip()
  if line.endswith('|--'):
    line = line[:-3]
  return line.split('-|-') 

def process_batch(lines):
  parsed_lines = [parse_line(line) for line in lines]

  parsed_lines = filter((lambda line: len(line) >= 5), parsed_lines) # empty lines

  for line in parsed_lines:
    if len(line) > 5:
      print "found a line with too many fields, ignoring extra fields:",line
      line = line[:5]

  load_parsed_lines(parsed_lines)

def main():

  numlines = 0
  credfile = open('cred','rb')
  buffer = []

  for line in credfile.xreadlines():
    buffer.append(line)
    if len(buffer) == BATCH_SIZE:
      process_batch(buffer)
      del buffer[:]
      numlines += BATCH_SIZE

      print 'processed %d/%d (%.2f%%) lines from cred file so far...' % (numlines, TOTAL_NUM_LINES, (numlines * 100.0 / TOTAL_NUM_LINES))

  if len(buffer) > 0:
    process_batch(buffer)

if __name__=='__main__':
  main()
