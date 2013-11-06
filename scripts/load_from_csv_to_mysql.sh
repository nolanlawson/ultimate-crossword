#!/bin/bash

for filename in $@
  do
  echo loading file $filename...
  echo load data infile "'"$filename"'" into table users fields terminated by "'","'" enclosed by "'"'"'"'" escaped by "'"\\\\"'" lines terminated by "'"\\r\\n"'" '('adobe_id, adobe_username, email, password, hint')'';' | mysql -uroot -ppassword adobe_leaks;
  echo loaded file $filename
done
