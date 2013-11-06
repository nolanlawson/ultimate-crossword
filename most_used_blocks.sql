
SELECT x.*,group_concat(u.hint) from
  (select count(*) as counter, bu.block_id 
  FROM user_blocks bu where bu.block_location = 0 
  group by bu.block_id having counter > 10 order by counter desc
  limit 200) x
join user_blocks ub on x.block_id = ub.block_id and ub.block_location = 0
join users u on ub.user_id = u.id
where u.hint != '' and u.hint is not null
group by x.block_id order by counter desc