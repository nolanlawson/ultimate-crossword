
SELECT x.*,group_concat(u.hint) from
  (select count, block_id 
  FROM block_counts
  where count > 10 order by count desc
  limit 1000) x
join user_blocks ub on x.block_id = ub.block_id and ub.block_location = 0
join users u on ub.user_id = u.id
where u.hint != '' and u.hint is not null
group by x.block_id order by count desc