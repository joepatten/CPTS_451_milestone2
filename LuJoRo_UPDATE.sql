UPDATE business as b
SET num_tips = sub_q.num_tips
FROM 
	(
	select business_id, count(*) as num_tips
	from tip
	group by business_id
	) as sub_q
WHERE sub_q.business_id = b.business_id;

UPDATE business as b
SET num_checkins = sub_q.num_checkins
FROM 
	(
	select business_id, count(*) as num_checkins
	from checkin
	group by business_id
	) as sub_q
WHERE sub_q.business_id = b.business_id;

UPDATE usertable as u
SET total_likes = sub_q.total_likes
FROM 
	(
	select user_id, sum(likes) as total_likes
	from tip
	group by user_id
	) as sub_q
WHERE sub_q.user_id = u.user_id;

UPDATE usertable as u
SET tipcount = sub_q.tipcount
FROM 
	(
	select user_id, count(*) as tipcount
	from tip
	group by user_id
	) as sub_q
WHERE sub_q.user_id = u.user_id;