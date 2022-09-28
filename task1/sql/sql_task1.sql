select * from rooms;

select * from students;

select room_id from rooms
where room_id not in (select distinct room_id from students);

-- список комнат и количество студентов в каждой из них
select room_id, count(*) as number_of_students
from students
group by room_id
order by number_of_students desc;

-- top 5 комнат, где самый маленький средний возраст студентов
with cte as (select *, timestampdiff(year, birthday, now()) as age_years
	from students)
select room_id, sum(age_years) / count(*) as avg_age
from cte
group by room_id
order by avg_age
limit 5;

-- top 5 комнат с самой большой разницей в возрасте студентов
with cte as (select *, timestampdiff(year, birthday, now()) as age_years
	from students)
select room_id, max(age_years) - min(age_years) as age_difference 
from cte
group by room_id
order by age_difference desc
limit 5;

-- список комнат где живут разнополые студенты.
with cte as (select *,
	sum(case when sex = 'F' then 1
	else 0
	end) as count_female
	from students
	group by room_id)
select room_id from cte
where count_female > 0;



SELECT JSON_ARRAYAGG(JSON_OBJECT('room_id', room_id, 'number_of_students', number_of_students))
from (select room_id, count(*) as number_of_students 
from students
group by room_id
order by number_of_students desc) as t;