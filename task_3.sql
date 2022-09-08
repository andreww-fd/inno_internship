--> Q1 Вывести количество фильмов в каждой категории, отсортировать по убыванию.
select c.name, count(*) as num_of_films from film
join film_category fc on film.film_id = fc.film_id
join category c on fc.category_id  = c.category_id
group by c.name
order by count(*) desc;

--> Q2 Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
select concat(concat(a.first_name, ' '), a.last_name) as artist_name, sum(f.rental_duration) as total_rental_duration 
from film f
join film_actor fa on fa.film_id = f.film_id
join actor a on fa.actor_id = a.actor_id
group by artist_name
order by total_rental_duration desc
limit 10;

--> Q3 Вывести категорию фильмов, на которую потратили больше всего денег.
select c.name as category_name, sum(f.rental_duration * f.rental_rate + f.replacement_cost) as total_costs
from film f
join film_category fc on fc.film_id = f.film_id
join category c on c.category_id = fc.category_id
group by c.name
order by total_costs desc
limit 1;

--> Q4 Вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.
select f.film_id, f.title
from film f
left join inventory i on f.film_id = i.film_id
where i.film_id is null

--> Q5 Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
with t1 as (select concat(concat(a.first_name, ' '), a.last_name) as artist_name, count(*) as num_of_films,
		dense_rank() over(order by count(*) desc) as ranking
		from film f
		join film_category fc on f.film_id = fc.film_id
		join film_actor fa on f.film_id = fa.film_id
		join category c on fc.category_id = c.category_id
		join actor a on fa.actor_id = a.actor_id
		where c.name = 'Children'
		group by artist_name)
select * from t1
where ranking <= 3

--> Q6 Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.
-- customer.address_id -> address.city_id -> city.city
with t1 as (select customer_id, first_name, last_name, address_id, active,
		case when active = 0 then 1
		else 0
		end as inactive
		from customer)
select city.city, sum(t1.active) as total_active_users, sum(t1.inactive) as total_inactive_users from t1
left join address on t1.address_id = address.address_id
left join city on address.city_id = city.city_id
group by city.city
order by total_inactive_users desc

--> Q7 Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), 
-- и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.
with t1 as (select city.city, category.name, count(category.name) as num_of_films from film
	join film_category on film.film_id = film_category.film_id
	join category on film_category.category_id = category.category_id
	join inventory on film.film_id = inventory.film_id
	join rental on inventory.inventory_id = rental.inventory_id
	join customer on rental.customer_id = customer.customer_id
	join address on customer.address_id = address.address_id
	join city on address.city_id = city.city_id
	where city.city like 'A%' or city.city like '%-%'
	group by city.city, category.name
	order by city.city, count(category.name) desc)
select * from t1
where num_of_films = (select max(num_of_films) from t1 t where t.city = t1.city group by city)

--> Q7 (3 CTE solution)
-- t1 - all_film_info (title, category, inventory_id, etc); t2 - (rental, addresses, customers, etc)
with t1 as (select * from film
	join film_category on film.film_id = film_category.film_id
	join category on film_category.category_id = category.category_id
	join inventory on film.film_id = inventory.film_id),

with t2 as (select * from rental
	join customer on rental.customer_id = customer.customer_id
	join address on customer.address_id = address.address_id
	join city on address.city_id = city.city_id
	where city.city like 'A%' or city.city like '%-%')
	select * from t2,

t3 as (select t2.city, t1.name, count(t1.name) as num_of_films from t1
	join t2 on t1.inventory_id = t2.inventory_id
	group by t2.city, t1.name
	order by t2.city)

select * from t3
where num_of_films = (select max(num_of_films) from t3 t where t.city = t3.city group by city)
