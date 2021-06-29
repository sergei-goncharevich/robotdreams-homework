--1. вывести количество фильмов в каждой категории, отсортировать по убыванию.
select c.name, count(*) cnt from film f join film_category fc on f.film_id = fc.film_id join category c on fc.category_id = c.category_id
group by c.category_id, c.name
order by cnt desc;


--2. вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
select * from 
(
	select a.first_name, a.last_name, count(*) rental_cnt, rank() over (order by count(*) desc) actor_rental_rank 
	from rental r join inventory i on r.inventory_id = i.inventory_id 
	join film f on f.film_id = i.film_id 
	join film_actor fa on f.film_id = fa.film_id
	join actor a on a.actor_id = fa.actor_id
	group by a.actor_id, a.first_name, a.last_name
) q
where q.actor_rental_rank <= 10
order by actor_rental_rank;

--3. вывести категорию фильмов, на которую потратили больше всего денег.
select * from 
(
    select c.name, sum(p.amount) amt, rank() over (order by sum(p.amount) desc) category_amt_rank 
	from rental r join inventory i on r.inventory_id = i.inventory_id
	join payment p on p.rental_id = r.rental_id 
	join film f on f.film_id = i.film_id 
    join film_category fc on f.film_id = fc.film_id 
    join category c on fc.category_id = c.category_id
    group by c.category_id, c.name
) q where category_amt_rank=1;


--4. вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.

--вариант 1
select f.title from film f left outer join inventory i on f.film_id = i.film_id 
where i.film_id is null
order by 1;

--вариант 2
select f.title from film f 
where not exists (select 1 from inventory i where f.film_id=i.film_id)
order by 1;


--5. вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
select * from 
(
	select a.first_name, a.last_name, count(*) actor_film_cnt, dense_rank() over (order by count(*) desc) actor_rank from category c
	join film_category fc ON fc.category_id = c.category_id 
	join film f on f.film_id = fc.film_id 
	join film_actor fa on fa.film_id = f.film_id 
	join actor a on a.actor_id = fa.actor_id 
	where c.name = 'Children'
	group by a.actor_id, a.first_name, a.last_name 
) q where actor_rank<=3
order by actor_rank;


--6. вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.
select c.city, sum(cu.active) active_cnt, sum(case when active=0 then 1 else 0 end) inactive_cnt from city c
join address a on a.city_id = c.city_id 
join customer cu on cu.address_id = a.address_id 
group by c.city_id, c.city 
order by inactive_cnt desc;


--7. вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. 
--То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.

with subq as
(
	select c.name, ci.city, r.return_date - r.rental_date rental_time from category c 
	join film_category fc on fc.category_id = c.category_id 
	join inventory i on fc.film_id = i.film_id
	join rental r on r.inventory_id = i.inventory_id 
	join customer cu on r.customer_id = cu.customer_id 
	join address a on cu.address_id = a.address_id 
	join city ci on a.city_id = ci.city_id 
	where r.return_date is not null
)
select 'most popular category for A% cities:' title, name from
(
	select name, sum(rental_time) rtime, rank() over (order by sum(rental_time) desc) category_rank from subq where lower(city) like 'a%' group by name
) q where category_rank=1
union all 
select 'most popular category for %-% cities:' title, name from
(
	select name, sum(rental_time) rtime, rank() over (order by sum(rental_time) desc) category_rank from subq where lower(city) like '%-%' group by name
) q where category_rank=1;

