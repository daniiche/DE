--1

SELECT c.category_id
    ,c.name
    ,count(f.film_id) as cnt
FROM public.film f
join film_category fc
    on f.film_id = fc.film_id
join category c
    on fc.category_id = c.category_id
group by c.category_id, c.name
order by cnt desc

--2

select a.actor_id,
       a.first_name
     , a.last_name
     , count(r.rental_id) as cnt_rented
from actor a
join film_actor fa
    on a.actor_id = fa.actor_id
join inventory i
    on i.film_id = fa.film_id
join rental r
    on i.inventory_id = r.inventory_id
group by a.actor_id, a.first_name, a.last_name
order by cnt_rented desc
limit 10

--3

select c.category_id
     , c.name
     , sum(p.amount) as sum_pay
from category c
join film_category fc
    on c.category_id = fc.category_id
join inventory i
    on i.film_id = fc.film_id
join rental r
    on r.inventory_id = i.inventory_id
join payment p
    on p.rental_id = i.inventory_id
group by c.category_id, c.name
    order by sum_pay desc
limit 1

--4

select f.title
    from film f
join
(select film_id
from film

except

select film_id
from inventory) flms
on f.film_id = flms.film_id

--5

;with child_cte as (
    select distinct count(fa.film_id) cnt
from actor a
join film_actor fa
    on a.actor_id = fa.actor_id
join film_category fc
    on fa.film_id = fc.film_id
join category c
    on fc.category_id = c.category_id
where c.name like 'Children'
group by a.actor_id
)
select a.actor_id
     , a.first_name
     , a.last_name
     , count(fa.film_id) cnt
from actor a
join film_actor fa
    on a.actor_id = fa.actor_id
join film_category fc
    on fa.film_id = fc.film_id
join category c
    on fc.category_id = c.category_id
where c.name like 'Children'
group by a.actor_id, a.first_name, a.last_name
having count(fa.film_id) in (select cnt from child_cte order by cnt desc limit 3)
order by cnt desc

--6

--так, если считать что адреса верно ссылаются на города, а клиенты верно ссылаются на адреса
--если предполагать, что клиенты относятся к конкретному магазину, то получается всего два города. Предполагаем, что считаем наличие клиентов в городах по указаному адресу, а не привязке к магазину.
select c.city_id
     , c.city
     , count(distinct ca.customer_id) as cnt_a
     , count(distinct cna.customer_id) as cnt_na
from city c
join address a
    on c.city_id = a.city_id
left join customer ca
    on a.address_id = ca.address_id
    and ca.active=1
left join customer cna
    on a.address_id = cna.address_id
    and cna.active=0
group by c.city_id, c.city
order by cnt_na desc

--7

(select c.category_id
     ,c.name
      , sum(extract(hour from return_date-rental_date)) as hrs
      , 'A cities' as condition
from category c
join film_category fc
    on c.category_id = fc.category_id
join inventory i
    on fc.film_id = i.film_id
join rental r
    on i.inventory_id = r.inventory_id
join customer c2
    on r.customer_id = c2.customer_id
join address a
    on c2.address_id = a.address_id
join city c3
    on a.city_id = c3.city_id
where c3.city like 'a%'
group by c.category_id, c.name
order by hrs desc
limit 1)

union all

(select c.category_id
     ,c.name, sum(extract(hour from return_date-rental_date)) as hrs
      , '- cities' as condition
from category c
join film_category fc
    on c.category_id = fc.category_id
join inventory i
    on fc.film_id = i.film_id
join rental r
    on i.inventory_id = r.inventory_id
join customer c2
    on r.customer_id = c2.customer_id
join address a
    on c2.address_id = a.address_id
join city c3
    on a.city_id = c3.city_id
where c3.city like '%-%'
group by c.category_id, c.name
order by hrs desc
limit 1)

