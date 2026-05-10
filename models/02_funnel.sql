-- users reaching each step, and conversion from the previous step
with steps as (
    select type, count(distinct user_id) as users
    from raw_events group by type
), ordered as (
    select type, users,
           case type when 'page_view' then 1 when 'add_to_cart' then 2 when 'checkout' then 3 else 4 end as step
    from steps
)
select step, type, users,
       round(users / nullif(lag(users) over (order by step), 0), 4) as conversion_from_prev
from ordered
order by step
