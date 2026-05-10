select user_id, count(*) filter (where type = 'purchase') as purchases, sum(amount) as revenue,
       max(ts) as last_seen
from raw_events
group by user_id
having purchases > 0
order by revenue desc
limit 20
