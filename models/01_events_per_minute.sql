select date_trunc('minute', ts) as minute, type, count(*) as events, sum(amount) as revenue
from raw_events
group by all
order by minute, type
