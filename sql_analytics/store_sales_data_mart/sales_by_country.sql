select
	country,
	sum(total_revenue) as country_revenue,
	count(distinct store_id) as store_count
from 
	reports.store_sales ss 
group by
	country
order by
	country_revenue desc
limit 30;