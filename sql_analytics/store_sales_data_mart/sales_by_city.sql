select
	country,
	city,
	sum(total_revenue) as city_revenue,
	count(distinct store_id) as store_count
from
	reports.store_sales 
group by
	country, city
order by
	city_revenue desc
limit 30;