select
	country,
	count(*) as customer_count,
	sum(total_spent) as total_revenue_by_country
from
	reports.customer_sales cs 
group by
	country
order by
	customer_count desc
limit 30;