select
	customer_id,
	first_name,
	last_name,
	country,
	total_spent
from
	reports.customer_sales cs 
order by
	total_spent desc
limit 10;