select
	customer_id,
	first_name,
	last_name,
	avg_check
from
	reports.customer_sales cs 
order by
	avg_check desc
limit 30;