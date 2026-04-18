select
	store_id,
	store_name,
	avg_check
from 
	reports.store_sales ss 
order by
	avg_check desc
limit 30;