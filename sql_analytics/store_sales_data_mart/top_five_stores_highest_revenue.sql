select
	year_month,
	avg_check,
	avg_items_per_order
from
	reports.monthly_sales ms 
order by 
	year_month;