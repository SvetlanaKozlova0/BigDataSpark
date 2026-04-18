select
	product_id,
	product_name,
	category_name,
	total_quantity
from 
	reports.product_sales ps
order by 
	total_quantity desc
limit 
	10;