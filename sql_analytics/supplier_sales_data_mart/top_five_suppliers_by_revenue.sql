select
	supplier_id,
	supplier_name,
	country,
	total_revenue,
	total_quantity,
	avg_product_price
from
	reports.supplier_sales ss 
order by
	total_revenue desc
limit 5;