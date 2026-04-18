select
	supplier_id,
	supplier_name,
	avg_product_price
from
	reports.supplier_sales 
order by
	avg_product_price desc
limit 30;