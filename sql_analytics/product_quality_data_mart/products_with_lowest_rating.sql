select
	product_id,
	product_name,
	category_name,
	rating,
	review_count,
	total_quantity_sold
from 	
	reports.product_quality pq
order by 
	rating asc
limit 5;