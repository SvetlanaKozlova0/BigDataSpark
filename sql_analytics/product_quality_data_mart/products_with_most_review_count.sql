select 
	product_id,
	product_name,
	category_name,
	review_count,
	rating,
	total_quantity_sold
from
	reports.product_quality pq 
order by
	review_count desc
limit 10;