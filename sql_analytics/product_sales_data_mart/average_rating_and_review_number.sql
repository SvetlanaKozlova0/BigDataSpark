select
	product_id,
	product_name,
	avg_rating,
	review_count
from
	reports.product_sales ps
order by
	avg_rating desc,
	review_count desc
limit 30;