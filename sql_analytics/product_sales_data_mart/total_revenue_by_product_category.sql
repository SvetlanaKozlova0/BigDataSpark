select
	category_name,
	sum(total_revenue) as category_revenue
from
	reports.product_sales ps
group by
	category_name
order by	
	category_revenue desc
limit 30;