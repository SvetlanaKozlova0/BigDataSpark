select
	corr(rating, total_quantity_sold) as rating_sales_correlation
from 	
	reports.product_quality pq 
where 
	total_quantity_sold > 0