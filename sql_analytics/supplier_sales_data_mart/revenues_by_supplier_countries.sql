select
	country,
	sum(total_revenue) as total_revenue_by_supplier_country,
	count(distinct supplier_id) as supplier_count
from
	reports.supplier_sales
group by 
	country
order by
	total_revenue_by_supplier_country desc
limit 30; 