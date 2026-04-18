select
	substring(year_month, 1, 4) as year,
	sum(total_revenue) as yearly_revenue,
	sum(total_orders) as yearly_orders
from
	reports.monthly_sales ms
group by
	year
order by
	year;