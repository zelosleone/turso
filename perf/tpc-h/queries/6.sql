select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= cast('1994-01-01' as datetime)
	and l_shipdate < cast('1995-01-01' as datetime) -- modified not to include cast({'year': 1} as interval)
	and l_discount between 0.08 - 0.01 and 0.08 + 0.01
	and l_quantity < 24;
