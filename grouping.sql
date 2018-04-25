select age, race, avg(capital_gain), sum(capital_gain)
from adult
group by grouping sets ((age), (race))
order by avg(capital_gain);
