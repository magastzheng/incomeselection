select distinct convert(varchar, TradingDay, 112) as TradingDay
from winddb.dbo.FactorMonthly_Comparable
where TradingDay is not null
order by TradingDay