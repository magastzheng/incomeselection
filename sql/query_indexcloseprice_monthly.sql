select a.TradingDay, b.ClosePrice
from
(select convert(varchar, TradingDay, 112) as TradingDay from basicdb.dbo.FactorTradingDay
where IfMonthEnd=1) a
join (select * from zhenggq.dbo.IndexTradingDataDaily
	where SecuCode='000300') b
on a.TradingDay = b.TradingDay
order by a.TradingDay