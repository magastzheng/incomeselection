select t.TradingDay, t.SecuCode, t.Weights, d.ClosePrice
from (
select a.TradingDay, b.SecuCode, b.Weights
from
(select convert(varchar, TradingDay, 112) as TradingDay from basicdb.dbo.FactorTradingDay
where IfMonthEnd=1) a
join
(select * from factordb.dbo.CSINextOpenWeights
where IndexCode='000300') b
on a.TradingDay=b.TradingDay) t
join zhenggq.dbo.TradingDataDaily d
on t.TradingDay = d.TradingDay
and t.SecuCode = d.SecuCode
order by TradingDay, SecuCode