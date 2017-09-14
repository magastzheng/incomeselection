#coding=utf-8

import dbaccessor
import pandas
import datetime
import dataapi

#TradingDay type
#1 - All TraingDay
#2 - Weekly TradingDay
#3 - Monthly TradingDay
#交易日sql文件
td_type_files = {
		"d": "query_tradingday_daily.sql",
		"w": "query_tradingday_weekly.sql",
		"m": "query_tradingday_monthly.sql"
}

#因子数据sql模板文件
fd_type_files = {
		"d": "query_factordata_daily.sql",
		"w": "query_factordata_weekly.sql",
		"m": "query_factordata_monthly.sql"
}

#获取交易日sql语句
def getSqlTradingDay(dtype, path):
	'''	获取指定频率类型的交易日查询sql语句
		dtype - 指定频率类型 d/w/m 分别表示每日/每周/每月
		path - sql子目录的根目录
		return - 查询sql语句字符串
	'''
	sqlfile = td_type_files.get(dtype, query_tradingday_monthly)
	filepath = '{0}/sql/{1}'.format(path, sqlfile)
	fmt = dataapi.getSqlFormat(filepath)

	return fmt

#获取因子数据查询语句
def getSqlFactorData(dtype, path):
	''' 获取指定频率类型的因子查询sql语句
		dtype - 指定频率类型 d/w/m 分别表示每日/每周/每月
		path - sql子目录的根目录
		return - 查询sql语句字符串模板

		注意 - 对于数据sql，仅仅存放模板，还需要加入时间才能构成完整sql语句
	'''
	sqlfile = fd_type_files.get(dtype, query_factordata_monthly)
	filepath = '{0}/sql/{1}'.format(path, sqlfile)
	fmt = dataapi.getSqlFormat(filepath)

	return fmt

#获取任意文件中查询语句
def getSqlString(filename, path):
	'''	获得指定文件中的内容
		filename - sql语句存放的文件
		path - sql子目录的跟目录
		return - 文件内容字符串
		
		注意 - 文件必须存放在sql目录下
	'''
	filepath = '{0}/sql/{1}'.format(path, filename)
	fmt = dataapi.getSqlFormat(filepath)
	return fmt
	
#获取交易日
def getTradingDay(dtype, path):
	''' 获取指定频率类型的交易日
		dtype - 指定频率类型 d/w/m 分别表示每日/每周/每月
		path - sql子目录的根目录
		return - 交易日列表，每个元素都是datetime类型
	'''

	#获得查询sql语句
	sql = getSqlTradingDay(dtype, path)
	
	#执行数据库查询
	df = dataapi.getData(sql)

	#把处理成datetime类型
	df['TradingDay'] = df['TradingDay'].apply(lambda x:datetime.datetime.strptime(x, '%Y%m%d'))

	return df['TradingDay'].tolist()

#获取因子数据
def getFactorData(dtype, td, path):
	''' 获取指定频率类型、指定交易日的因子数据
		dtype - 指定频率类型 d/w/m 分别表示每日/每周/每月
		td - 指定的交易日为datetime.date类型
		path - sql子目录的根目录
		return - pandas DataFrame类型的数据，存放当期所有股票因子数据
	'''
	sqlfmt = getSqlFactorData(dtype, path)
	sql = sqlfmt.format(td.strftime('%Y%m%d'))

	df = dataapi.getData(sql)
	return df

#获取任意数据
def getGeneralData(filename, path):
	sql = getSqlString(filename, path)
	df = dataapi.getData(sql)
	
	return df
