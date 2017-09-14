#coding=utf-8

import os
import sys
import dataapi
import datasql
import pandas as pd
import numpy as np

#获得交易日
def getTradingDay_Monthly():
	sqlfile = "query_tradingday_monthly.sql"
	curpath = os.getcwd()
	
	df = datasql.getGeneralData(sqlfile, curpath)
	#tdarray = df['TradingDay'].unique()
	
	return df

#获得原始数据
def fetchMonthlyData(tdarray):
	#tdarray - 交易日列表
	#保存到data目录下
	sqlfile = "query_factordata_monthly.sql"
	curpath = os.getcwd()
	
	#df = datasql.getGeneralData(sqlfile, curpath)
	sqlformat = datasql.getSqlString(sqlfile, curpath)
	for td in tdarray:
		sql = sqlformat.format(td)
		df = dataapi.getData(sql)
		filename = "./data/{0}.pkl".format(td)
		df.to_pickle(filename)

#获得交易日
def getTradingDays(df):
	tdarray = df['TradingDay'].unique()
	return tdarray.tolist()

if __name__ == "__main__":
	#获得交易日
	filename_td = "./data/tradingday_monthly.pkl"
	#tddf = getTradingDay_Monthly()
	#tddf.to_pickle(filename_td)
	tddf = pd.read_pickle(filename_td)
	tdarray = getTradingDays(tddf)
	
	#获得因子数据
	#filename = "./data/factordata_monthly.pkl"
	fetchMonthlyData(tdarray)
	#df.to_pickle(filename)
	
	#根据条件筛选出符合要求的
	#df.sort(