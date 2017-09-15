#coding=utf-8

import os
import sys
import dataapi
import datasql
import pandas as pd
import numpy as np
import json
import fileutil

class SelectItem:
	def __init__(self, secuCode, industryCode, afloatcap, income, weights_cap, weights_in):
		self.secuCode = secuCode
		self.industryCode = industryCode
		self.afloatcap = afloatcap
		self.income = income
		self.weights_cap = weights_cap
		self.weights_in = weights_in
		
	def __repr__(self):
		return repr((self.secuCode, self.industryCode, self.afloatcap, self.income, self.weights_cap, self.weights_in))
	#def __str__(self):
	#	return self.__dict__

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
#按升序排列
def getTradingDays(df):
	tdarray = df['TradingDay'].unique()
	tdlist = tdarray.tolist()
	return sorted(tdlist, key=lambda x:x)

def filter(df):
	#df - DataFrame，其中是当期因子数据
	#return - DataFrame，通过筛选之后的对象
	#筛选条件
	col1 = "GP_Margin"
	col2 = "ROE"
	col3 = "Income_Growth_YOY_Comparable"
	col1door = 0.3
	col2door = 0.15
	col3door = 0
	
	matchdf = df[(df[col1] > col1door) & (df[col2] > col2door) & (df[col3] > col3door)]
	matchdf.sort_values([col1, col2, col3], ascending=[False, False, False])
	
	return matchdf

def getSelectItem(df):
	#df - DataFrame, 筛选之后得到的数据
	#return - 一个列表，每个元素为一个SelectItem对象
	
	itemlst = []
	totalcap = float(df['AFloatsCap'].sum())
	totalin = float(df['TTMIncome'].sum())
	for i, row in df.iterrows():
		secuCode = row['SecuCode']
		industryCode = row['IndustrySecuCode_I']
		afloatcap = float(row['AFloatsCap'])
		income = float(row['TTMIncome'])
		weights_cap = afloatcap/totalcap
		weights_in = income/totalin
		
		item = SelectItem(secuCode, industryCode, afloatcap, income, weights_cap, weights_in)
		itemlst.append(item)
		
	return itemlst

def handleAllDay(tds, curpath):
	#tds - 交易日列表
	#curpath - 每一期数据存放目录
	#return - 字典对象，key: 交易日, value: 组合列表; 选不出的时间
	ports = {}
	nullrecord = []
	preitems = None
	for td in tds:
		filename = "{0}/{1}.pkl".format(curpath, td)
		#print('handle the file: {0}'.format(filename))
		df = pd.read_pickle(filename)
		
		mdf = filter(df)
		#如果超出50个，自选前50个
		if len(mdf) > 50:
			mdf = mdf.head(50)
			
		items = getSelectItem(mdf)
		
		#如果选不出结果，延续使用上一期结果
		if items is None or len(items) == 0:
			items = preitems
			nullrecord.append(td)
			
		preitems = items
		ports[td] = items
	return ports, nullrecord
		
if __name__ == "__main__":
	#获得交易日
	filename_td = "./data/tradingday_monthly.pkl"
	#tddf = getTradingDay_Monthly()
	#tddf.to_pickle(filename_td)
	tddf = pd.read_pickle(filename_td)
	tdarray = getTradingDays(tddf)
	
	#获得因子数据
	#filename = "./data/factordata_monthly.pkl"
	#fetchMonthlyData(tdarray)
	#df.to_pickle(filename)
	
	#根据条件筛选出符合要求的
	#df.sort(
	ports, nullrecord = handleAllDay(tdarray, "./data/")
	strdata = json.dumps(ports, default=lambda x:x.__dict__)
	fileutil.writeFile("./data/final.json", strdata)
	
	strnullrecord = json.dumps(nullrecord)
	fileutil.writeFile("./data/nullrecord.json", strnullrecord)
	
	