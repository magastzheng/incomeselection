#coding=utf-8

import os
import sys
import dataapi
import datasql
import pandas as pd
import numpy as np
import json
import fileutil
import matplotlib.pyplot as plt
import codecs
import copy

class SelectItem:
	def __init__(self, secuCode, industryCode, closeprice, afloatcap, income, weights_cap, weights_in):
		self.secuCode = secuCode
		self.industryCode = industryCode
		self.closeprice = closeprice
		self.afloatcap = afloatcap
		self.income = income
		self.weights_cap = weights_cap
		self.weights_in = weights_in
		
	def __repr__(self):
		return repr((self.secuCode, self.industryCode, self.closeprice, self.afloatcap, self.income, self.weights_cap, self.weights_in))
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
	#return - 一个列表，每个元素为一个字典对象
	
	itemlst = []
	totalcap = float(df['AFloatsCap'].sum())
	totalin = float(df['TTMIncome'].sum())
	for i, row in df.iterrows():
		secuCode = row['SecuCode']
		industryCode = row['IndustrySecuCode_I']
		afloatcap = float(row['AFloatsCap'])
		closeprice = float(row['ClosePrice'])
		income = float(row['TTMIncome'])
		weights_cap = afloatcap/totalcap
		weights_in = income/totalin
		
		#item = SelectItem(secuCode, industryCode, closeprice, afloatcap, income, weights_cap, weights_in)
		item = {
			"secuCode": secuCode,
			"industryCode": industryCode,
			"closeprice": closeprice,
			"afloatcap": afloatcap,
			"income": income,
			"weights_cap": weights_cap,
			"weights_in": weights_in,
		}
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

def allocate(items, weight_cap_cap, weight_in_cap, weight_eq_cap):
	#items - 本期列表
	#weight_cap_cap - 按市值分配权重的总市值
	#weight_in_cap - 按收入分配的总市值
	#weight_eq_cap - 等权重分配的总市值
	#计算各种权重分配的持股数
	#return - 返回新的列表，在原有基础上添加先字段表示持股数
	
	num = len(items)
	nitems = []
	for item in items:
		nitem = copy.deepcopy(item)
		closeprice = nitem["closeprice"]
		if closeprice > 0:
			nitem["shares_cap"] = weight_cap_cap * nitem["weights_cap"] / closeprice
			nitem["shares_in"] = weight_in_cap * nitem["weights_in"] / closeprice
			nitem["shares_eq"] = weight_eq_cap / (num * closeprice) 
		else:
			nitem["shares_cap"] = 0.0
			nitem["shares_in"] = 0.0
			nitem["shares_eq"] = 0.0
		nitems.append(nitem)
	return nitems

def settle(items, df):
	#计算持股到期时的总市值，组合是上期选出来的，股价在到期时候确定
	#items - 上一期持股列表
	#df - 本期全市场股票信息
	#return - 返回新的列表，在原有列表基础上添加新字段表示到期市值
	
	nitems = []
	for item in items:
		nitem = copy.deepcopy(item)
		secuCode = nitem['secuCode']
		#print("****{0}****".format(secuCode))
		closeprice = 0.0
		newdf = df[df['SecuCode'].isin([secuCode])]
		if len(newdf) > 0:
			closeprice = float(newdf[newdf['SecuCode'] == secuCode]['ClosePrice'])
		
		nitem["next_closeprice"] = closeprice
		nitem['weights_cap_cap'] = closeprice * nitem['shares_cap']
		nitem['weights_in_cap'] = closeprice * nitem['shares_in']
		nitem['weights_eq_cap'] = closeprice * nitem['shares_eq']
		
		nitems.append(nitem)
		
	return nitems

def getcaponeperiod(td, items):
	#td - 交易日
	#items - 一期组合
	#return - 一个字典包含交易日，市值等
	weights_cap_total = sum(d['weights_cap_cap'] for d in items)
	weights_in_total = sum(d['weights_in_cap'] for d in items)
	weights_eq_total = sum(d['weights_eq_cap'] for d in items)
	p = {"td": td, "weights_cap": weights_cap_total, "weights_in": weights_in_total, "weights_eq": weights_eq_total}
	
	return p
	
def getcap(tds, dictdata, totalcap):
	#tds - 排过序的交易日
	#dictdata - 每一期选出的股票，key: 交易日，value: 为股票组合列表
	#totalcap - 初始总投入
	#return - 每一期总市值
	
	data = []
	for td in tds:
		items = dictdata[td]
		p = getcaponeperiod(items)
		data.append(p)
		
	return data

	
def workflow(tds, ports):
	#tds - 排好序的交易日
	#ports - 选出的每期组合, key: 交易日, value: 为股票组合列表
	#return - 每一期总市值列表，每个元素是一个字典，包含: td, weights_cap, weights_in, weights_eq
	
	datas = []
	newports = {}
	num = len(tds)
	weight_cap_cap = 10000.0
	weight_in_cap = 10000.0
	weight_eq_cap = 10000.0
	for i in range(num):
		td = tds[i]
		items = ports[td]
		
		p = None
		if i == 0:
			p = {"td": td, "weights_cap": weight_cap_cap, "weights_in": weight_in_cap, "weights_eq": weight_eq_cap}
		else:
			#计算上一期持股到期市值，需要获得上一期组合，在根据本期全市场数据获得收盘价计算
			prevtd = tds[i-1]
			previtems = newports[prevtd]
			filename = "./data/{0}.pkl".format(td)
			df = pd.read_pickle(filename)
			citems = settle(previtems, df)
			p = getcaponeperiod(td, citems)
			
			#更新新市值
			weight_cap_cap = p["weights_cap"]
			weight_in_cap = p["weights_in"]
			weight_eq_cap = p["weights_eq"]
		
		#期初再平衡
		nitems = allocate(items, weight_cap_cap, weight_in_cap, weight_eq_cap)
		
		#存储本期分配后信息，以便计算持有到期市值
		newports[td] = nitems

		#把当前市值加入列表
		datas.append(p)
		
	return datas

def draw(datas):
	x = []
	xlabel = []
	y_cap = []
	y_in = []
	y_eq = []
	
	size = len(datas)
	for i in range(size):
		current = datas[i]
		x.append(i+1)
		xlabel.append(current["td"])
		y_cap.append(current["weights_cap"])
		y_in.append(current["weights_in"])
		y_eq.append(current["weights_eq"])
	
	fig,ax = plt.subplots()
	#设置横坐标
	ax.set_xticks(x)
	ax.set_xticklabels(xlabel, rotation=40)
	#plt.xticks(x, xlabel)
	plt.plot(x, y_cap, label="cap weight")
	plt.plot(x, y_in, label="income weight")
	plt.plot(x, y_eq, label="equal weight")
	plt.legend(loc="upper left")
	plt.show()

def output_port_csv(tdarray, ports, filename):
	#with codecs.open('./data/port.csv', 
	fmt = "{0},{1},{2},{3},{4},{5},{6},{7}\n"
	header = "TradingDay,SecuCode,IndustryCode,ClosePrice,AFloatCap,Income,Weights_cap,Weights_in\n"
	with codecs.open(filename=filename, mode='w', encoding='utf-8') as f:
		f.write(header)
		for td in tdarray:
			items = ports[td]
			for item in items:
				line = fmt.format(td, item["secuCode"], item["industryCode"], item["closeprice"], item["afloatcap"], item["income"], item["weights_cap"], item["weights_in"])
				f.write(line)

def output_point_csv(datas, filename):
	fmt = "{0},{1},{2},{3}\n"
	header = "TradingDay,Weights_cap,Weights_in,Weights_eq\n"
	with codecs.open(filename=filename, mode='w', encoding='utf-8') as f:
		f.write(header)
		for data in datas:
			line = fmt.format(data["td"], data["weights_cap"], data["weights_in"], data["weights_eq"])
			f.write(line)
			
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
	#ports, nullrecord = handleAllDay(tdarray, "./data/")
	#strdata = json.dumps(ports, default=lambda x:x.__dict__)
	#fileutil.writeFile("./data/final.json", strdata)
	
	#strnullrecord = json.dumps(nullrecord)
	#fileutil.writeFile("./data/nullrecord.json", strnullrecord)
	
	#从选出数据中计算权重
	strjson = fileutil.readFile("./data/final.json")
	ports = json.loads(strjson)
	
	#output_port_csv(tdarray, ports, "./data/port.csv")
	datas = workflow(tdarray, ports)
	
	strpoints = json.dumps(datas)
	fileutil.writeFile("./data/point.json", strpoints)
	
	#最终分配数据
	#strpoints = fileutil.readFile("./data/point.json")
	#datas = json.loads(strpoints)
	
	#output_point_csv(datas, "./data/point_csv.csv")
	
	draw(datas)