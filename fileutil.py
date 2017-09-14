#coding=utf8
import os
import pandas as pd
import datetime

def readFile(fileName):
	"""按文本方式读取文件，并返回所有文本内容
		fileName - 文件名（包含完整路径）
		return - 文本内容
	"""
	text = ''
	if os.path.exists(fileName) and os.path.isfile(fileName):
			fileObj = open(fileName)
			try:
					text=fileObj.read()
			finally:
					fileObj.close()
	else:
			print 'File: {0} is not found!'.format(fileName)
	
	return text

def writeFile(fileName, text):
	"""往指定文件中写入文本
		fileName - 指定写入文本的文件，包含完整路径
		text - 需要写入的文本
	"""
	fileObj = open(fileName, 'w')
	try:
			fileObj.write(text)
	finally:
			fileObj.close()

def saveCSV(filepath, td, df):
	""" 保存成csv格式
		filepath - 保存文件的路径名，为str类型
		td - 交易日，为datetime.date类型
		df - pandas DataFrame对象
	"""
	df['SecuAbbr'] = df['SecuAbbr'].apply(lambda x:x.encode('raw-unicode-escape').decode('gbk'))
	#df['SecuAbbr'] = df['SecuAbbr'].apply(lambda x:x.decode('gbk'))
	filename = td.strftime('%Y%m%d')
	fullpath='{0}{1}.csv'.format(filepath, filename)
	df.to_csv(fullpath, encoding='gbk')

def loadCSV(filepath, td):
	"""	从csv格式文件中读入到pandas DataFrame
		filepath - 保存文件的路径名，为str类型
		td - 交易日，为datetime.date类型
	"""
	filename = td.strftime('%Y%m%d')
	fullpath='{0}{1}.csv'.format(filepath, filename)
	df = None
	if os.path.exists(fullpath):
		df = pd.read_csv(fullpath)
	else:
		print 'Cannot open file: {0}'.format(fullpath)

	return df

def saveStockCSV(filepath, stock, df):
	""" 保存成csv格式
		filepath - 保存文件的路径名，为str类型
		td - 交易日，为datetime.date类型
		df - pandas DataFrame对象
	"""
	df['SecuAbbr'] = df['SecuAbbr'].apply(lambda x:x.encode('raw-unicode-escape').decode('gbk'))
	#df['SecuAbbr'] = df['SecuAbbr'].apply(lambda x:x.decode('gbk'))
	fullpath='{0}{1}.csv'.format(filepath, stock)
	df.to_csv(fullpath, encoding='gbk')

def loadStockCSV(filepath, stock):
	"""	从csv格式文件中读入到pandas DataFrame
		filepath - 保存文件的路径名，为str类型
		td - 交易日，为datetime.date类型
	"""
	fullpath='{0}{1}.csv'.format(filepath, stock)
	df = None
	if os.path.exists(fullpath):
		df = pd.read_csv(fullpath)
	else:
		print 'Cannot open file: {0}'.format(fullpath)

	return df

def savePickle(filepath, td, df):
	"""	保存成pickle格式
		filepath - 保存文件的路径名，为str类型
		td - 交易日，为datetime.date类型
		df - pandas DataFrame对象
	"""
	filename = td.strftime('%Y%m%d')
	fullpath='{0}{1}.pkl'.format(filepath, filename)
	df.to_pickle(fullpath)

def loadPickle(filepath, td):
	"""	从pickle格式文件中读入到pandas DataFrame
		filepath - 保存文件的路径名，为str类型
		td - 交易日，为datetime.date类型
	"""
	filename = td.strftime('%Y%m%d')
	fullpath='{0}{1}.pkl'.format(filepath, filename)
	df = None
	if os.path.exists(fullpath):
		df = pd.read_pickle(fullpath)
	else:
		print 'Cannot open file: {0}'.format(fullpath)

	return df

def saveStockPickle(filepath, secucode, df):
	"""	保存成pickle格式
		filepath - 保存文件的路径名，为str类型
		secucode - 股票代码
		df - pandas DataFrame对象
	"""
	fullpath='{0}{1}.pkl'.format(filepath, secucode)
	df.to_pickle(fullpath)

def loadStockPickle(filepath, secucode):
	"""	从pickle格式文件中读入到pandas DataFrame
		filepath - 保存文件的路径名，为str类型
		secucode - 股票代码
	"""

	fullpath='{0}{1}.pkl'.format(filepath, secucode)
	df = None
	if os.path.exists(fullpath):
		df = pd.read_pickle(fullpath)
	else:
		print 'Cannot open file: {0}'.format(fullpath)

	return df
