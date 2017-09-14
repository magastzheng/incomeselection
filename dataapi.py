#encoding=utf-8

import dbaccessor
import fileutil

def getengine():
	host = '176.1.11.55'
	#host = 'localhost'
	user = 'zhenggq'
	password = 'Yuzhong0931'
	#password = 'yuzhong'
	return dbaccessor.getdb('sqlserver', host, user, password)

def getSqlFormat(filename):
	'''读取给定文件的内容，并以字符串方式返回
		filename - 指定的文件名，绝对路径和相对路径都可以
		return - 文件内容字符串
	'''
	return fileutil.readFile(filename)

def getData(sql):
	'''给定sql语句，到数据查询数据，并返回DataFrame对象
		sql - 字符串类型的sql语句
		return - 一个DataFrame对象，包含查询到的数据
	'''
	
	engine = getengine()
	df = dbaccessor.query(engine, sql)
	engine.dispose()
	
	return df