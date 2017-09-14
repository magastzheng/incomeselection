#encoding=utf-8

import sqlalchemy
import pymysql
import pymssql
import pandas as pd
import datetime

#database connection protocol
dbtypemap = {
	'sqlserver': 'mssql+pymssql',
	'mysql': 'mysql+pymysql',
}

#database server port
dbportmap = {
	'sqlserver': 1433,
	'mysql': 3306,
}

def getconstr(dbtype, host, user, password, db, port):
	"""	Get the database connection string
		dbtype - the database type, such as sqlserver, mysql
		host - the database server
		user - the database account
		password - the database account password
		db - the database name
		port - the database port

		return - the string type of connection string
	"""
	dbtypeval = dbtypemap.get(dbtype, 'mssql')
	if port == 0:
		port = dbportmap.get(dbtype, 1433)
	
	constr = ''
	if len(db) > 0:
		constr = '{0}://{1}:{2}@{3}:{4}/{5}'.format(dbtypeval, user, password, host, port, db)
	else:
		constr = '{0}://{1}:{2}@{3}:{4}'.format(dbtypeval, user, password, host, port)
	
	return constr
	
def getdb(dbtype, host, user, password, db='', port=0):
		"""	Open the database connection
			dbtype - the database type, such as sqlserver, mysql
			host - the database server
			user - the database account
			password - the database account password
			db - the database name
			port - the database port

			return - the engine object
		"""
		
		constr = getconstr(dbtype, host, user, password, db, port)
		
		print 'db connection string: {0}'.format(constr)
		
		start = datetime.datetime.now()
		engine = sqlalchemy.create_engine(constr)
		end = datetime.datetime.now()
		
		print 'Cost: {0}. Open database: {1}'.format(end-start, constr)

		return engine

def query(engine, sql):
		"""Execute a sql query. It will return an pandas.DataFrame object and then close the connection"""
		conn = engine.connect()
		#execute sql here
		resultProxy = conn.execute(sql) 

		#convert the sa data to pandas DataFrame
		df = pd.DataFrame(resultProxy.fetchall(), columns=resultProxy.keys())
		conn.close()

		return df

def raw_query(engine, sql):
		"""Execute a raw_query. It will return a ResultProxy object. Caller needs to close it after finishing."""
		conn = engine.connect()
		#execute sql here
		resultProxy = conn.execute(sql) 
		conn.close()
		
		return resultProxy

if __name__ == '__main__':
		#test the mysql
		sql = 'select * from importrecord'
		engine = getdb(dbtype='mysql', host='176.1.3.10', user='zhenggq', password='Yuzhong0931', db='hfqcommodity')
		#result = query(engine, sql)
		result = raw_query(engine, sql)
		#print result
		print result.keys()
		for k in result.keys():
			print k
		#destroy the engine, it will allocate the database connection
		engine.dispose()
		
		#test the sql server
		sql = 'select * from securitybasicinfo'
		engine = getdb(dbtype='sqlserver', host='176.1.11.55', user='zhenggq', password='Yuzhong0931', db='basicdb')
		df = query(engine, sql)
		engine.dispose()
		