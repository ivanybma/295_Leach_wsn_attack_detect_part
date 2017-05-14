from celery import Celery
import hello_config
import requests
import pymysql
import json
import sys
from FuncSet import *
from ClassSet import *
app = Celery('tasks', backend='rpc://', broker='amqp://localhost//')
bdurl = hello_config.BDURL
url = bdurl+"/runclass"
url_batch = bdurl + "/runclass_batch"
db = pymysql.connect("localhost","ubuntu","","bigdata")
cursor = db.cursor(pymysql.cursors.DictCursor)
@app.task
def add(x,y):
	return x+y
@app.task
def runclass(pdata):
	rsp = requests.post(url,data=pdata)
	sys.stdout = open('/home/ubuntu/project_src/flaskapp/tasks_output.logs', 'w')
	print(rsp.text)
	sys.stdout = sys.__stdout__
	classrst = json.loads(rsp.text, object_hook=classrst_json_hook)
	sql = "update chkrst_history set attacktype='"+classrst.attacktype+"' where connection_id='"+classrst.connection_id+"'"
	cursor.execute(sql)
	db.commit()
	results = cursor.fetchall()
#	for row in results:
#		rshitemd = RefshItem(row["connection_id"],row["current_CH_id"],row["from_node_id"],row["from_node_cluster_id"],row["attacktype"],row["to_node_id"],row["to_node_cluster_id"],row["timestamp"])
#	return rshitemd.connection_id+" ****** "+rshitemd.attacktype+" ******** "+rsp.text
	return rsp.text

@app.task
def runclass_batch(pdata):
	data0 = json.loads(pdata)
	rsp = requests.post(url_batch, data=data0["detail"])
	sql = "update blkrst_history set detail='"+rsp.text+"' where rq_id='"+data0["uid"]+"'"
	cursor.execute(sql)
	db.commit()
	return data0["uid"] + " **** " + rsp.text
