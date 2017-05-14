import flask
from tasks import runclass
from tasks import runclass_batch
import hello_config
from flask import Flask, request
from flask import render_template
from pyspark import SparkConf, SparkContext
from FuncSet import *
from ClassSet import *
import json
import requests
import pymysql
import sys
import time
#sys.stdout = open('/home/ubuntu/project_src/flaskapp/output.logs', 'w')
#print("testing1")
#sys.stdout = sys.__stdout__
#print("testing")
db = pymysql.connect("localhost","ubuntu","","bigdata")
#cursor = db.cursor()  this is just a basic cursor, can only get column by index
cursor = db.cursor(pymysql.cursors.DictCursor)
app = Flask(__name__)
conf = SparkConf()
conf.setAppName("testSpark")
sc = SparkContext.getOrCreate(conf=conf)
bdurl = hello_config.BDURL
url = bdurl+"/runclass"
feature0='125	20170326123042	0	125	0	1	4	1	55	55	0	0	0	0	0	0	0	0.00723	scheduling'
feature1='101	20170314031133	0	159830964	0	0	2	1	0	0	1	10	22	0	0	0	1	0.04124	normal'
feature2='155	20170326170102	0	155	0	1	0	0	34	1	0	0	0	1258	0	0	0	0.00225	blackhole'
feature3='5	20170418083936	0	5	0	1	0	0	34	1	0	0	0	1258	0	0	0	0.00225	blackhole'
feature4='4	20170418083929	0	4	0	1	0	0	34	1	0	0	0	1258	0	0	0	0.00225	blackhole'
@app.route('/')
def hello_world():
	return 'From python hello!'

@app.route('/index')
def index():
	return render_template("index.html")

@app.route('/triggerrunclass')
def test_spk_tst_cnt():
#	url = "http://ec2-54-218-250-157.us-west-2.compute.amazonaws.com/getSpkTstCnt"
	loga = LogItem("con_nnn11","aaa","xxx","xxx000n",feature1,"yyy","yyy000n","yyyymmddhhmmss")
#	pdata = json.dumps(loga,default=jdft)
	sql = "select count(*) as 'count' from chkrst_history where connection_id='"+loga.connection_id+"'"
	cursor.execute(sql)
	record = cursor.fetchall()
	if record[0]['count']!=0:
		return json.dumps(ReturnCode("duplicate"),default=jdft)
	
	sql = "insert into chkrst_history values('"+loga.connection_id+"','"+loga.current_CH_id+"','"+loga.from_node_id+"','"+loga.from_node_cluster_id+"','"+loga.to_node_id+"','"+loga.to_node_cluster_id+"','"+loga.timestamp+"','','"+loga.traffic_pattern+"',' ',' ',' ')"
	cursor.execute(sql)
	db.commit()
	logax = LogItemX(loga.connection_id,loga.traffic_pattern)
	pdata = json.dumps(logax,default=jdft)		
	runclass.delay(pdata)
	return json.dumps(ReturnCode("ok"),default=jdft)
#	rsp = requests.post(url,data=pdata)
#	return rsp.text
#	return "after processed by test api " + rsp.headers['Content-Type']
@app.route('/getSpkTstCnt', methods=['POST','GET'])
def spk_tst_cnt():
#	sc = SparkContext(conf=conf)
#	test_data_file = "./corrected.gz"(this expression only work in case py app is submitted with python3 or spark-submit
	test_data_file = "hdfs://ip-172-31-25-255:9000/user/ubuntu/corrected.gz"
#	test_raw_data = sc.textFile(test_data_file)
#	tst_cnt = test_raw_data.count()
	tst_cnt = 1
	contentType = ""
	jsondump = ""
#	if request.headers['content-type'] == 'text/plain':
#		contentType = 'text/plain'
#	elif request.headers['Content-Type'] == 'application/json':
#		contentType = 'application/json'
	#jsondump = json.dumps(request.json,default=jdft)
	jsondump = request.data.decode('utf-8')
#	contentType = request.headers.__dict__
#	itval = ""
#	for key, val in contentType.items():
#		itval = itval + key +":"+ val +"  "
#	rtn = str(tst_cnt) + " request type=" + request.method + " *** " + jsondump
	
	newLog = json.loads(jsondump,object_hook=logitem_json_hook)
	rtndict = {"connection_id":newLog.connection_id,"attacktype":"dummy"}
	rtn = json.dumps(rtndict)
	#rtn = jsondump
	return rtn
@app.route('/backend/bigdata/refresh', methods=['GET'])
def rstrefresh():
	rshitem1 = RefshItem("con_1","CH_002","from_n002","from_cluster_00a","probe","to_n004","to_cluster_009","20170210123022")
	rshitem2 = RefshItem("con_12","CH_0022","from_n0021","from_cluster_00b","probe","to_n004","to_cluster_009","20170210123021")
	lst = list()
#	lst.append(rshitem1)
#	lst.append(rshitem2)
	sql = "select * from chkrst_history"
	cursor.execute(sql)
	results = cursor.fetchall()
	for row in results:
		if row["action"] !=" " or row["attacktype"] in (" ","normal","sybil_attack"):
			continue
		rshitemd = RefshItem(row["connection_id"],row["current_CH_id"],row["from_node_id"],row["from_node_cluster_id"],row["attacktype"],row["to_node_id"],row["to_node_cluster_id"],row["timestamp"])
		lst.append(rshitemd)
	rtnlst = RefshLst(lst)
	return json.dumps(rtnlst,default=jdft)

@app.route('/backend/bigdata/history', methods=['POST'])
def rtvhistory():
	lst = list()
	jsondump = request.data.decode('utf-8')
	historyRtvRst = json.loads(jsondump, object_hook=historyrtv_json_hook)
	sql = "select * from chkrst_history where timestamp >='"+historyRtvRst.ftimestamp+"' and timestamp <='"+historyRtvRst.ttimestamp+"'"
	sys.stdout = open('/home/ubuntu/project_src/flaskapp/history_output.logs', 'w')
	print(sql)
	sys.stdout = sys.__stdout__
	cursor.execute(sql)
	results = cursor.fetchall()
	for row in results:
		rshitemd = RefshItem(row["connection_id"],row["current_CH_id"],row["from_node_id"],row["from_node_cluster_id"],row["attacktype"],row["to_node_id"],row["to_node_cluster_id"],row["timestamp"])
		lst.append(rshitemd)
	rtnlst = RefshLst(lst)
#	print(json.dumps(rtnlst,default=jdft)
#	sys.stdout = sys.__stdout__
	return json.dumps(rtnlst,default=jdft)


@app.route('/backend/bigdata/attcksolved', methods=['POST','GET'])
def attcksolved():
	if request.method == 'POST':
		jsondump = request.data.decode('utf-8')
		solveda = json.loads(jsondump, object_hook=attacksolved_json_hook)
		sql = "update chkrst_history set action ='"+solveda.action+"', comment ='"+solveda.comment+"',updatetime = '"+solveda.updatetime+"' where connection_id='"+solveda.connection_id+"'"
		cursor.execute(sql)
		db.commit()
		return json.dumps(ReturnCode("updated"),default=jdft)
	return json.dumps(ReturnCode(request.method),default=jdft)

@app.route('/backend/bigdata/nodesolved', methods=['POST','GET'])
def nodesolved():
	if request.method == 'POST':
		jsondump = request.data.decode('utf-8')
		solveda = json.loads(jsondump, object_hook=nodesolved_json_hook)
		sql = "update chkrst_history set action ='"+solveda.action+"', comment ='"+solveda.comment+"',updatetime = '"+solveda.updatetime+"' where from_node_id='"+solveda.from_node_id+"'"
		cursor.execute(sql)
		db.commit()
		return json.dumps(ReturnCode("updated"),default=jdft)
	return json.dumps(ReturnCode(request.method),default=jdft)


@app.route('/backend/bigdata/feedlog', methods=['POST','GET'])
def feedlog():
	loga = LogItem("con_nnn7804","aaa","xxx","xxx000n",feature4,"yyy","yyy000n","yyyymmddhhmmss")
#       pdata = json.dumps(loga,default=jdft)
	if request.method == 'POST':
		jsondump = request.data.decode('utf-8')
		#sys.stdout = open('/home/ubuntu/project_src/flaskapp/output.logs', 'w')
		#print("testing1")
		#print(str(request.data))
		#print("testing2")
		#sys.stdout = sys.__stdout__
		loga = json.loads(jsondump, object_hook=logitem_json_hook)
	sql = "select count(*) as 'count' from chkrst_history where connection_id='"+loga.connection_id+"'"
	cursor.execute(sql)
	record = cursor.fetchall()
	if record[0]['count']!=0:
		return json.dumps(ReturnCode("duplicate"),default=jdft)
	sql = "insert into chkrst_history values('"+loga.connection_id+"','"+loga.current_CH_id+"','"+loga.from_node_id+"','"+loga.from_node_cluster_id+"','"+loga.to_node_id+"','"+loga.to_node_cluster_id+"','"+loga.timestamp+"','','"+loga.traffic_pattern+"',' ',' ',' ')"
	cursor.execute(sql)
	db.commit()
	logax = LogItemX(loga.connection_id,loga.traffic_pattern)
	pdata = json.dumps(logax,default=jdft)	
	runclass.delay(pdata)	
	return json.dumps(ReturnCode("ok"),default=jdft)

@app.route('/backend/bigdata/NB_batch', methods=['POST'])
def NB_batch():
	lst = list()
	jsondump = request.data.decode('utf-8')
	historyRtvRst = json.loads(jsondump, object_hook=historyrtv_json_hook)
	sql = "select * from chkrst_history where timestamp >='"+historyRtvRst.ftimestamp+"' and timestamp <='"+historyRtvRst.ttimestamp+"'"
	cursor.execute(sql)
	results = cursor.fetchall()
	for row in results:
		blkitem = LogItemX_NB(row["connection_id"],row["timestamp"], row["traffic_pattern"])
		lst.append(blkitem)
	pdlst = BlkLst(lst)
	uid = str(int(round(time.time() * 1000)))
	sql = "insert into blkrst_history values('"+uid+"','"+historyRtvRst.ftimestamp+"','"+historyRtvRst.ttimestamp+"',' ')"
	cursor.execute(sql)
	db.commit()
	blkiddetail = BlkIdDetail(uid,json.dumps(pdlst,default=jdft))
	pdata = json.dumps(blkiddetail,default=jdft)
	runclass_batch.delay(pdata)
	return json.dumps(ReturnCode(uid),default=jdft)

@app.route('/backend/bigdata/NB_blkrqrst', methods=['POST'])
def NB_blkrqrst():
	jsondump = request.data.decode('utf-8')
	data = json.loads(jsondump)
	sql = "select count(*) as 'count' from blkrst_history where rq_id='"+data["rq_id"]+"'"
	cursor.execute(sql)
	record = cursor.fetchall()
	sys.stdout = open('/home/ubuntu/project_src/flaskapp/output.logs', 'w')
#	sys.stdout = sys.__stdout__
	if record[0]['count']==0:
		print(data["rq_id"])
		sys.stdout = sys.__stdout__
		return " "
	sql = "select * from blkrst_history where rq_id='"+data["rq_id"]+"'"
	cursor.execute(sql)
	results = cursor.fetchall()
	detail = results[0]["detail"]
	if detail == " ":
		return " "
#	for row in results:
#		detail = row['detail']
#		break
#	print(data["rq_id"]+" ***** "+results[0]['detail']+ " $$$ " +detail)
	print(data["rq_id"] + " ***** " + detail)
	sys.stdout = sys.__stdout__
	
	return detail

if __name__ == '__main__':
	app.run('0.0.0.0') #it open up to all kind of port and address
#	app.run()
