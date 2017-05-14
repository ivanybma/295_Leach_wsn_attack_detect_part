import createLabeledPoint
from createLabeledPoint import *
import flask
from flask import Flask, request
from flask import render_template
from pyspark import SparkConf, SparkContext
from FuncSet import *
from ClassSet import *
import json
import requests
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from numpy import array
app = Flask(__name__)
conf = SparkConf()
conf.setAppName("Classification")
try:
	sc.stop()
except:
	pass
sc = SparkContext(pyFiles=['/home/ubuntu/project_src/flaskapp/createLabeledPoint.py','/home/ubuntu/project_src/flaskapp/ClassSet.py','/home/ubuntu/project_src/flaskapp/FuncSet.py','/home/ubuntu/project_src/flaskapp/hello.py']).getOrCreate(conf=conf)
#testm = DecisionTreeModel.load(sc, "hdfs://ip-172-31-1-239:9000/home/ubuntu/project_src/typedt_model")
#testm = DecisionTreeModel.load(sc, "hdfs://ip-172-31-1-239:9000/home/ubuntu/project_src/probe_model")
testm = DecisionTreeModel.load(sc, "hdfs://ip-172-31-1-239:9000/home/ubuntu/project_src/probe_model")
testm_portsweep = DecisionTreeModel.load(sc, "hdfs://ip-172-31-1-239:9000/home/ubuntu/project_src/probe_portsweep_model")
@app.route('/')
def hello_world():
	return 'From python hello!'
@app.route('/index')
def index():
	return render_template("index.html")
@app.route('/train')
def trainodule():
	pass
@app.route('/getSpkTstCnt')
def runclass():
#	testm = DecisionTreeModel.load(sc, "hdfs://ip-172-31-1-239:9000/home/ubuntu/project_src/tree_model")
	test_data_file = "hdfs://ip-172-31-1-239:9000/user/ubuntu/corrected.gz"
	test_raw_data = sc.textFile(test_data_file)
	test_csv_data = test_raw_data.map(lambda x:x.split(","))
	test_data = test_csv_data.map(create_labeled_point)
	test1 = sc.parallelize(test_data.map(lambda p: p.features).take(1))
	predictions = testm.predict(test1)
	return str(predictions.take(1)[0])
#	return "dummy testing"
@app.route('/runclass', methods=['POST', 'GET'])
def spk_tst_cnt():
	jsondump = request.data.decode('utf-8')
	classpd = json.loads(jsondump, object_hook=logx_json_hook)
#	having classpd.traffic_pattern, we will call the spark module to classify and then retrieve the result, with the result, we can create the classrst object with connection_id + attacktype
#
	test_raw_data = sc.parallelize(list([classpd.traffic_pattern]))
	test_csv_data = test_raw_data.map(lambda x:x.split(","))
	test_data = test_csv_data.map(create_labeled_point)
	test1 = test_data.map(lambda p: p.features)
	predictions = testm_portsweep.predict(test1)
	if str(predictions.take(1)[0]) == "3.0":
		classrst = ClassRst(classpd.connection_id,"probe_portsweep")
		rtn = json.dumps(classrst, default=jdft)
		return rtn
	predictions = testm.predict(test1)
	classrst = ClassRst(classpd.connection_id,"")
	if str(predictions.take(1)[0]) == "0.0":
		classrst.attacktype = "normal"
	elif str(predictions.take(1)[0]) == "1.0":
		classrst.attacktype = "probe_ipsweep"
	elif str(predictions.take(1)[0]) == "2.0":
		classrst.attacktype = "probe_nmap"
	elif str(predictions.take(1)[0]) == "4.0":
		classrst.attacktype = "attack_unknowType"
	rtn = json.dumps(classrst, default=jdft)
	return rtn

if __name__ == '__main__':
	app.run('0.0.0.0')
