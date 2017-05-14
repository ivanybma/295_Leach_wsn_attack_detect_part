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
import sys
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from numpy import array
app = Flask(__name__)
conf = SparkConf()
conf.setAppName("Classification")
try:
	sc.stop()
except:
	pass
sc = SparkContext(pyFiles=['/home/ubuntu/project_src/flaskapp/createLabeledPoint.py','/home/ubuntu/project_src/flaskapp/ClassSet.py','/home/ubuntu/project_src/flaskapp/FuncSet.py','/home/ubuntu/project_src/flaskapp/hello.py']).getOrCreate(conf=conf)
testm = DecisionTreeModel.load(sc, "hdfs://ip-172-31-12-132:9000/home/ubuntu/project_src/leach_model")
testm_NB = NaiveBayesModel.load(sc, "hdfs://ip-172-31-12-132:9000/home/ubuntu/project_src/leach_model_NB")
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
def spk_tst_cnt():
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
def runclass():
	jsondump = request.data.decode('utf-8')
	sys.stdout = open('/home/ubuntu/project_src/flaskapp/output.logs', 'w')
	classpd = json.loads(jsondump, object_hook=logx_json_hook)
#	having classpd.traffic_pattern, we will call the spark module to classify and then retrieve the result, with the result, we can create the classrst object with connection_id + attacktype
#
	test_raw_data = sc.parallelize(list([classpd.traffic_pattern]))
	print(str(test_raw_data.take(1)))
	print("nxt")
	test_csv_data = test_raw_data.map(lambda x:x.split("\t"))
	print(test_csv_data.take(1))
#	sys.stdout = sys.__stdout__
	test_data = test_csv_data.map(create_labeled_point)
	test1 = test_data.map(lambda p: p.features)
	predictions = testm.predict(test1)
	classrst = ClassRst(classpd.connection_id,"")
	if str(predictions.take(1)[0]) == "0.0":
		classrst.attacktype = "normal"
	elif str(predictions.take(1)[0]) == "1.0":
		classrst.attacktype = "blackhole_attack"
	elif str(predictions.take(1)[0]) == "2.0":
		classrst.attacktype = "scheduling_attack"
	elif str(predictions.take(1)[0]) == "3.0":
		classrst.attacktype = "sinkhole_attack"
	elif str(predictions.take(1)[0]) == "4.0":
		classrst.attacktype = "sybil_attack"
	elif str(predictions.take(1)[0]) == "5.0":
		classrst.attacktype = "attack_unknowType"
	rtn = json.dumps(classrst, default=jdft)
	print(rtn)
	sys.stdout = sys.__stdout__
	return rtn
@app.route('/runclass_batch', methods=['POST', 'GET'])
def runclass_batch():
	jsondump = request.data.decode('utf-8')
	sys.stdout = open('/home/ubuntu/project_src/flaskapp/runclass_batch_output.logs', 'w')
#	print("test")
	print(jsondump)
#	classpd = json.loads(jsondump, object_hook=logx_json_hook)
#	having classpd.traffic_pattern, we will call the spark module to classify and then retrieve the result, with the result, we can create the classrst object with connection_id + attacktype
#
#	sys.stdout = sys.__stdout__
	classpd_lst = list()
	data = json.loads(jsondump)
	traffic_lst = list()
	connectionid_lst = list()
	for x in data["pdlst"]:
		classpd_lst.append(ClassRst_NB(x["connection_id"],x["timestamp"],""))
		connectionid_lst.append(x["connection_id"])
		traffic_lst.append(x["traffic_pattern"])
	test_raw_data = sc.parallelize(traffic_lst)
	test_csv_data = test_raw_data.map(lambda x:x.split("\t"))
#	print(test_csv_data.take(3))
#	sys.stdout = sys.__stdout__
	test_data = test_csv_data.map(create_labeled_point)
	test1 = test_data.map(lambda p: p.features)
	predictions = testm_NB.predict(test1)
#	sys.stdout = sys.__stdout__
	predict_rst_lst = predictions.collect()
	index = 0
	for rst in predict_rst_lst:
		rst = str(rst)
		if rst == "0.0":		
			classpd_lst[index].attacktype = "normal"
		elif rst == "1.0":
			print(str(index))
			classpd_lst[index].attacktype = "blackhole_attack"
		elif rst == "2.0":
			classpd_lst[index].attacktype = "scheduling_attack"
		elif rst == "3.0":
			classpd_lst[index].attacktype = "sinkhole_attack"
		elif rst == "4.0":
			classpd_lst[index].attacktype = "sybil_attack"
		elif rst == "5.0":
			classpd_lst[index].attacktype = "attack_unknowType"
		print(str(rst) + " *** " +classpd_lst[index].connection_id+" *** "+classpd_lst[index].attacktype)
		index = index + 1
	sys.stdout = sys.__stdout__
	rtnlst = RefshLst(classpd_lst)
	return json.dumps(rtnlst,default=jdft)

if __name__ == '__main__':
	app.run('0.0.0.0')
