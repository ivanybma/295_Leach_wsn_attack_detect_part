from pyspark import SparkConf, SparkContext
import urllib.request
import urllib
from pyspark.mllib.regression import LabeledPoint
from numpy import array
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from time import time
conf = SparkConf()
conf.setAppName("Classification")
try:
	sc.stop()
except:
	pass
#sc.stop()
sc = SparkContext(pyFiles=['/home/ubuntu/project_src/flaskapp/ClassSet.py','/home/ubuntu/project_src/flaskapp/FuncSet.py','/home/ubuntu/project_src/flaskapp/hello.py']).getOrCreate(conf=conf)
def create_labeled_point(line_split):
	clean_line_split = line_split[0:18]
	attack = 5.0
	if line_split[18]=='normal':
		attack = 0.0
	elif line_split[18]=='blackhole':
		attack = 1.0
	elif line_split[18]=='scheduling':
		attack = 2.0
	elif line_split[18]=='sinkhole':
		attack = 3.0
	elif line_split[18]=='sybil':
		attack = 4.0
	else:
		attack = 5.0
	return LabeledPoint(attack, array([float(x) for x in clean_line_split]))
