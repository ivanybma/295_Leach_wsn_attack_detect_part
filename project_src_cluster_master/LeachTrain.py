from pyspark import SparkConf, SparkContext
import urllib.request
import urllib
from pyspark.mllib.regression import LabeledPoint
from numpy import array
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from time import time

#conf = SparkConf().setAppName("DtreeTraining")
#sc = SparkContext(conf=conf)
#       conf = SparkConf()
#       sc = SparkContext(conf=conf)
sc = SparkContext.getOrCreate()
data_file = "./4attck_normal_formatted.txt"
#data_file = "./blackhole_scheduling_attack_normal_formatted.txt"
#data_file = "./kddcup.data.gz"
raw_data = sc.textFile(data_file)
csv_data = raw_data.map(lambda x: x.split("\t"))
#csv_data = csv_data.filter(lambda x: x[41]  in ['ipsweep.','nmap.','portsweep.','normal.'])

test_data_file = "./4attck_normal_test_formatted.txt"
test_raw_data = sc.textFile(test_data_file)
test_csv_data = test_raw_data.map(lambda x: x.split("\t"))
#test_csv_data = test_csv_data.filter(lambda x: x[41] in ['ipsweep.','portsweep.','nmap.','normal.'])
#test_csv_data = test_csv_data.filter(lambda x: x[41] in ['ipsweep.','nmap.','portsweep.','normal.'])
#test_csv_data = test_csv_data.filter(lambda x: x[41] in ['portsweep.'])

#protocols = csv_data.map(lambda x: x[1]).distinct().collect()
#services = csv_data.map(lambda x: x[2]).distinct().collect()
#flags = csv_data.map(lambda x: x[3]).distinct().collect()


        #conf = SparkConf().setAppName("DtreeTraining")
        #sc = SparkContext(conf=conf)

def create_labeled_point(line_split):
	clean_line_split = line_split[0:18]
	attack = 5.0
#	clean_line_split[4]=0
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
training_data = csv_data.map(create_labeled_point)
test_data = test_csv_data.map(create_labeled_point)
t0 = time()
print ("Classifier training started at: ".format(round(t0,3)))
tree_model = DecisionTree.trainClassifier(training_data, numClasses=6, categoricalFeaturesInfo={},impurity='gini', maxDepth=4, maxBins=100)
tree_model.save(sc, "/home/ubuntu/project_src/leach_model")
tt = time() - t0
print ("Classifier trained in {} seconds".format(round(tt,4)))
predictions = tree_model.predict(test_data.map(lambda p: p.features))
labels_and_preds = test_data.map(lambda p: p.label).zip(predictions)
t0 = time()
test_accuracy = labels_and_preds.filter(lambda vp: vp[0] == vp[1]).count() / float(test_data.count())
tt = time() - t0
print(labels_and_preds.collect())
print("correct count: "+str(labels_and_preds.filter(lambda vp: vp[0]==vp[1]).count())+" incorrect count: "+str(labels_and_preds.filter(lambda vp: vp[0]!=vp[1]).count())+" total count: "+str(float(test_data.count())))

print ("************************************************Prediction made in {} seconds. Test accuracy is {}".format(round(tt,3), round(test_accuracy,4)))
