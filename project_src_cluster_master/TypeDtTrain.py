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
data_file = "./kddcup.data_10_percent.gz"
#data_file = "./kddcup.data.gz"
raw_data = sc.textFile(data_file)
csv_data = raw_data.map(lambda x: x.split(","))
#csv_data = csv_data.filter(lambda x: x[41] in ['ipsweep.','portsweep.','nmap.','normal.'])
#csv_data = csv_data.filter(lambda x: x[41]  in ['ipsweep.','nmap.','portsweep.','normal.'])

test_data_file = "./corrected.gz"
test_raw_data = sc.textFile(test_data_file)
test_csv_data = test_raw_data.map(lambda x: x.split(","))
#test_csv_data = test_csv_data.filter(lambda x: x[41] in ['ipsweep.','portsweep.','nmap.','normal.'])
#test_csv_data = test_csv_data.filter(lambda x: x[41] in ['ipsweep.','nmap.','portsweep.','normal.'])
#test_csv_data = test_csv_data.filter(lambda x: x[41] in ['portsweep.'])

protocols = csv_data.map(lambda x: x[1]).distinct().collect()
services = csv_data.map(lambda x: x[2]).distinct().collect()
flags = csv_data.map(lambda x: x[3]).distinct().collect()


        #conf = SparkConf().setAppName("DtreeTraining")
        #sc = SparkContext(conf=conf)
def create_labeled_point(line_split):
	clean_line_split = line_split[0:41]
        # convert protocol to numeric categorical variable
	try:
		clean_line_split[1] = protocols.index(clean_line_split[1])
	except:
		clean_line_split[1] = len(protocols)
        # convert service to numeric categorical variable
	try:
		clean_line_split[2] = services.index(clean_line_split[2])
	except:
		clean_line_split[2] = len(services)
                # convert flag to numeric categorical variable
	try:
		clean_line_split[3] = flags.index(clean_line_split[3])
	except:
		clean_line_split[3] = len(flags)
        # convert label to binary label
#	attack = 1.0
	attack = 4.0
	if line_split[41]=='normal.':
		attack = 0.0
#	elif line_split[41]=='back.':
#	if line_split[41]=='back.':
#		attack = 1.0
#	elif line_split[41]=='land.':
#		attack = 2.0
#	elif line_split[41]=='neptune.':
#		attack = 3.0
#	elif line_split[41]=='pod.':
#		attack = 4.0
#	elif line_split[41]=='smurf.':
#		attack = 5.0
#	elif line_split[41]=='teardrop.':
#		attack = 6.0
	elif line_split[41]=='ipsweep.':
#	if line_split[41]=='ipsweep.':
		attack = 1.0
	elif line_split[41]=='nmap.':
		attack = 2.0
#	elif line_split[41]=='portsweep.':
#	if line_split[41]=='portsweep.':
#		attack = 3.0
#	elif line_split[41]=='normal.':
#		attack = 0.0
	else:
		attack = 4.0
#	elif line_split[41]=='imap.':
#		attack = 10.0
#	elif line_split[41]=='ftp_write.':
#		attack = 11.0
#	elif line_split[41]=='guess_passwd.':
#		attack = 12. line_split[41]=='spy.':
#		attack = 13.0
#	elif line_split[41]=='warezclient.':
#		attack = 14.0
#	elif line_split[41]=='warezmaster.':
#		attack = 15.0
#	elif line_split[41]=='multihop.':
#		attack = 16.0
#	elif line_split[41]=='phf.':
#		attack = 17.0

#	elif line_split[41]=='buffer_overflow.':
#		attack = 18.0
#	elif line_split[41]=='rootkit.':
#		attack = 19.0
#	elif line_split[41]=='perl.':
#		attack = 20.0
#	elif line_split[41]=='loadmodule.':
#		attack = 21.0
	return LabeledPoint(attack, array([float(x) for x in clean_line_split]))
training_data = csv_data.map(create_labeled_point)
test_data = test_csv_data.map(create_labeled_point)
t0 = time()
print ("Classifier training started at: ".format(round(t0,3)))
tree_model = DecisionTree.trainClassifier(training_data, numClasses=5, categoricalFeaturesInfo={1: len(protocols), 2: len(services), 3: len(flags)},impurity='gini', maxDepth=4, maxBins=100)
tree_model.save(sc, "/home/ubuntu/project_src/probe_model")
tt = time() - t0
print ("Classifier trained in {} seconds".format(round(tt,3)))
predictions = tree_model.predict(test_data.map(lambda p: p.features))
labels_and_preds = test_data.map(lambda p: p.label).zip(predictions)
t0 = time()
test_accuracy = labels_and_preds.filter(lambda vp: vp[0] == vp[1]).count() / float(test_data.count())
tt = time() - t0
#print(labels_and_preds.collect())
print(str(labels_and_preds.filter(lambda vp: vp[0]==vp[1]).count())+" "+str(labels_and_preds.filter(lambda vp: vp[0]!=vp[1]).count())+" "+str(float(test_data.count())))
print ("************************************************Prediction made in {} seconds. Test accuracy is {}".format(round(tt,3), round(test_accuracy,4)))
