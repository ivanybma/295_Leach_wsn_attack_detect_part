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
raw_data = sc.textFile(data_file)
test_data_file = "./corrected.gz"
test_raw_data = sc.textFile(test_data_file)
csv_data = raw_data.map(lambda x: x.split(","))
test_csv_data = test_raw_data.map(lambda x: x.split(","))
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
        attack = 1.0
        if line_split[41]=='normal.':
                attack = 0.0
        return LabeledPoint(attack, array([float(x) for x in clean_line_split]))
training_data = csv_data.map(create_labeled_point)
test_data = test_csv_data.map(create_labeled_point)
t0 = time()
print ("Classifier training started at: ".format(round(t0,3)))
tree_model = DecisionTree.trainClassifier(training_data, numClasses=2, categoricalFeaturesInfo={1: len(protocols), 2: len(services), 3: len(flags)},impurity='gini', maxDepth=4, maxBins=100)
tree_model.save(sc, "/home/ubuntu/project_src/tree_model")
tt = time() - t0
print ("Classifier trained in {} seconds".format(round(tt,3)))
predictions = tree_model.predict(test_data.map(lambda p: p.features))
labels_and_preds = test_data.map(lambda p: p.label).zip(predictions)
t0 = time()
#test_accuracy = labels_and_preds.filter(lambda (v, p): v == p).count()
test_accuracy = labels_and_preds.filter(lambda vp: vp[0] == vp[1]).count() / float(test_data.count())
tt = time() - t0
print ("Prediction made in {} seconds. Test accuracy is {}".format(round(tt,3), round(test_accuracy,4)))
