from pyspark import SparkConf, SparkContext
import urllib.request
import urllib
from pyspark.mllib.regression import LabeledPoint
from numpy import array
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from time import time
import createLabeledPoint
from createLabeledPoint import *
try:
	sc.stop()
except:
	pass
sc = SparkContext().getOrCreate(SparkConf())
testm = DecisionTreeModel.load(sc, "/home/ubuntu/project_src/probe_model")
testm_portsweep = DecisionTreeModel.load(sc, "/home/ubuntu/project_src/probe_portsweep_model")
test_data_file = "./corrected.gz"
test_raw_data = sc.textFile(test_data_file)

typename = test_raw_data.filter(lambda x: 'portsweep' in x)
cur=0
idx=0
count = typename.count()
for idx in range(count):
	typename_pd = typename.zipWithIndex().filter(lambda x: x[1]==idx).map(lambda x: x[0])
	test_csv_data = typename_pd.map(lambda x: x.split(","))
	test_data = test_csv_data.map(create_labeled_point)
	predictions = testm_portsweep.predict(test_data.map(lambda p: p.features))
	if str(predictions.take(1)[0]) == "3.0":
		print(typename_pd.collect())
		cur=cur+1
		if cur>10:
			break
	idx=idx+1

typename = test_raw_data.filter(lambda x: 'normal' in x)
cur=0
idx=0
count = typename.count()
testm = DecisionTreeModel.load(sc, "/home/ubuntu/project_src/probe_model")
for idx in range(count):
	typename_pd = typename.zipWithIndex().filter(lambda x: x[1]==idx).map(lambda x: x[0])
	test_csv_data = typename_pd.map(lambda x: x.split(","))
	test_data = test_csv_data.map(create_labeled_point)
	predictions = testm.predict(test_data.map(lambda p: p.features))
	if str(predictions.take(1)[0]) == "0.0":
		print(typename_pd.collect())
		cur=cur+1
		if cur>10:
			break
	idx=idx+1
typename = test_raw_data.filter(lambda x: 'ipsweep' in x)
cur=0
idx=0
count = typename.count()
for idx in range(count):
	typename_pd = typename.zipWithIndex().filter(lambda x: x[1]==idx).map(lambda x: x[0])
	test_csv_data = typename_pd.map(lambda x: x.split(","))
	test_data = test_csv_data.map(create_labeled_point)
	predictions = testm.predict(test_data.map(lambda p: p.features))
	if str(predictions.take(1)[0]) == "1.0":
		print(typename_pd.collect())
		cur=cur+1
		if cur>10:
			break
	idx=idx+1

typename = test_raw_data.filter(lambda x: 'nmap' in x)
cur=0
idx=0
count = typename.count()
for idx in range(count):
	typename_pd = typename.zipWithIndex().filter(lambda x: x[1]==idx).map(lambda x: x[0])
	test_csv_data = typename_pd.map(lambda x: x.split(","))
	test_data = test_csv_data.map(create_labeled_point)
	predictions = testm.predict(test_data.map(lambda p: p.features))
	if str(predictions.take(1)[0]) == "2.0":
		print(typename_pd.collect())
		cur=cur+1
		if cur>10:
			break
	idx=idx+1

typename = test_raw_data.filter(lambda x: 'smurf' in x)
cur=0
idx=0
count = typename.count()
for idx in range(count):
        typename_pd = typename.zipWithIndex().filter(lambda x: x[1]==idx).map(lambda x: x[0])
        test_csv_data = typename_pd.map(lambda x: x.split(","))
        test_data = test_csv_data.map(create_labeled_point)
        predictions = testm.predict(test_data.map(lambda p: p.features))
        if str(predictions.take(1)[0]) == "4.0":
                print(typename_pd.collect())
                cur=cur+1
                if cur>5:
                        break
        idx=idx+1

typename = test_raw_data.filter(lambda x: 'buffer_overflow' in x)
cur=0
idx=0
count = typename.count()
for idx in range(count):
	typename_pd = typename.zipWithIndex().filter(lambda x: x[1]==idx).map(lambda x: x[0])
	test_csv_data = typename_pd.map(lambda x: x.split(","))
	test_data = test_csv_data.map(create_labeled_point)
	predictions = testm.predict(test_data.map(lambda p: p.features))
	if str(predictions.take(1)[0]) == "4.0":
		print(typename_pd.collect())
		cur=cur+1
		if cur>5:
			break
	idx = idx +1

typename = test_raw_data.filter(lambda x: 'rootkit' in x)
cur=0
idx=0
count = typename.count()
for idx in range(count):
	typename_pd = typename.zipWithIndex().filter(lambda x: x[1]==idx).map(lambda x: x[0])
	test_csv_data = typename_pd.map(lambda x: x.split(","))
	test_data = test_csv_data.map(create_labeled_point)
	predictions = testm.predict(test_data.map(lambda p: p.features))
	if str(predictions.take(1)[0]) == "4.0":
		print(typename_pd.collect())
		cur=cur+1
		if cur>5:
			break
	idx = idx +1

typename = test_raw_data.filter(lambda x: 'perl' in x)
cur=0
idx=0
count = typename.count()
for idx in range(count):
	typename_pd = typename.zipWithIndex().filter(lambda x: x[1]==idx).map(lambda x: x[0])
	test_csv_data = typename_pd.map(lambda x: x.split(","))
	test_data = test_csv_data.map(create_labeled_point)
	predictions = testm.predict(test_data.map(lambda p: p.features))
	if str(predictions.take(1)[0]) == "4.0":
		print(typename_pd.collect())
		cur=cur+1
		if cur>5:
			break
	idx = idx + 1

typename = test_raw_data.filter(lambda x: 'loadmodule' in x)
cur=0
idx=0
count = typename.count()
for idx in range(count):
	typename_pd = typename.zipWithIndex().filter(lambda x: x[1]==idx).map(lambda x: x[0])
	test_csv_data = typename_pd.map(lambda x: x.split(","))
	test_data = test_csv_data.map(create_labeled_point)
	predictions = testm.predict(test_data.map(lambda p: p.features))
	if str(predictions.take(1)[0]) == "4.0":
		print(typename_pd.collect())
		cur=cur+1
		if cur>5:
			break
	idx = idx + 1
