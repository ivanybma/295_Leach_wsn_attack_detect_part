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
sc = SparkContext(pyFiles=['/home/ubuntu/project_src/flaskapp/ClassSet.py','/home/ubuntu/project_src/flaskapp/FuncSet.py','/home/ubuntu/project_src/flaskapp/hello.py']).getOrCreate(conf=conf)
#sc = SparkContext.getOrCreate(conf=conf)
data_file = "hdfs://ip-172-31-1-239:9000/user/ubuntu/kddcup.data_10_percent.gz"
raw_data = sc.textFile(data_file)
csv_data = raw_data.map(lambda x: x.split(","))
protocols = csv_data.map(lambda x: x[1]).distinct().collect()
services = csv_data.map(lambda x: x[2]).distinct().collect()
flags = csv_data.map(lambda x: x[3]).distinct().collect()
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
	attack = 4.0
	if line_split[41]=='normal.':
		attack = 0.0
#       elif line_split[41]=='back.':
#       if line_split[41]=='back.':
#               attack = 1.0
#       elif line_split[41]=='land.':
#               attack = 2.0
#       elif line_split[41]=='neptune.':
#               attack = 3.0
#       elif line_split[41]=='pod.':
#               attack = 4.0
#       elif line_split[41]=='smurf.':
#               attack = 5.0
#       elif line_split[41]=='teardrop.':
#               attack = 6.0

	elif line_split[41]=='ipsweep.':
#       if line_split[41]=='ipsweep.':
		attack = 1.0
	elif line_split[41]=='nmap.':
		attack = 2.0
	elif line_split[41]=='portsweep.':
		attack = 3.0
	else:
		attack = 4.0
#       elif line_split[41]=='imap.':
#               attack = 10.0
#       elif line_split[41]=='ftp_write.':
#               attack = 11.0
#       elif line_split[41]=='guess_passwd.':
#               attack = 12.0
#       elif line_split[41]=='spy.':
#               attack = 13.0
#       elif line_split[41]=='warezclient.':
#               attack = 14.0
#       elif line_split[41]=='warezmaster.':
#               attack = 15.0
#       elif line_split[41]=='multihop.':
#               attack = 16.0
#       elif line_split[41]=='phf.':
#               attack = 17.0

#       elif line_split[41]=='buffer_overflow.':
#               attack = 18.0
#       elif line_split[41]=='rootkit.':
#               attack = 19.0
#       elif line_split[41]=='perl.':
#               attack = 20.0
#       elif line_split[41]=='loadmodule.':
#               attack = 21.0

	return LabeledPoint(attack, array([float(x) for x in clean_line_split]))
