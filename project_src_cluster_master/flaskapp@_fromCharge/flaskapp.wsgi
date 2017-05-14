import sys
import os
sys.path.insert(0, '/var/www/html/flaskapp')
sys.path.append('/usr/local/spark/python/lib/py4j-0.10.3-src.zip')
sys.path.append('/usr/local/spark/python')
os.environ['SPARK_HOME']='/usr/local/spark'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
from hello import app as application
