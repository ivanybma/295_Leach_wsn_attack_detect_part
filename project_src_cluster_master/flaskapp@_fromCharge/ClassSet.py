class LogItem(object):
	def __init__(self, connection_id, current_CH_id, from_node_id, from_node_cluster_id, traffic_pattern, to_node_id, to_node_cluster_id, timestamp):
		self.connection_id = connection_id
		self.current_CH_id = current_CH_id
		self.from_node_id = from_node_id
		self.from_node_cluster_id = from_node_cluster_id
		self.traffic_pattern = traffic_pattern
		self.to_node_id = to_node_id
		self.to_node_cluster_id = to_node_cluster_id
		self.timestamp = timestamp
class LogItemX(object):
	def __init__(self, connection_id, traffic_pattern):
		self.connection_id = connection_id
		self.traffic_pattern = traffic_pattern
class ClassRst(object):
	def __init__(self, connection_id, attacktype):
		self.connection_id = connection_id
		self.attacktype = attacktype
class RefshItem(object):
	def __init__(self, connection_id, current_CH_id, from_node_id, from_node_cluster_id, attacktype, to_node_id, to_node_cluster_id, timestamp):
		self.connection_id = connection_id
		self.current_CH_id = current_CH_id
		self.from_node_id = from_node_id
		self.from_node_cluster_id = from_node_cluster_id
		self.attacktype = attacktype
		self.to_node_id = to_node_id
		self.to_node_cluster_id = to_node_cluster_id
		self.timestamp = timestamp
class RefshLst(object):
	rstlst = list()
	def __init__(self, rstlst):
		self.rstlst = rstlst
class AttckSolvedRequest(object):
	def __init__(connection_id, from_node_id, from_node_cluster_id, to_node_id, to_node_cluster_id, timestamp, comment):
		self.connection_id = connection_id
		self.from_node_id = from_node_id
		self.from_node_cluster_id = from_node_cluster_id
		self.to_node_id = to_node_id
		self.to_node_cluster_id = to_node_cluster_id
		self.timestamp = timestamp
		self.comment = comment
class ReturnCode(object):
	def __init__(self, responsed_code):
		self.responsed_code = responsed_code
