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
class LogItemX_NB(object):
	def __init__(self, connection_id, timestamp, traffic_pattern):
		self.connection_id = connection_id
		self.timestamp = timestamp
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
class BlkLst(object):
	pdlst = list()
	def __init__(self, pdlst):
                self.pdlst = pdlst
class BlkIdDetail(object):
	uid = ""
	detail = ""
	def __init__(self,uid,detail):
		self.uid = uid
		self.detail = detail
class AttckSolvedRequest(object):
	def __init__(self, connection_id, action, timestamp, comment):
		self.connection_id = connection_id
		self.action = action
		self.updatetime = timestamp
		self.comment = comment
class NodeSolvedRequest(object):
	def __init__(self, from_node_id, action, timestamp, comment):
		self.from_node_id = from_node_id
		self.action = action
		self.updatetime = timestamp
		self.comment = comment
class HistoryRtvRequest(object):
	def __init__(self, ftimestamp, ttimestamp):
		self.ftimestamp = ftimestamp
		self.ttimestamp = ttimestamp
class ReturnCode(object):
	def __init__(self, responsed_code):
		self.responsed_code = responsed_code
