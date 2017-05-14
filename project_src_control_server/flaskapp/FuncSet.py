from ClassSet import *
def jdft(object):
	return object.__dict__
def attacksolved_json_hook(json_format):
	return AttckSolvedRequest(json_format['connection_id'],json_format['action'],json_format['timestamp'],json_format['comment'])
def nodesolved_json_hook(json_format):
	return NodeSolvedRequest(json_format['from_node_id'],json_format['action'],json_format['timestamp'],json_format['comment'])
def historyrtv_json_hook(json_format):
	return HistoryRtvRequest(json_format['ftimestamp'],json_format['ttimestamp'])
def classrst_json_hook(json_format):
	return ClassRst(json_format['connection_id'],json_format['attacktype'])
def logitem_json_hook(json_format):
	return LogItem(json_format['connection_id'],json_format['current_CH_id'],json_format['from_node_id'],json_format['from_node_cluster_id'],json_format['traffic_pattern'],json_format['to_node_id'],json_format['to_node_cluster_id'],json_format['timestamp'])
