from ClassSet import *
def jdft(object):
	return object.__dict__
def logitem_json_hook(json_format):
	return LogItem(json_format['connection_id'],json_format['current_CH_id'],json_format['from_node_id'],json_format['from_node_cluster_id'],json_format['traffic_pattern'],json_format['to_node_id'],json_format['to_node_cluster_id'],json_format['timestamp'])
def logx_json_hook(json_format):
	return LogItemX(json_format['connection_id'], json_format['traffic_pattern'])
