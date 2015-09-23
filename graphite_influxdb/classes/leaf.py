from graphite_api.node import LeafNode

class InfluxDBLeafNode(LeafNode):
    """Tell Graphite-Api that our leaf node supports multi-fetch"""
    __fetch_multi__ = 'influxdb'
