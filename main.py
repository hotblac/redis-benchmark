# pip install redis
import redis
from redis.cluster import ClusterNode

# Redis nodes for bootstrapping.
CLUSTER_NODES = [ClusterNode("localhost", 6371),
                 ClusterNode("localhost", 6372),
                 ClusterNode("localhost", 6373)]


def connect_cluster():
    return redis.RedisCluster(startup_nodes=CLUSTER_NODES)


if __name__ == '__main__':
    redis = connect_cluster()

