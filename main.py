# pip install redis
import redis
from redis.cluster import ClusterNode
import os

ENTRY_COUNT = 1000000
CHUNK = 1000
VALUE_SIZE = 16
# Redis nodes for bootstrapping.
CLUSTER_NODES = [ClusterNode("localhost", 6371),
                 ClusterNode("localhost", 6372),
                 ClusterNode("localhost", 6373)]


def connect_cluster():
    """
    Obtain a connection to the Redis cluster
    :return: RedisCluster
    """
    return redis.RedisCluster(startup_nodes=CLUSTER_NODES)


def benchmark_list(redis):
    """
    Run a benchmark for the LIST data structure
    :param redis: Redis connection
    """

    # Insert list of entries against a single key
    key = "benchmark:list"
    j = 0
    values = []
    for _ in range(0, ENTRY_COUNT):
        j += 1
        value = os.urandom(VALUE_SIZE)
        values.append(value)
        if j <= CHUNK:
            redis.rpush(key, *values)
            values.clear()
            j = 0
    if values:
        redis.rpush(key, *values)

    # Get stats
    insert_count = redis.llen(key)
    memory_usage = redis.execute_command("MEMORY USAGE", key)
    bytes_per_entry = memory_usage / insert_count
    overhead = bytes_per_entry - VALUE_SIZE
    print(f'LIST: Inserted count: {insert_count}. Memory usage: {memory_usage}. Bytes per entry: {bytes_per_entry}. Overhead per entry: {overhead}')

    # Clean up
    redis.delete(key)


def benchmark_set(redis):
    """
    Run a benchmark for the SET data structure
    :param redis: Redis connection
    """

    # Insert set of entries against a single key
    key = "benchmark:set"
    j = 0
    values = []
    for _ in range(0, ENTRY_COUNT):
        j += 1
        value = os.urandom(VALUE_SIZE)
        values.append(value)
        if j <= CHUNK:
            redis.sadd(key, *values)
            values.clear()
            j = 0
    if values:
        redis.sadd(key, *values)

    # Get stats
    insert_count = redis.scard(key)
    memory_usage = redis.execute_command("MEMORY USAGE", key)
    bytes_per_entry = memory_usage / insert_count
    overhead = bytes_per_entry - VALUE_SIZE
    print(f'SET: Inserted count: {insert_count}. Memory usage: {memory_usage}. Bytes per entry: {bytes_per_entry}. Overhead per entry: {overhead}')

    # Clean up
    redis.delete(key)


def benchmark_sorted_set(redis):
    """
    Run a benchmark for the SORTED SET data structure
    :param redis: Redis connection
    """

    # Insert sorted set of entries against a single key
    key = "benchmark:zset"
    j = 0
    values = {}
    for _ in range(0, ENTRY_COUNT):
        j += 1
        value = os.urandom(VALUE_SIZE)
        values[value] = 0
        if j <= CHUNK:
            redis.zadd(key, values)
            values.clear()
            j = 0
    if values:
        redis.zadd(key, values)

    # Get stats
    insert_count = redis.zcard(key)
    memory_usage = redis.execute_command("MEMORY USAGE", key)
    bytes_per_entry = memory_usage / insert_count
    overhead = bytes_per_entry - VALUE_SIZE
    print(f'SORTED SET: Inserted count: {insert_count}. Memory usage: {memory_usage}. Bytes per entry: {bytes_per_entry}. Overhead per entry: {overhead}')

    # Clean up
    redis.delete(key)


def benchmark_hash(redis):
    """
    Run a benchmark for the HASH data structure
    :param redis: Redis connection
    """

    # Insert hash of entries against a single key
    key = "benchmark:hash"
    j = 0
    values = {}
    for _ in range(0, ENTRY_COUNT):
        j += 1
        value = os.urandom(VALUE_SIZE)
        values[value] = 0
        if j <= CHUNK:
            redis.hset(key, mapping=values)
            values.clear()
            j = 0
    if values:
        redis.hset(key, mapping=values)

    # Get stats
    insert_count = redis.hlen(key)
    memory_usage = redis.execute_command("MEMORY USAGE", key)
    bytes_per_entry = memory_usage / insert_count
    overhead = bytes_per_entry - VALUE_SIZE
    print(f'HASH: Inserted count: {insert_count}. Memory usage: {memory_usage}. Bytes per entry: {bytes_per_entry}. Overhead per entry: {overhead}')

    # Clean up
    redis.delete(key)


if __name__ == '__main__':
    redis = connect_cluster()
    benchmark_list(redis)
    benchmark_set(redis)
    benchmark_sorted_set(redis)
    benchmark_hash(redis)

