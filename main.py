# pip install redis tabulate
import redis
from redis.cluster import ClusterNode
from tabulate import tabulate
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
    print(f'Inserting {ENTRY_COUNT} entries into list')
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
    if insert_count != ENTRY_COUNT:
        raise ValueError('Incorrect number of entries. Expected: ' + str(ENTRY_COUNT) + ' actual: ' + str(insert_count))
    memory_usage = redis.execute_command("MEMORY USAGE", key)

    # Clean up
    redis.delete(key)

    return memory_usage


def benchmark_set(redis):
    """
    Run a benchmark for the SET data structure
    :param redis: Redis connection
    """

    # Insert set of entries against a single key
    print(f'Inserting {ENTRY_COUNT} entries into set')
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
    if insert_count != ENTRY_COUNT:
        raise ValueError('Incorrect number of entries. Expected: ' + str(ENTRY_COUNT) + ' actual: ' + str(insert_count))
    memory_usage = redis.execute_command("MEMORY USAGE", key)

    # Clean up
    redis.delete(key)

    return memory_usage


def benchmark_sorted_set(redis):
    """
    Run a benchmark for the SORTED SET data structure
    :param redis: Redis connection
    """

    # Insert sorted set of entries against a single key
    print(f'Inserting {ENTRY_COUNT} entries into sorted set')
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
    if insert_count != ENTRY_COUNT:
        raise ValueError('Incorrect number of entries. Expected: ' + str(ENTRY_COUNT) + ' actual: ' + str(insert_count))
    memory_usage = redis.execute_command("MEMORY USAGE", key)

    # Clean up
    redis.delete(key)

    return memory_usage


def benchmark_hash(redis):
    """
    Run a benchmark for the HASH data structure
    :param redis: Redis connection
    """

    # Insert hash of entries against a single key
    print(f'Inserting {ENTRY_COUNT} entries into hash')
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
    if insert_count != ENTRY_COUNT:
        raise ValueError('Incorrect number of entries. Expected: ' + str(ENTRY_COUNT) + ' actual: ' + str(insert_count))
    memory_usage = redis.execute_command("MEMORY USAGE", key)

    # Clean up
    redis.delete(key)

    return memory_usage


def print_results(results):
    table = []
    for struct, memory_usage in results.items():
        bytes_per_entry = memory_usage / ENTRY_COUNT
        overhead = bytes_per_entry - VALUE_SIZE
        table.append([struct, ENTRY_COUNT, memory_usage, bytes_per_entry, overhead])
    print(tabulate(table,
                   headers=['Structure', 'Count', 'Total memory (bytes)', 'Memory per entry (bytes)', 'Overhead per entry (bytes)'],
                   floatfmt='.2f'))


if __name__ == '__main__':
    redis = connect_cluster()
    results = {
        'list': benchmark_list(redis),
        'set': benchmark_set(redis),
        'sorted set (zset)': benchmark_sorted_set(redis),
        'hash': benchmark_hash(redis)
    }
    print_results(results)
