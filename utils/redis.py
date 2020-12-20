from redis import Redis
from redisbloom.client import Client as RedisBloom

from utils.checkpoints import get_checkpoint, set_checkpoint


def init_redis():
    if not get_checkpoint('redis-init'):
        r1 = Redis(port=6379)
        r2 = Redis(port=6378)
        r1.flushall()
        r2.flushall()
        r1.close()
        r2.close()
        set_checkpoint('redis-init')

    if not get_checkpoint('bloom-init'):
        bloom = RedisBloom(port=6378)
        bloom.bfCreate("domain", 1.0e-6, 1000000)
        bloom.close()
        set_checkpoint('bloom-init')