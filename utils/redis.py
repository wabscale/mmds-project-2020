from redis import Redis

from utils.checkpoints import get_checkpoint, set_checkpoint


def init_redis():
    if not get_checkpoint('redis-init'):
        r1 = Redis(port=6379)
        r1.flushall()
        r1.close()
        set_checkpoint('redis-init')
