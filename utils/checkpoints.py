import multiprocessing as mp
import os
import pickle

lock: mp.Lock = None


def init_checkpoints():
    if not os.path.exists('checkpoint.pickle'):
        pickle.dump(set(), open('checkpoint.pickle', 'wb'))


def set_checkpoint(name):
    if lock is not None:
        lock.acquire()
    checkpoint: set = pickle.load(open('checkpoint.pickle', 'rb'))
    checkpoint.add(name)
    pickle.dump(checkpoint, open('checkpoint.pickle', 'wb'))
    if lock is not None:
        lock.release()
    print(f'reached checkpoint {name}')


def get_checkpoint(name):
    if lock is not None:
        lock.acquire()
    contains = name in pickle.load(open('checkpoint.pickle', 'rb'))
    if lock is not None:
        lock.release()
    return contains
