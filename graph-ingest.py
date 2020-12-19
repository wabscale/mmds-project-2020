#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from easydict import EasyDict as edict
from cachetools import cached, LRUCache
from redis import Redis
from redisbloom.client import Client as RedisBloom
import multiprocessing as mp
import pandas as pd
import pydgraph
import hashlib
import random
import pickle
import time
import tqdm
import json
import gzip
import csv
import os


filepaths = list(
    map(
        lambda path: "./common-crawl/" + path,
        filter(lambda x: x.endswith(".csv.gz"), os.listdir("./common-crawl/")),
    )
)


# Create a client.
def get_client():
    class Stubs:
        def __init__(self, n):
            i = random.randint(0, n)
            self.stubs = [
                pydgraph.DgraphClientStub(f"localhost:{9080+(i%n)}"),
                pydgraph.DgraphClientStub(f"localhost:{9080+((i+1)%n)}"),
            ]

        def close(self):
            for stub in self.stubs:
                stub.close()

    client_stub = Stubs(6)
    return pydgraph.DgraphClient(*client_stub.stubs), client_stub


# Drop All - discard all data and start from a clean slate.
def drop_all(client):
    print("Dropping all")
    return client.alter(pydgraph.Operation(drop_all=True))


# Set schema.
def set_schema(client):
    print("Setting Schema")
    schema = """
    asnnum: int @index(int) .
    org: string .
    domains: [uid] @reverse .
    country: [uid] @reverse .
    
    type ASN {
        asnnum
        org
        domains
    }
    
    domain: string @index(term,exact) .
    tld: string .
    ip: string .
    documents: [uid] @reverse .
    
    type Domain {
        domain
        tld
        ip
        documents
        country
    }
    
    path: string @index(term) .
    type Document {
        path
    }
    
    country_code: string @index(exact) .
    type Country {
        country_code
        domains
    }
    
    """
    return client.alter(pydgraph.Operation(schema=schema))


def init_checkpoints():
    if not os.path.exists('checkpoint.pickle'):
        pickle.dump(set(), open('checkpoint.pickle', 'wb'))


def set_checkpoint(name):
    checkpoint: set = pickle.load(open('checkpoint.pickle', 'rb'))
    checkpoint.add(name)
    pickle.dump(checkpoint, open('checkpoint.pickle', 'wb'))
    print(f'reached checkpoint {name}')


def get_checkpoint(name):
    return name in pickle.load(open('checkpoint.pickle', 'rb'))


class SimpleCache(object):
    def __init__(self, objname, localsize):
        super(SimpleCache, self).__init__()

        self.objname = objname
        self.local_cache = LRUCache(maxsize=localsize)
        self.redis = Redis("localhost")
        self.set_timeout = False

    def _key(self, key):
        return f"{self.objname}-{key}"

    def __setitem__(self, key, value):
        self.local_cache[self._key(key)] = value

        if self.set_timeout:
            timeout = 300
            self.redis.setex(self._key(key), timeout, value)
        else:
            self.redis.set(self._key(key), value)

    def __contains__(self, key):
        local_result = self.local_cache.get(self._key(key), None)
        if local_result is not None:
            return True

        redis_result = self.redis.get(self._key(key))
        if redis_result is not None:
            self[key] = redis_result.decode()
            return True

        return False

    def __getitem__(self, key):
        local_result = self.local_cache.get(self._key(key), None)
        if local_result is not None:
            return local_result

        redis_result = self.redis.get(self._key(key))
        if redis_result is not None:
            self[key] = redis_result.decode()
            return redis_result.decode()

        return False

    def close(self):
        self.redis.close()


class SpicyCache(SimpleCache):
    def __init__(self, objname, localsize):
        super(SpicyCache, self).__init__(objname, localsize)

        self.set_timeout = True
        self.bloom = RedisBloom(port=6378)
        self.dgraph, self.stub = get_client()
        self.txn = self.dgraph.txn()

    def __contains__(self, key):
        if super(SpicyCache, self).__contains__(key):
            return True

        if self.bloom.bfExists(self.objname, self._key(key)) == 0:
            query = (
                """query all($a: string) { all(func: eq(%s, $a)) { uid } }"""
                % self.objname
            )
            dgraph_result = self.txn.query(query, variables={"$a": str(key)})
            thing = json.loads(dgraph_result.json)
            if len(thing["all"]) > 0:
                self[key] = thing["all"][0]["uid"]
                return True

        return False

    def __getitem__(self, key):
        item = super(SpicyCache, self).__getitem__(key)
        if item is not None:
            return item

        if self.bloom.bfExists(self.objname, self._key(key)) == 0:
            query = (
                """query all($a: string) { all(func: eq(%s, $a)) { uid } }"""
                % self.objname
            )
            dgraph_result = self.txn.query(query, variables={"$a": str(key)})
            thing = json.loads(dgraph_result.json)
            if len(thing["all"]) > 0:
                self[key] = thing["all"][0]["uid"]
                return thing["all"][0]["uid"]

        return None

    def close(self):
        super(SpicyCache, self).close()
        self.stub.close()


def insert(thread_id, filename, batch_size=100, iterations=50000):
    if get_checkpoint(filename):
        return 0

    print(f"starting job {thread_id}")
    client, stub = get_client()

    # Create caches
    domain_uids = SpicyCache("domain", 1000000)
    asn_uids = SimpleCache("asnnum", 10000)
    country_uids = SimpleCache('country', 300)

    # Create file read streamer
    file = gzip.open(filename, "rt")
    reader = csv.DictReader(file)
    count = 0

    # Create a new transaction.
    txn = client.txn()
    try:
        n = 0
        for index, row in enumerate(reader):
            row = edict(row)

            # Create domain if not exists
            if row.domain not in domain_uids:
                domain = {
                    "uid": "_:" + row.domain,
                    "dgraph.type": "Domain",
                    "domain": row.domain,
                    "tld": row.domain.split(".")[-1],
                    "ip": row.ip,
                }
                response = txn.mutate(set_obj=domain)
                domain_uids[row.domain] = response.uids[row.domain]

                # Draw edge from asn to domain
                edge = {
                    "uid": asn_uids[row.asn_num],
                    "domains": [
                        {"uid": domain_uids[row.domain]},
                    ],
                }
                response = txn.mutate(set_obj=edge)

                # Draw edge from country to domain
                edge = {
                    "uid": country_uids[row.country],
                    "domains": [{"uid": domain_uids[row.domain]}],
                }
                response = txn.mutate(set_obj=edge)

                # Draw edge from domain to country
                edge = {
                    "uid": domain_uids[row.domain],
                    "country": [{"uid": country_uids[row.country]}],
                }
                response = txn.mutate(set_obj=edge)

            # Create document
            doc_uid = hashlib.md5(row.path.encode()).hexdigest()
            document = {
                "uid": "_:" + doc_uid,
                "dgraph.type": "Document",
                "path": row.path,
            }
            response = txn.mutate(set_obj=document)

            # Draw edge to domain
            edge = {
                "uid": domain_uids[row.domain],
                "documents": [{"uid": response.uids[doc_uid]}],
            }
            response = txn.mutate(set_obj=edge)

            n += 1
            if n == batch_size:
                n = 0
                txn.commit()

                # If max iterations exceded, return
                if iterations is not None and index > iterations:
                    return

                # Help garbage collection
                del txn
                txn = client.txn()

            count += 1

        # Commit transaction.
        txn.commit()

    finally:
        # Clean up. Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()
        stub.close()
        domain_uids.close()
        asn_uids.close()
        file.close()

    set_checkpoint(filename)

    return count


def ingest_country_asn():
    client, stub = get_client()

    asntbl = pd.read_csv('ip2asn-v4.tsv.gz', compression='gzip', sep='\t', header=None)
    asntbl.columns = ['start', 'end', 'asn number', 'country', 'organization']
    asntbl.drop(['start', 'end'], inplace=True, axis=1)
    asntbl.drop_duplicates(subset=['asn number'], inplace=True)
    asntbl = asntbl[asntbl['asn number'] != 0]
    asntbl.set_index('asn number', inplace=True)
    asntbl.drop_duplicates(inplace=True)

    country_uids = SimpleCache('country', 300)
    asn_uids = SimpleCache('asnnum', 10000)

    txn = client.txn()

    if not get_checkpoint('asns'):
        for index, asnrow in tqdm.tqdm(enumerate(asntbl.itertuples()), desc='Ingesting ASNs', total=len(asntbl)):
            asn = {
                "uid": "_:" + str(asnrow.Index),
                "dgraph.type": "ASN",
                "asnnum": asnrow.Index,
                "org": asnrow.organization,
            }
            response = txn.mutate(set_obj=asn)
            asn_uids[asnrow.Index] = response.uids[str(asnrow.Index)]
            if index % 500 == 0:
                txn.commit()
                del txn
                txn = client.txn()
        txn.commit()
        del txn
        txn = client.txn()
        set_checkpoint('asns')

    if not get_checkpoint('countries'):
        for country_code in tqdm.tqdm(asntbl['country'].dropna().unique(), desc='Ingesting countries'):
            country = {
                "uid": "_:" + country_code,
                "dgraph.type": "Country",
                "country_code": country_code,
            }
            response = txn.mutate(set_obj=country)
            country_uids[country_code] = response.uids[country_code]
        txn.commit()
        set_checkpoint('countries')

    stub.close()
    country_uids.close()
    asn_uids.close()


def initialize_dgraph():
    if not get_checkpoint('init'):
        client, stub = get_client()
        drop_all(client)
        set_schema(client)
        stub.close()
        set_checkpoint('init')


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


init_checkpoints()
initialize_dgraph()
ingest_country_asn()


start_time = time.time()

# for args in enumerate(filepaths):
#     insert(*args)

with mp.Pool(16) as pool:
   print("Initializing pool")
   counts = pool.starmap(insert, enumerate(filepaths))
   pool.close()

end_time = time.time()

print(
    f"Finished in {end_time-start_time}s with {sum(counts)/(end_time-start_time)}rows/s"
)


# In[ ]:
