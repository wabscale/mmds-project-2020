#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from easydict import EasyDict as edict
from cachetools import cached, LRUCache
from redis import Redis
import multiprocessing as mp
import pandas as pd
import pydgraph
import hashlib
import pickle
import time
import tqdm
import json
import gzip
import csv
import os


filepaths = list(
    map(lambda path: "./common-crawl/" + path, os.listdir("./common-crawl/"))
)

# # Preview data
# df = pd.read_csv(
#     filepaths[0],
#     nrows=10,
#     compression="gzip",
#     usecols=["asn_num", "domain", "ip", "country", "asn_org", "path"],
# )
# df.to_csv("sample.csv")
# df.head(10)


# In[ ]:


# Create a client.
def get_client():
    client_stub = pydgraph.DgraphClientStub("localhost:9080")
    return pydgraph.DgraphClient(client_stub), client_stub


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


# In[ ]:


class SpicyCache(object):
    def __init__(self, objname, localsize):
        super(SpicyCache, self).__init__()

        self.objname = objname
        self.local_cache = LRUCache(maxsize=localsize)
        self.redis = Redis("localhost")
        self.dgraph, self.stub = get_client()
        self.txn = self.dgraph.txn()

    def _key(self, key):
        return f"{self.objname}-{key}"

    def __setitem__(self, key, value):
        timeout = 3600 if self.objname == "asnnum" else "30"
        self.redis.setex(self._key(key), timeout, value)
        self.local_cache[self._key(key)] = value

    def __contains__(self, key):
        local_result = self.local_cache.get(self._key(key), None)
        if local_result is not None:
            return True

        redis_result = self.redis.get(self._key(key))
        if redis_result is not None:
            self[key] = redis_result.decode()
            return True

        query = (
            """query all($a: string) { all(func: eq(%s, $a)) { uid } }""" % self.objname
        )
        dgraph_result = self.txn.query(query, variables={"$a": str(key)})
        thing = json.loads(dgraph_result.json)
        if len(thing["all"]) > 0:
            self[key] = thing["all"][0]["uid"], 16
            return True

        return False

    def __getitem__(self, key):
        local_result = self.local_cache.get(self._key(key), None)
        if local_result is not None:
            return local_result

        redis_result = self.redis.get(self._key(key))
        if redis_result is not None:
            self[key] = int(redis_result.decode())
            return redis_result.decode()

        query = (
            """query all($a: string) { all(func: eq(%s, $a)) { uid } }""" % self.objname
        )
        dgraph_result = self.txn.query(query, variables={"$a": str(key)})
        thing = json.loads(dgraph_result.json)
        if len(thing["all"]) > 0:
            self[key] = thing["all"][0]["uid"]
            return thing["all"][0]["uid"]

        return None

    def close(self):
        self.stub.close()


# In[ ]:


# In[ ]:


def insert(thread_id, filename, batch_size=100, iterations=50000):
    print(f'starting job {thread_id}')
    client, stub = get_client()
    # Create caches
    domain_uids = SpicyCache("domain", 100000)
    asn_uids = SpicyCache("asnnum", 10000)
    country_uids = dict()

    # Create file read streamer
    reader = csv.DictReader(gzip.open(filename, "rt"))
    count = 0

    # Create a new transaction.
    txn = client.txn()
    try:
        n = 0
        for index, row in enumerate(reader):
            row = edict(row)

            # Create ASN if not exists
            if row.asn_num not in asn_uids:
                asn = {
                    "uid": "_:" + str(row.asn_num),
                    "dgraph.type": "ASN",
                    "asnnum": row.asn_num,
                    "org": row.asn_org,
                }
                response = txn.mutate(set_obj=asn)
                asn_uids[row.asn_num] = response.uids[str(row.asn_num)]

            # Create country if not exists
            if row.country not in country_uids:
                country = {
                    "uid": "_:" + row.country,
                    "dgraph.type": "Country",
                    "country_code": row.country,
                }
                response = txn.mutate(set_obj=country)
                country_uids[row.country] = response.uids[row.country]

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

    reader.close()

    return count


def yeet():
    client, stub = get_client()
    drop_all(client)
    set_schema(client)
    stub.close()

yeet()

print('Initializing pool')

Redis('localhost').flushall()

start_time = time.time()
with mp.Pool(20) as pool:
    counts = pool.starmap(insert, enumerate(filepaths))
    pool.close()
end_time = time.time()

print(f"Finished in {end_time-start_time}s with {sum(counts)/(end_time-start_time)}rows/s")

redis_push_thread.join()


# In[ ]:
