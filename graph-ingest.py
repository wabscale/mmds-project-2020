#!/usr/bin/env python3


import csv
import gzip
import hashlib
import multiprocessing as mp
import os
import time
import traceback

import tqdm
from easydict import EasyDict as edict

from utils.asn import get_asn_table
from utils.cache import SimpleCache, SpicyCache
from utils.checkpoints import init_checkpoints, set_checkpoint, get_checkpoint
from utils.dgraph import get_client, initialize_dgraph

filepaths = list(
    map(
        lambda path: "./common-crawl/" + path,
        filter(lambda x: x.endswith(".csv.gz"), os.listdir("./common-crawl/")),
    )
)


def ingest_country_asn():
    client, stub = get_client()

    asntbl = get_asn_table()
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


def insert(job_index, filename, batch_size=100, iterations=50000):
    if get_checkpoint(filename):
        return 0

    print(f"starting job {job_index}")
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

                # If max iterations exceeded, return
                if iterations is not None and index > iterations:
                    return

                # Help garbage collection
                del txn
                txn = client.txn()

            count += 1

        # Commit transaction.
        txn.commit()

    except Exception as e:
        print(e)
        print(traceback.format_exc())

    finally:
        stub.close()
        domain_uids.close()
        asn_uids.close()
        file.close()
        set_checkpoint(filename)
        return count


def ingest_process_init(l: mp.Lock):
    global lock
    lock = l


def main():
    init_checkpoints()
    initialize_dgraph()
    ingest_country_asn()

    processes = 16

    l = mp.Lock()
    print("Initializing pool")

    start_time = time.time()
    with mp.Pool(processes=processes, initializer=ingest_process_init, initargs=(l,)) as pool:
        print(f"Starting {processes} worker processes")
        counts = pool.starmap(insert, enumerate(filepaths))
        pool.close()
    end_time = time.time()

    print(
        "Finished in {:.2f}s with {:.2f}rows/s".format(end_time - start_time, sum(counts) / (end_time - start_time))
    )


if __name__ == '__main__':
    main()
