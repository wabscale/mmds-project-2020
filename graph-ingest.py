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
from utils.cache import LayeredCache, FullLayeredCache
from utils.checkpoints import set_checkpoint_lock, init_checkpoints, set_checkpoint, get_checkpoint
from utils.dgraph import get_client, initialize_dgraph


def ingest_country_asn():
    """
    This function will ingest the country, and asn data into both DGraph and
    into the redis cache.

    Both the ASN and country data can be stored in memory in redis and or
    the layer 1 LRU cache. For this, we handle these node types initially.

    There are less than 300 countries and ~62K ASNs

    :return:
    """

    # Create DGraph client
    client, stub = get_client()

    # Get pandas.DataFrame of ASN data
    asn_table = get_asn_table()

    # Create layered caches for both countries and ASNs
    country_uids = LayeredCache('country', 300)
    asn_uids = LayeredCache('asnnum', 10000)

    txn = client.txn()

    if not get_checkpoint('asns'):
        for index, asnrow in tqdm.tqdm(enumerate(asn_table.itertuples()), desc='Ingesting ASNs', total=len(asn_table)):
            # Create ASN Node
            asn = {
                "uid": "_:" + str(asnrow.Index),
                "dgraph.type": "ASN",
                "asnnum": asnrow.Index,
                "org": asnrow.organization,
            }
            response = txn.mutate(set_obj=asn)
            asn_uids[asnrow.Index] = response.uids[str(asnrow.Index)]

            # Batch ASN node commits. 500 seems to be the sweet
            # spot. If we go too low or too high, it gets painfully
            # slow.
            if index % 500 == 0:
                txn.commit()
                del txn
                txn = client.txn()

        txn.commit()
        del txn
        txn = client.txn()
        set_checkpoint('asns')

    if not get_checkpoint('countries'):
        for country_code in tqdm.tqdm(asn_table['country'].dropna().unique(), desc='Ingesting countries'):
            # Create Country Node
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
    domain_uids = FullLayeredCache("domain", 1000000)
    asn_uids = LayeredCache("asnnum", 10000)
    country_uids = LayeredCache('country', 300)

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


def main():
    # Initialize checkpoint file
    init_checkpoints()

    # Initialize dgraph schema
    initialize_dgraph()

    # Ingest country and ASN data
    ingest_country_asn()

    # Number of processes to use in the worker pool
    processes = 16

    # Initialize lock for checkpoint file
    checkpoint_lock = mp.Lock()

    # Get data file paths
    file_paths = map(
        lambda path: "./common-crawl/" + path,
        filter(lambda x: x.endswith(".csv.gz"), os.listdir("./common-crawl/")),
    )

    # Create worker pool
    start_time = time.time()
    with mp.Pool(processes=processes, initializer=set_checkpoint_lock, initargs=(checkpoint_lock,)) as pool:
        print(f"Starting {processes} worker processes")

        # Run insert function on all files we can see
        counts = pool.starmap(insert, enumerate(file_paths))

        # Close pool
        pool.close()
    elapsed = time.time() - start_time

    print("Finished in {:.2f}s with {:.2f}rows/s {} processes".format(elapsed, sum(counts) / elapsed, processes))


if __name__ == '__main__':
    main()
