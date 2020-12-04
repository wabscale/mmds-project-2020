import os
import csv
import gzip
import json
import time
import pyasn
import parse
import socket
import typing
import numpy as np
import pandas as pd
import multiprocessing as mp
import matplotlib.pyplot as plt
from ipaddress import ip_address, ip_network, summarize_address_range

asndb = pyasn.pyasn('asn-db.dat')

asntbl = pd.read_csv('ip2asn-v4.tsv.gz', compression='gzip', sep='\t', header=None)
asntbl.columns = ['start', 'end', 'asn number', 'country', 'organization']
asntbl.drop(['start', 'end'], inplace=True, axis=1)
asntbl.drop_duplicates(subset=['asn number'], inplace=True)
asntbl = asntbl[asntbl['asn number'] != 0]
asntbl.set_index('asn number', inplace=True)

columns = ['domain', 'ip', 'asn_num', 'country', 'asn_org', 'tld', 'path', 'status', 'timestamp', 'mime', 'mime_detected', 'length', 'url', 'redirect']
dnscache = dict()

def resolv(domain: str) -> str:
    if dnscache.get(domain, None) is None:
        try:
            ip_address(domain)
            ip = domain
            dnscache[domain] = ip
        except ValueError:
            try:
                ip = socket.gethostbyname(domain)
                dnscache[domain] = ip
            except socket.gaierror:
                ip = None
    else:
        ip = dnscache[domain]

    return ip

def getasn(ip: str) -> typing.Tuple[int, str, str]:
    asnnum = -1
    try:
        asnnum, _ = asndb.lookup(ip)
        asnrow = asntbl.loc[asnnum]
        country = asnrow['country']
        org = asnrow['organization']
    except:
        country, org = None, None
    return asnnum, country, org

def parse_n_save(filenames: typing.List[str], split: int):
    buffer = []
    buffer_size = 20000

    for filename in filenames:
        if not filename.endswith('.gz'):
            continue

        cdx_num = parse.parse('cdx-{num}.gz', filename).named['num']
        csv_name = 'common-crawl/cdx-{}.csv.gz'.format(cdx_num)
        csv_file = gzip.open(csv_name, 'wt')

        writer = csv.writer(csv_file)
        writer.writerow(columns)

        _start = time.time()
        with gzip.open('./common-crawl-raw/' + filename, 'rb') as f:
            for line in f:
                parsed = parse.parse("{domain}){path} {timestamp} {meta}", line.decode())
                meta = json.loads(parsed['meta'])
                domain = '.'.join(parsed.named.get('domain', None).split(',')[::-1])
                ip = resolv(domain)
                asnnum, country, org = getasn(ip)

                buffer.append([
                    domain,
                    ip,
                    asnnum,
                    country,
                    org,
                    parsed.named.get('domain', None).split(',')[-1],
                    parsed.named.get('path', None),
                    meta.get('status', None),
                    parsed.named.get('timestamp', None),
                    meta.get('mime', None),
                    meta.get('mime-detected'),
                    meta.get('length', None),
                    meta.get('url', None),
                    meta.get('redirect', None),
                ])

                if len(buffer) == buffer_size:
                    for row in buffer:
                        writer.writerow(row)

                    global dnscache
                    del buffer
                    del dnscache
                    dnscache = dict()
                    buffer = list()

        print('closing {} {:.2f} hrs'.format(csv_name, (time.time() - _start) / 3600.))
        csv_file.close()


filenames = list(filter(lambda filename: filename.endswith('.gz'), os.listdir('common-crawl-raw')))

splits = []
splitsize = 2
for i in range(0, len(filenames), splitsize):
    splits.append(filenames[i:i+splitsize])

args = list(map(lambda x: (x[1], x[0]), enumerate(splits)))
with mp.Pool(len(splits)) as pool:
    pool.starmap(parse_n_save, args)
    pool.close()
