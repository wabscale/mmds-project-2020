import pandas as pd


def get_asn_table():
    asntbl = pd.read_csv('ip2asn-v4.tsv.gz', compression='gzip', sep='\t', header=None)
    asntbl.columns = ['start', 'end', 'asn number', 'country', 'organization']
    asntbl.drop(['start', 'end'], inplace=True, axis=1)
    asntbl.drop_duplicates(subset=['asn number'], inplace=True)
    asntbl = asntbl[asntbl['asn number'] != 0]
    asntbl.set_index('asn number', inplace=True)
    asntbl.drop_duplicates(inplace=True)

    return asntbl