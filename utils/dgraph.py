import random

import pydgraph
from utils.checkpoints import get_checkpoint, set_checkpoint


def get_client():
    class Stubs:
        def __init__(self, n):
            i = random.randint(0, n)
            self.stubs = [
                pydgraph.DgraphClientStub(f"localhost:{9080 + (i % n)}"),
                pydgraph.DgraphClientStub(f"localhost:{9080 + ((i + 1) % n)}"),
            ]

        def close(self):
            for stub in self.stubs:
                stub.close()

    client_stub = Stubs(6)
    return pydgraph.DgraphClient(*client_stub.stubs), client_stub


def drop_all(client):
    print("Dropping all")
    return client.alter(pydgraph.Operation(drop_all=True))


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


def initialize_dgraph():
    if not get_checkpoint('init'):
        client, stub = get_client()
        drop_all(client)
        set_schema(client)
        stub.close()
        set_checkpoint('init')