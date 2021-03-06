import random

import pydgraph

from utils.checkpoints import get_checkpoint, set_checkpoint

schema = """
asnnum: int @index(int) .
org: string .
domains: [uid] @reverse .

type ASN {
    asnnum
    org
    domains
}

domain: string @index(trigram,exact) .
tld: string .
ip: string .
documents: [uid] @reverse .

type Domain {
    domain
    tld
    ip
    documents
}

path: string @index(term) .
type Document {
    path
}

country_code: string @index(exact) .
asns: [uid] @reverse .
type Country {
    country_code
    asns
}

countries: [uid] @reverse .
type Root {
    countries
}

"""


class StubWrapper:
    """
    Very simple class for initializing and tracking DGraph connection stubs
    """

    def __init__(self, n: int):
        """
        Initialize random stubs to ensure relatively balanced load on dgraph
        alpha nodes.

        :param n:
        """

        i = random.randint(0, n)

        # Pick 2 random stubs from those available
        self.stubs = [
            pydgraph.DgraphClientStub(f"localhost:{9080 + (i % n)}"),
            pydgraph.DgraphClientStub(f"localhost:{9080 + ((i + 1) % n)}"),
            pydgraph.DgraphClientStub(f"localhost:{9080 + ((i + 2) % n)}"),
        ]

    def close(self):
        """
        Close each stub connection.

        :return:
        """

        for stub in self.stubs:
            stub.close()


def get_client():
    """
    Get a new dgraph client and stub wrapper

    :return: client, stubs
    """

    # Initialize stub wrapper
    client_stub = StubWrapper(6)

    # Pass back client and stubs
    return pydgraph.DgraphClient(*client_stub.stubs), client_stub


def drop_all(client):
    """
    This function drops all dgraph nodes, edges and schemas.

    Essentially a reset on DGraph

    :param client:
    :return:
    """

    print("Dropping DGraph data")
    return client.alter(pydgraph.Operation(drop_all=True))


def set_schema(client):
    """
    Sets up schema for data we are about to ingest.

    :param client:
    :return:
    """

    print("Initializing DGraph Schema")
    return client.alter(pydgraph.Operation(schema=schema))


def initialize_dgraph():
    # Check for init checkpoint
    if get_checkpoint('init'):
        return

    # Get DGraph client and stub
    client, stub = get_client()

    # Drop any and all data and schema
    drop_all(client)

    # Setup schema in DGraph
    set_schema(client)

    # Close outstanding connections
    stub.close()

    # Set checkpoint
    set_checkpoint('init')
