import pytest
import os
import tempfile
import shutil
import rdflib
from rdflib_leveldb.leveldbstore import readable_index, NoopMethods
from rdflib.graph import Graph, Literal, URIRef
from rdflib.namespace import XSD, RDFS
from rdflib.store import VALID_STORE, NO_STORE
import logging

logging.basicConfig(level=logging.ERROR, format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


store = "LevelDB"
storetest = True
path = os.path.join(tempfile.gettempdir(), f"test_{store.lower()}")


michel = URIRef("urn:michel")
bob = URIRef("urn:bob")
cheese = URIRef("urn:cheese")
likes = URIRef("urn:likes")
pizza = URIRef("urn:pizza")
uri1 = URIRef("urn:graph1")
uri2 = URIRef("urn:graph2")


@pytest.fixture
def getgraph():
    graph = Graph(store=store)
    rt = graph.open(path, create=True)
    assert rt == VALID_STORE, "The underlying store is corrupt"
    assert (
        len(graph) == 0
    ), "There must be zero triples in the graph just after store (file) creation"
    data = """
            PREFIX : <https://example.org/>

            :a :b :c .
            :d :e :f .
            :d :g :h .
            """
    graph.parse(data=data, format="ttl")
    assert (
        len(graph) == 3
    ), "There must be three triples in the graph after the first data chunk parse"
    yield graph

    graph.close()
    graph.destroy(configuration=path)


def test_create_db(getgraph):
    graph = getgraph
    graph.add((michel, likes, pizza))
    graph.add((michel, likes, cheese))
    graph.commit()
    assert (
        len(graph) == 5
    )  # f"There must be three triples in the graph after the first data chunk parse, not {len(graph)}"


# def test_dumpdb(getconjunctivegraph):
#     logger.debug(graph.store.dumpdb())


def test_escape_quoting(getgraph):
    graph = getgraph
    assert (
        len(graph) == 3
    ), "There must be three triples in the graph after the first data chunk parse"
    test_string = "That’s a Literal!!"
    graph.add(
        (
            URIRef("http://example.org/foo"),
            RDFS.label,
            Literal(test_string, datatype=XSD.string),
        )
    )
    graph.commit()
    assert ("That’s a Literal!!") in graph.serialize(format="xml")


def test_namespaces(getgraph):
    graph = getgraph
    graph.bind("dc", "http://http://purl.org/dc/elements/1.1/")
    graph.bind("foaf", "http://xmlns.com/foaf/0.1/")
    assert (
        len(list(graph.namespaces())) == 7
    )  # f"expected 6, got {len(list(graph.namespaces()))}"
    assert ("foaf", URIRef("http://xmlns.com/foaf/0.1/")) in list(
        graph.namespaces()
    )


def test_readable_index(getgraph):
    assert readable_index(111) == "s,p,o"


# def test_missing_db_exception(getgraph):
#     graph.store.close()
#     if getattr(self, "path", False) and path is not None:
#         if os.path.exists(path):
#             if os.path.isdir(path):
#                 shutil.rmtree(path)
#             elif len(path.split(":")) == 1:
#                 os.unlink(path)
#             else:
#                 os.remove(path)
#     graph.store.open(path, create=True)
#     ntriples = graph.triples((None, None, None))
#     assertTrue(len(list(ntriples)) == 0)


def test_reopening_db(getgraph):
    graph = getgraph
    graph.add((michel, likes, pizza))
    graph.add((michel, likes, cheese))
    graph.commit()
    graph.store.close()
    graph.store.open(path, create=False)
    ntriples = graph.triples((None, None, None))
    listntriples = list(ntriples)
    assert len(listntriples) == 5  # f"Expected 2 not {len(listntriples)}"


def test_reopening_missing_db(getgraph):
    graph = getgraph
    graph.store.close()
    graph.store.destroy()
    assert graph.open(path, create=False) == NO_STORE


def test_isopen_db(getgraph):
    graph = getgraph
    assert graph.store.is_open() is True
    graph.store.close()
    assert graph.store.is_open() is False
