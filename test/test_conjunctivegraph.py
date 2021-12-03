# -*- coding: utf-8 -*-
import pytest
import tempfile
import rdflib
from rdflib_leveldb.leveldbstore import readable_index, NoopMethods
from rdflib.graph import ConjunctiveGraph, URIRef
import logging

logging.basicConfig(level=logging.ERROR, format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

storetest = True

michel = URIRef("urn:michel")
bob = URIRef("urn:bob")
cheese = URIRef("urn:cheese")
likes = URIRef("urn:likes")
pizza = URIRef("urn:pizza")
uri1 = URIRef("urn:graph1")
uri2 = URIRef("urn:graph2")


@pytest.fixture
def getconjunctivegraph():
    store = "LevelDB"
    graph = ConjunctiveGraph(store=store)
    path = tempfile.mktemp(prefix="testleveldb")
    graph.open(path, create=True)
    yield graph
    graph.close()
    graph.destroy(path)


def test_namespaces(getconjunctivegraph):
    graph = getconjunctivegraph
    graph.bind("dc", "http://http://purl.org/dc/elements/1.1/")
    graph.bind("foaf", "http://xmlns.com/foaf/0.1/")
    assert len(list(graph.namespaces())) == 6
    assert ("foaf", rdflib.term.URIRef("http://xmlns.com/foaf/0.1/")) in list(
        graph.namespaces()
    )


def test_readable_index(getconjunctivegraph):
    assert repr(readable_index(111)) == "'s,p,o'"


def test_triples_context_reset(getconjunctivegraph):
    # I don't think this is doing what it says on the tin
    graph = getconjunctivegraph
    graph.add((michel, likes, pizza))
    graph.add((michel, likes, cheese))
    graph.commit()
    ntriples = list(
        graph.triples((None, None, None), context=next(graph.contexts()))
    )
    assert len(ntriples) == 2  # len(ntriples))


def test_remove_context_reset(getconjunctivegraph):
    graph = getconjunctivegraph
    graph.add((michel, likes, pizza))
    graph.add((michel, likes, cheese))
    graph.commit()
    graph.remove((michel, likes, cheese, next(graph.contexts())))
    graph.commit()
    ntriples = list(graph.triples((None, None, None)))
    assert len(ntriples) == 1  # len(ntriples))


def test_remove_db_exception(getconjunctivegraph):
    graph = getconjunctivegraph
    graph.add((michel, likes, pizza))
    graph.add((michel, likes, cheese))
    graph.commit()
    ntriples = list(
        graph.triples((None, None, None), context=next(graph.contexts()))
    )
    assert len(ntriples) == 2  # len(ntriples))


def test_nquads_default_graph(getconjunctivegraph):
    graph = getconjunctivegraph
    data = """
    <http://example.org/s1> <http://example.org/p1> <http://example.org/o1> .
    <http://example.org/s2> <http://example.org/p2> <http://example.org/o2> .
    <http://example.org/s3> <http://example.org/p3> <http://example.org/o3> <http://example.org/g3> .
    """

    publicID = URIRef("http://example.org/g0")

    graph.parse(data=data, format="nquads", publicID=publicID)

    assert len(graph) == 3, len(graph)
    assert len(list(graph.contexts())) == 2, len(list(graph.contexts()))
    assert len(graph.get_context(publicID)) == 2, len(
        graph.get_context(publicID)
    )


def test_serialize(getconjunctivegraph):
    graph = getconjunctivegraph
    graph.get_context(uri1).add((bob, likes, pizza))
    graph.get_context(uri2).add((bob, likes, pizza))
    s = graph.serialize(format="nquads")
    assert len([x for x in s.split("\n") if x.strip()]) == 2

    g2 = ConjunctiveGraph(store="LevelDB")
    g2.open(tempfile.mktemp(prefix="leveldbstoretest"), create=True)
    g2.parse(data=s, format="nquads")

    assert len(graph) == len(g2)
    assert sorted(x.identifier for x in graph.contexts()) == sorted(
        x.identifier for x in g2.contexts()
    )


def test_NoopMethods():
    obj = NoopMethods()
    res = obj.__getattr__("amethod")
    assert res() is None
