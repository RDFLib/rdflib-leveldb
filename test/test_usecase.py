# -*- coding: utf-8 -*-import tempfile
import os
import tempfile
from rdflib import URIRef
from rdflib.graph import Graph


path = os.path.join(tempfile.gettempdir(), "test_leveldb")


def test_create_db():
    if os.path.exists(path):
        if os.path.isdir(path):
            import shutil

            shutil.rmtree(path)
        elif len(path.split(":")) == 1:
            os.unlink(path)
        else:
            os.remove(path)

    graph = Graph("LevelDB", URIRef("http://rdflib.net"))
    graph.open(path, create=True)
    assert repr(graph.identifier) == "rdflib.term.URIRef('http://rdflib.net')"
    assert (
        str(graph)
        == "<http://rdflib.net> a rdfg:Graph;rdflib:storage [a rdflib:Store;rdfs:label 'LevelDBStore']."
    )
    graph.close()
    graph.destroy(configuration=path)


def test_reuse():
    graph = Graph("LevelDB", URIRef("http://rdflib.net"))
    graph.open(path, create=True)
    assert repr(graph.identifier) == "rdflib.term.URIRef('http://rdflib.net')"
    graph.close()

    graph = Graph("LevelDB", URIRef("http://rdflib.net"))
    graph.open(path, create=False)
    assert repr(graph.identifier) == "rdflib.term.URIRef('http://rdflib.net')"
    assert (
        str(graph)
        == "<http://rdflib.net> a rdfg:Graph;rdflib:storage [a rdflib:Store;rdfs:label 'LevelDBStore']."
    )
    graph.close()
    graph.destroy(configuration=path)


def test_example():
    graph = Graph("LevelDB", URIRef("http://rdflib.net"))
    graph.open(path, create=True)
    assert repr(graph.identifier) == "rdflib.term.URIRef('http://rdflib.net')"
    # Parse in an RDF file hosted on the Internet
    graph.parse("http://www.w3.org/People/Berners-Lee/card")

    # Loop through each triple in the graph (subj, pred, obj)
    for subj, pred, obj in graph:
        # Check if there is at least one triple in the Graph
        if (subj, pred, obj) not in graph:
            raise Exception("It better be!")

    assert len(graph) == 86, len(graph)

    # Print out the entire Graph in the RDF Turtle format
    # print(graph.serialize(format="turtle"))
    graph.close()
    graph.destroy(configuration=path)
