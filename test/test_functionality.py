# -*- coding: utf-8 -*-
import pytest
import os
import re
import tempfile
from rdflib import BNode, Literal, RDF, RDFS, URIRef, Variable
from rdflib.graph import ConjunctiveGraph, Graph, QuotedGraph
import logging

logging.basicConfig(level=logging.ERROR, format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


storename = "LevelDB"
storetest = True

implies = URIRef("http://www.w3.org/2000/10/swap/log#implies")

testN3 = """\
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix : <http://test/> .
{:a :b :c;a :foo} => {:a :d :c,?y} .
_:foo a rdfs:Class .
:a :d :c .
"""

michel = URIRef("urn:michel")
tarek = URIRef("urn:tarek")
bob = URIRef("urn:bob")
likes = URIRef("urn:likes")
hates = URIRef("urn:hates")
pizza = URIRef("urn:pizza")
cheese = URIRef("urn:cheese")

graphuri = URIRef("urn:graph")
othergraphuri = URIRef("urn:othergraph")


create = True
reuse = False


@pytest.fixture
def getgraph():
    graph = ConjunctiveGraph(store="LevelDB")
    path = os.path.join(tempfile.gettempdir(), "test_leveldb")
    if os.path.exists(path):
        if os.path.isdir(path):
            import shutil

            shutil.rmtree(path)
        elif len(path.split(":")) == 1:
            os.unlink(path)
        else:
            os.remove(path)

    graph.open(path, create=create)
    yield graph

    graph.close()

    if not reuse:
        graph.store.destroy(configuration=path)


def testSimpleGraph(getgraph):
    graph = getgraph
    g = graph.get_context(graphuri)
    g.add((tarek, likes, pizza))
    g.add((bob, likes, pizza))
    g.add((bob, likes, cheese))

    g2 = graph.get_context(othergraphuri)
    g2.add((michel, likes, pizza))

    assert len(g) == 3  # "graph contains 3 triples")
    assert len(g2) == 1  # "other graph contains 1 triple")

    r = g.query("SELECT * WHERE { ?s <urn:likes> <urn:pizza> . }")
    assert len(list(r)) == 2  # "two people like pizza")

    r = g.triples((None, likes, pizza))
    assert len(list(r)) == 2  # "two people like pizza")

    # Test initBindings
    r = g.query(
        "SELECT * WHERE { ?s <urn:likes> <urn:pizza> . }",
        initBindings={"s": tarek},
    )
    assert len(list(r)) == 1  # "i was asking only about tarek")

    r = g.triples((tarek, likes, pizza))
    assert len(list(r)) == 1  # "i was asking only about tarek")

    r = g.triples((tarek, likes, cheese))
    assert len(list(r)) == 0  # "tarek doesn't like cheese")

    g2.add((tarek, likes, pizza))
    g.remove((tarek, likes, pizza))
    r = g.query("SELECT * WHERE { ?s <urn:likes> <urn:pizza> . }")


def testConjunctiveDefault(getgraph):
    graph = getgraph
    g = graph.get_context(graphuri)
    g.add((tarek, likes, pizza))
    g2 = graph.get_context(othergraphuri)
    g2.add((bob, likes, pizza))
    g.add((tarek, hates, cheese))

    assert len(g) == 2  # "graph contains 2 triples")

    # the following are actually bad tests as they depend on your endpoint,
    # as pointed out in the sparqlstore.py code:
    #
    # # For ConjunctiveGraphs, reading is done from the "default graph" Exactly
    # # what this means depends on your endpoint, because SPARQL does not offer a
    # # simple way to query the union of all graphs as it would be expected for a
    # # ConjuntiveGraph.
    # #
    # # Fuseki/TDB has a flag for specifying that the default graph
    # # is the union of all graphs (tdb:unionDefaultGraph in the Fuseki config).
    assert (
        len(graph) == 3
    )  # "default union graph should contain three triples but contains:\n" "%s" % list(graph),

    r = graph.query("SELECT * WHERE { ?s <urn:likes> <urn:pizza> . }")
    assert len(list(r)) == 2  # "two people like pizza")

    r = graph.query(
        "SELECT * WHERE { ?s <urn:likes> <urn:pizza> . }",
        initBindings={"s": tarek},
    )
    assert len(list(r)) == 1  # "i was asking only about tarek")

    r = graph.triples((tarek, likes, pizza))
    assert len(list(r)) == 1  # "i was asking only about tarek")

    r = graph.triples((tarek, likes, cheese))
    assert len(list(r)) == 0  # "tarek doesn't like cheese")

    g2.remove((bob, likes, pizza))

    r = graph.query("SELECT * WHERE { ?s <urn:likes> <urn:pizza> . }")
    assert len(list(r)) == 1  # "only tarek likes pizza")


def testUpdate(getgraph):
    graph = getgraph
    graph.update(
        "INSERT DATA { GRAPH <urn:graph> { <urn:michel> <urn:likes> <urn:pizza> . } }"
    )

    g = graph.get_context(graphuri)
    assert len(g) == 1  # "graph contains 1 triples")


def testUpdateWithInitNs(getgraph):
    graph = getgraph
    graph.update(
        "INSERT DATA { GRAPH ns:graph { ns:michel ns:likes ns:pizza . } }",
        initNs={"ns": URIRef("urn:")},
    )

    g = graph.get_context(graphuri)
    assert set(g.triples((None, None, None))) == set(
        [(michel, likes, pizza)]
    )  # "only michel likes pizza"


def testUpdateWithInitBindings(getgraph):
    graph = getgraph
    graph.update(
        "INSERT { GRAPH <urn:graph> { ?a ?b ?c . } } WherE { }",
        initBindings={
            "a": URIRef("urn:michel"),
            "b": URIRef("urn:likes"),
            "c": URIRef("urn:pizza"),
        },
    )

    g = graph.get_context(graphuri)
    assert set(g.triples((None, None, None))) == set(
        [(michel, likes, pizza)]
    )  # only michel likes pizza"


def testMultipleUpdateWithInitBindings(getgraph):
    graph = getgraph
    graph.update(
        "INSERT { GRAPH <urn:graph> { ?a ?b ?c . } } WHERE { };"
        "INSERT { GRAPH <urn:graph> { ?d ?b ?c . } } WHERE { }",
        initBindings={
            "a": URIRef("urn:michel"),
            "b": URIRef("urn:likes"),
            "c": URIRef("urn:pizza"),
            "d": URIRef("urn:bob"),
        },
    )

    g = graph.get_context(graphuri)
    assert set(g.triples((None, None, None))) == set(
        [(michel, likes, pizza), (bob, likes, pizza)]
    )  # "michel and bob like pizza",


def testNamedGraphUpdate(getgraph):
    graph = getgraph
    g = graph.get_context(graphuri)
    r1 = "INSERT DATA { <urn:michel> <urn:likes> <urn:pizza> }"
    g.update(r1)
    assert set(g.triples((None, None, None))) == set(
        [(michel, likes, pizza)]
    )  # "only michel likes pizza"

    r2 = (
        "DELETE { <urn:michel> <urn:likes> <urn:pizza> } "
        + "INSERT { <urn:bob> <urn:likes> <urn:pizza> } WHERE {}"
    )
    g.update(r2)
    assert set(g.triples((None, None, None))) == set(
        [(bob, likes, pizza)]
    )  # "only bob likes pizza",

    says = URIRef("urn:says")

    # Strings with unbalanced curly braces
    tricky_strs = [
        "With an unbalanced curly brace %s " % brace for brace in ["{", "}"]
    ]
    for tricky_str in tricky_strs:
        r3 = (
            """INSERT { ?b <urn:says> "%s" }
        WHERE { ?b <urn:likes> <urn:pizza>} """
            % tricky_str
        )
        g.update(r3)

    values = set()
    for v in g.objects(bob, says):
        values.add(str(v))
    assert values == set(tricky_strs)

    # Complicated Strings
    r4strings = []
    r4strings.append(r'''"1: adfk { ' \\\" \" { "''')
    r4strings.append(r'''"2: adfk } <foo> #éï \\"''')

    r4strings.append(r"""'3: adfk { " \\\' \' { '""")
    r4strings.append(r"""'4: adfk } <foo> #éï \\'""")

    r4strings.append(r'''"""5: adfk { ' \\\" \" { """''')
    r4strings.append(r'''"""6: adfk } <foo> #éï \\"""''')
    r4strings.append('"""7: ad adsfj \n { \n sadfj"""')

    r4strings.append(r"""'''8: adfk { " \\\' \' { '''""")
    r4strings.append(r"""'''9: adfk } <foo> #éï \\'''""")
    r4strings.append("'''10: ad adsfj \n { \n sadfj'''")

    r4 = "\n".join(
        ["INSERT DATA { <urn:michel> <urn:says> %s } ;" % s for s in r4strings]
    )
    g.update(r4)
    values = set()
    for v in g.objects(michel, says):
        values.add(str(v))
    assert values == set(
        [
            re.sub(
                r"\\(.)",
                r"\1",
                re.sub(r"^'''|'''$|^'|'$|" + r'^"""|"""$|^"|"$', r"", s),
            )
            for s in r4strings
        ]
    )

    # IRI Containing ' or #
    # The fragment identifier must not be misinterpreted as a comment
    # (commenting out the end of the block).
    # The ' must not be interpreted as the start of a string, causing the }
    # in the literal to be identified as the end of the block.
    r5 = """INSERT DATA { <urn:michel> <urn:hates> <urn:foo'bar?baz;a=1&b=2#fragment>, "'}" }"""

    g.update(r5)
    values = set()
    for v in g.objects(michel, hates):
        values.add(str(v))
    assert values == set(["urn:foo'bar?baz;a=1&b=2#fragment", "'}"])

    # Comments
    r6 = """
        INSERT DATA {
            <urn:bob> <urn:hates> <urn:bob> . # No closing brace: }
            <urn:bob> <urn:hates> <urn:michel>.
        }
    #Final { } comment"""

    g.update(r6)
    values = set()
    for v in g.objects(bob, hates):
        values.add(v)
    assert values == set([bob, michel])


def testNamedGraphUpdateWithInitBindings(getgraph):
    graph = getgraph
    g = graph.get_context(graphuri)
    r = "INSERT { ?a ?b ?c } WHERE {}"
    g.update(r, initBindings={"a": michel, "b": likes, "c": pizza})
    assert set(g.triples((None, None, None))) == set(
        [(michel, likes, pizza)]
    )  # "only michel likes pizza",


def testEmptyLiteral(getgraph):
    graph = getgraph
    # test for https://github.com/RDFLib/rdflib/issues/457
    # also see test_issue457.py which is sparql store independent!
    g = graph.get_context(graphuri)
    g.add(
        (
            URIRef("http://example.com/s"),
            URIRef("http://example.com/p"),
            Literal(""),
        )
    )

    o = tuple(g)[0][2]
    assert o == Literal(""), repr(o)


def testN3Store(getgraph):
    g = getgraph
    g.parse(data=testN3, format="n3")
    formulaA = BNode()
    formulaB = BNode()
    for s, p, o in g.triples((None, implies, None)):
        formulaA = s
        formulaB = o

    assert type(formulaA) == QuotedGraph and type(formulaB) == QuotedGraph
    a = URIRef("http://test/a")
    b = URIRef("http://test/b")
    c = URIRef("http://test/c")
    d = URIRef("http://test/d")
    v = Variable("y")

    universe = ConjunctiveGraph(g.store)

    # test formula as terms
    assert len(list(universe.triples((formulaA, implies, formulaB)))) == 1

    # test variable as term and variable roundtrip
    assert len(list(formulaB.triples((None, None, v)))) == 1
    for s, p, o in formulaB.triples((None, d, None)):
        if o != c:
            assert isinstance(o, Variable)
            assert o == v
    s = list(universe.subjects(RDF.type, RDFS.Class))[0]
    assert isinstance(s, BNode)
    assert len(list(universe.triples((None, implies, None)))) == 1
    assert len(list(universe.triples((None, RDF.type, None)))) == 1
    assert len(list(formulaA.triples((None, RDF.type, None)))) == 1
    assert len(list(formulaA.triples((None, None, None)))) == 2
    assert len(list(formulaB.triples((None, None, None)))) == 2
    assert len(list(universe.triples((None, None, None)))) == 3
    assert (
        len(list(formulaB.triples((None, URIRef("http://test/d"), None)))) == 2
    )
    assert (
        len(list(universe.triples((None, URIRef("http://test/d"), None)))) == 1
    )

    # context tests
    # test contexts with triple argument
    assert len(list(universe.contexts((a, d, c)))) == 1

    # Remove test cases
    universe.remove((None, implies, None))
    assert len(list(universe.triples((None, implies, None)))) == 0
    assert len(list(formulaA.triples((None, None, None)))) == 2
    assert len(list(formulaB.triples((None, None, None)))) == 2

    formulaA.remove((None, b, None))
    assert len(list(formulaA.triples((None, None, None)))) == 1
    formulaA.remove((None, RDF.type, None))
    assert len(list(formulaA.triples((None, None, None)))) == 0

    universe.remove((None, RDF.type, RDFS.Class))

    # remove_context tests
    universe.remove_context(formulaB)
    assert len(list(universe.triples((None, RDF.type, None)))) == 0
    assert len(universe) == 1
    assert len(formulaB) == 0

    universe.remove((None, None, None))
    assert len(universe) == 0


xmltestdoc = """<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF
   xmlns="http://example.org/"
   xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
>
  <rdf:Description rdf:about="http://example.org/a">
    <b rdf:resource="http://example.org/c"/>
  </rdf:Description>
</rdf:RDF>
"""

n3testdoc = """@prefix : <http://example.org/> .

:a :b :c .
"""

nttestdoc = (
    "<http://example.org/a> <http://example.org/b> <http://example.org/c> .\n"
)
