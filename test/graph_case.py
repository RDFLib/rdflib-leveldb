import unittest
from rdflib import Graph
from rdflib import RDF
from rdflib import URIRef

michel = URIRef("urn:michel")
tarek = URIRef("urn:tarek")
bob = URIRef("urn:bob")
likes = URIRef("urn:likes")
hates = URIRef("urn:hates")
pizza = URIRef("urn:pizza")
cheese = URIRef("urn:cheese")


class GraphTestCase(unittest.TestCase):
    storetest = True
    store_name = "LevelDB"
    create = True
    identifier = URIRef("http://rdflib.net")

    def setUp(self):
        self.graph = Graph(store=self.store_name)
        self.graph.open(self.path, create=self.create)

    def tearDown(self):
        self.graph.close()
        self.graph.destroy(self.path)

    def addStuff(self):
        self.graph.add((tarek, likes, pizza))
        self.graph.add((tarek, likes, cheese))
        self.graph.add((michel, likes, pizza))
        self.graph.add((michel, likes, cheese))
        self.graph.add((bob, likes, cheese))
        self.graph.add((bob, hates, pizza))
        self.graph.add((bob, hates, michel))  # gasp!
        self.graph.commit()

    def removeStuff(self):
        self.graph.remove((tarek, likes, pizza))
        self.graph.remove((tarek, likes, cheese))
        self.graph.remove((michel, likes, pizza))
        self.graph.remove((michel, likes, cheese))
        self.graph.remove((bob, likes, cheese))
        self.graph.remove((bob, hates, pizza))
        self.graph.remove((bob, hates, michel))  # gasp!

    def testAdd(self):
        self.addStuff()

    def testRemove(self):
        self.addStuff()
        self.removeStuff()

    def testTriples(self):
        asserte = self.assertEqual
        triples = self.graph.triples
        Any = None

        self.addStuff()

        # unbound subjects
        asserte(len(list(triples((Any, likes, pizza)))), 2)
        asserte(len(list(triples((Any, hates, pizza)))), 1)
        asserte(len(list(triples((Any, likes, cheese)))), 3)
        asserte(len(list(triples((Any, hates, cheese)))), 0)

        # unbound objects
        asserte(len(list(triples((michel, likes, Any)))), 2)
        asserte(len(list(triples((tarek, likes, Any)))), 2)
        asserte(len(list(triples((bob, hates, Any)))), 2)
        asserte(len(list(triples((bob, likes, Any)))), 1)

        # unbound predicates
        asserte(len(list(triples((michel, Any, cheese)))), 1)
        asserte(len(list(triples((tarek, Any, cheese)))), 1)
        asserte(len(list(triples((bob, Any, pizza)))), 1)
        asserte(len(list(triples((bob, Any, michel)))), 1)

        # unbound subject, objects
        asserte(len(list(triples((Any, hates, Any)))), 2)
        asserte(len(list(triples((Any, likes, Any)))), 5)

        # unbound predicates, objects
        asserte(len(list(triples((michel, Any, Any)))), 2)
        asserte(len(list(triples((bob, Any, Any)))), 3)
        asserte(len(list(triples((tarek, Any, Any)))), 2)

        # unbound subjects, predicates
        asserte(len(list(triples((Any, Any, pizza)))), 3)
        asserte(len(list(triples((Any, Any, cheese)))), 3)
        asserte(len(list(triples((Any, Any, michel)))), 1)

        # all unbound
        asserte(len(list(triples((Any, Any, Any)))), 7)
        self.removeStuff()
        asserte(len(list(triples((Any, Any, Any)))), 0)

    @unittest.skip(
        "DeprecationWarning: Class Statement is deprecated, and will be removed in the future."
    )
    def testStatementNode(self):
        graph = self.graph

        from rdflib.term import Statement

        c = URIRef("http://example.org/foo#c")
        r = URIRef("http://example.org/foo#r")
        s = Statement((self.michel, self.likes, self.pizza), c)
        graph.add((s, RDF.value, r))
        self.assertEqual(r, graph.value(s, RDF.value))
        self.assertEqual(s, graph.value(predicate=RDF.value, object=r))

    def testGraph(self):
        from rdflib.graph import Graph

        graph = self.graph

        alice = URIRef("alice")

        g1 = Graph()
        g1.add((alice, RDF.value, pizza))
        g1.add((bob, RDF.value, cheese))
        g1.add((bob, RDF.value, pizza))

        g2 = Graph()
        g2.add((bob, RDF.value, pizza))
        g2.add((bob, RDF.value, cheese))
        g2.add((alice, RDF.value, pizza))

        gv1 = Graph(store=graph.store, base=g1)
        gv2 = Graph(store=graph.store, base=g2)
        graph.add((gv1, RDF.value, gv2))
        v = graph.value(gv1)
        self.assertEqual(gv2, v)
        graph.remove((gv1, RDF.value, gv2))

    def testConnected(self):
        graph = self.graph
        self.addStuff()
        self.assertEqual(True, graph.connected())

        jeroen = URIRef("jeroen")
        unconnected = URIRef("unconnected")

        graph.add((jeroen, likes, unconnected))

        self.assertEqual(False, graph.connected())

    def testSub(self):
        g1 = Graph()
        g2 = Graph()

        g1.add((tarek, likes, pizza))
        g1.add((bob, likes, cheese))

        g2.add((bob, likes, cheese))

        g3 = g1 - g2

        self.assertEqual(len(g3), 1)
        self.assertEqual((tarek, likes, pizza) in g3, True)
        self.assertEqual((tarek, likes, cheese) in g3, False)

        self.assertEqual((bob, likes, cheese) in g3, False)

        g1 -= g2

        self.assertEqual(len(g1), 1)
        self.assertEqual((tarek, likes, pizza) in g1, True)
        self.assertEqual((tarek, likes, cheese) in g1, False)

        self.assertEqual((bob, likes, cheese) in g1, False)

    def testGraphAdd(self):
        g1 = Graph()
        g2 = Graph()

        g1.add((tarek, likes, pizza))

        g2.add((bob, likes, cheese))

        g3 = g1 + g2

        self.assertEqual(len(g3), 2)
        self.assertEqual((tarek, likes, pizza) in g3, True)
        self.assertEqual((tarek, likes, cheese) in g3, False)

        self.assertEqual((bob, likes, cheese) in g3, True)

        g1 += g2

        self.assertEqual(len(g1), 2)
        self.assertEqual((tarek, likes, pizza) in g1, True)
        self.assertEqual((tarek, likes, cheese) in g1, False)

        self.assertEqual((bob, likes, cheese) in g1, True)

    def testGraphIntersection(self):
        g1 = Graph()
        g2 = Graph()

        g1.add((tarek, likes, pizza))
        g1.add((michel, likes, cheese))

        g2.add((bob, likes, cheese))
        g2.add((michel, likes, cheese))

        g3 = g1 * g2

        self.assertEqual(len(g3), 1)
        self.assertEqual((tarek, likes, pizza) in g3, False)
        self.assertEqual((tarek, likes, cheese) in g3, False)

        self.assertEqual((bob, likes, cheese) in g3, False)

        self.assertEqual((michel, likes, cheese) in g3, True)

        g1 *= g2

        self.assertEqual(len(g1), 1)

        self.assertEqual((tarek, likes, pizza) in g1, False)
        self.assertEqual((tarek, likes, cheese) in g1, False)

        self.assertEqual((bob, likes, cheese) in g1, False)

        self.assertEqual((michel, likes, cheese) in g1, True)


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

if __name__ == "__main__":
    unittest.main()
