# -*- coding: utf-8 -*-
import unittest
import gc
import os
import re
import logging
from time import time
import tempfile
from rdflib import ConjunctiveGraph, URIRef

logging.basicConfig(level=logging.ERROR, format="%(message)s")
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

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


class StoreTestCase(unittest.TestCase):
    """
    Test case for testing store performance... probably should be
    something other than a unit test... but for now we'll add it as a
    unit test.
    """

    store = "Memory"
    path = None
    storetest = True
    performancetest = True

    def setUp(self):
        self.gcold = gc.isenabled()
        gc.collect()
        gc.disable()
        self.graph = ConjunctiveGraph(store=self.store)
        path = os.path.join(
            tempfile.gettempdir(), f"test_{self.store.lower()}"
        )
        self.path = path
        self.graph.open(self.path, create=True)
        self.input = ConjunctiveGraph()

    def tearDown(self):
        self.graph.close()
        if self.gcold:
            gc.enable()
        # TODO: delete a_tmp_dir
        self.graph.close()
        del self.graph

        # Remove test detritus
        if hasattr(self, "path") and self.path is not None:
            if os.path.exists(self.path):
                if os.path.isdir(self.path):
                    import shutil

                    shutil.rmtree(self.path)
                elif len(self.path.split(":")) == 1:
                    os.unlink(self.path)
                else:
                    os.remove(self.path)

    # @unittest.skip("WIP")
    def testSimpleGraph(self):
        t0 = time()
        g = self.graph.get_context(graphuri)
        g.add((tarek, likes, pizza))
        g.add((bob, likes, pizza))
        g.add((bob, likes, cheese))

        g2 = self.graph.get_context(othergraphuri)
        g2.add((michel, likes, pizza))

        self.assertEqual(3, len(g), "graph contains 3 triples")
        self.assertEqual(1, len(g2), "other graph contains 1 triple")

        r = g.query("SELECT * WHERE { ?s <urn:likes> <urn:pizza> . }")
        self.assertEqual(2, len(list(r)), "two people like pizza")

        r = g.triples((None, likes, pizza))
        self.assertEqual(2, len(list(r)), "two people like pizza")

        # Test initBindings
        r = g.query(
            "SELECT * WHERE { ?s <urn:likes> <urn:pizza> . }",
            initBindings={"s": tarek},
        )
        self.assertEqual(1, len(list(r)), "i was asking only about tarek")

        r = g.triples((tarek, likes, pizza))
        self.assertEqual(1, len(list(r)), "i was asking only about tarek")

        r = g.triples((tarek, likes, cheese))
        self.assertEqual(0, len(list(r)), "tarek doesn't like cheese")

        g2.add((tarek, likes, pizza))
        g.remove((tarek, likes, pizza))
        r = g.query("SELECT * WHERE { ?s <urn:likes> <urn:pizza> . }")
        t1 = time()
        log.debug(f"testSimpleGraph {self.store}: {t1 - t0:.5f}")

    # @unittest.skip("WIP")
    def testConjunctiveDefault(self):
        t0 = time()
        g = self.graph.get_context(graphuri)
        g.add((tarek, likes, pizza))
        g2 = self.graph.get_context(othergraphuri)
        g2.add((bob, likes, pizza))
        g.add((tarek, hates, cheese))

        self.assertEqual(2, len(g), "graph contains 2 triples")

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
        self.assertEqual(
            3,
            len(self.graph),
            "default union graph should contain three triples but contains:\n"
            "%s" % list(self.graph),
        )

        r = self.graph.query("SELECT * WHERE { ?s <urn:likes> <urn:pizza> . }")
        self.assertEqual(2, len(list(r)), "two people like pizza")

        r = self.graph.query(
            "SELECT * WHERE { ?s <urn:likes> <urn:pizza> . }",
            initBindings={"s": tarek},
        )
        self.assertEqual(1, len(list(r)), "i was asking only about tarek")

        r = self.graph.triples((tarek, likes, pizza))
        self.assertEqual(1, len(list(r)), "i was asking only about tarek")

        r = self.graph.triples((tarek, likes, cheese))
        self.assertEqual(0, len(list(r)), "tarek doesn't like cheese")

        g2.remove((bob, likes, pizza))

        r = self.graph.query("SELECT * WHERE { ?s <urn:likes> <urn:pizza> . }")
        self.assertEqual(1, len(list(r)), "only tarek likes pizza")
        t1 = time()
        log.debug(f"testConjunctiveDefault {self.store}: {t1 - t0:.5f}")

    # @unittest.skip("WIP")
    def testUpdate(self):
        t0 = time()
        self.graph.update(
            "INSERT DATA { GRAPH <urn:graph> { <urn:michel> <urn:likes> <urn:pizza> . } }"
        )

        g = self.graph.get_context(graphuri)
        self.assertEqual(1, len(g), "graph contains 1 triples")
        t1 = time()
        log.debug(f"testUpdate {self.store}: {t1 - t0:.5f} ")

    # @unittest.skip("WIP")
    def testUpdateWithInitNs(self):
        t0 = time()
        self.graph.update(
            "INSERT DATA { GRAPH ns:graph { ns:michel ns:likes ns:pizza . } }",
            initNs={"ns": URIRef("urn:")},
        )

        g = self.graph.get_context(graphuri)
        self.assertEqual(
            set(g.triples((None, None, None))),
            set([(michel, likes, pizza)]),
            "only michel likes pizza",
        )
        t1 = time()
        log.debug(f"testUpdateWithInitNs {self.store}: {t1 - t0:.5f}")

    # @unittest.skip("WIP")
    def testUpdateWithInitBindings(self):
        t0 = time()
        self.graph.update(
            "INSERT { GRAPH <urn:graph> { ?a ?b ?c . } } WherE { }",
            initBindings={
                "a": URIRef("urn:michel"),
                "b": URIRef("urn:likes"),
                "c": URIRef("urn:pizza"),
            },
        )

        g = self.graph.get_context(graphuri)
        self.assertEqual(
            set(g.triples((None, None, None))),
            set([(michel, likes, pizza)]),
            "only michel likes pizza",
        )
        t1 = time()
        log.debug(f"testUpdateWithInitBindings {self.store}: {t1 - t0:.5f}")

    # @unittest.skip("WIP")
    def testMultipleUpdateWithInitBindings(self):
        t0 = time()
        self.graph.update(
            "INSERT { GRAPH <urn:graph> { ?a ?b ?c . } } WHERE { };"
            "INSERT { GRAPH <urn:graph> { ?d ?b ?c . } } WHERE { }",
            initBindings={
                "a": URIRef("urn:michel"),
                "b": URIRef("urn:likes"),
                "c": URIRef("urn:pizza"),
                "d": URIRef("urn:bob"),
            },
        )

        g = self.graph.get_context(graphuri)
        self.assertEqual(
            set(g.triples((None, None, None))),
            set([(michel, likes, pizza), (bob, likes, pizza)]),
            "michel and bob like pizza",
        )
        t1 = time()
        log.debug(
            f"testMultipleUpdateWithInitBindings {self.store}: {t1 - t0:.5f}"
        )

    # @unittest.skip("WIP")
    def testNamedGraphUpdate(self):
        t0 = time()
        g = self.graph.get_context(graphuri)
        r1 = "INSERT DATA { <urn:michel> <urn:likes> <urn:pizza> }"
        g.update(r1)
        self.assertEqual(
            set(g.triples((None, None, None))),
            set([(michel, likes, pizza)]),
            "only michel likes pizza",
        )

        r2 = (
            "DELETE { <urn:michel> <urn:likes> <urn:pizza> } "
            + "INSERT { <urn:bob> <urn:likes> <urn:pizza> } WHERE {}"
        )
        g.update(r2)
        self.assertEqual(
            set(g.triples((None, None, None))),
            set([(bob, likes, pizza)]),
            "only bob likes pizza",
        )
        says = URIRef("urn:says")

        # Strings with unbalanced curly braces
        tricky_strs = [
            "With an unbalanced curly brace %s " % brace
            for brace in ["{", "}"]
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
        self.assertEqual(values, set(tricky_strs))

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
            [
                "INSERT DATA { <urn:michel> <urn:says> %s } ;" % s
                for s in r4strings
            ]
        )
        g.update(r4)
        values = set()
        for v in g.objects(michel, says):
            values.add(str(v))
        self.assertEqual(
            values,
            set(
                [
                    re.sub(
                        r"\\(.)",
                        r"\1",
                        re.sub(
                            r"^'''|'''$|^'|'$|" + r'^"""|"""$|^"|"$', r"", s
                        ),
                    )
                    for s in r4strings
                ]
            ),
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
        self.assertEqual(
            values, set(["urn:foo'bar?baz;a=1&b=2#fragment", "'}"])
        )

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
        self.assertEqual(values, set([bob, michel]))
        t1 = time()
        log.debug(f"testNamedGraphUpdate {self.store}: {t1 - t0:.5f}")

    # @unittest.skip("WIP")
    def testNamedGraphUpdateWithInitBindings(self):
        t0 = time()
        g = self.graph.get_context(graphuri)
        r = "INSERT { ?a ?b ?c } WHERE {}"
        g.update(r, initBindings={"a": michel, "b": likes, "c": pizza})
        self.assertEqual(
            set(g.triples((None, None, None))),
            set([(michel, likes, pizza)]),
            "only michel likes pizza",
        )
        t1 = time()
        log.debug(
            f"testNamedGraphUpdateWithInitBindings {self.store}: {t1 - t0:.5f}"
        )


class LevelDBStoreTestCase(StoreTestCase, unittest.TestCase):
    store = "LevelDB"

    def setUp(self):
        self.store = "LevelDB"
        # self.path = mktemp(prefix="testleveldb")
        StoreTestCase.setUp(self)


class BerkeleyDBStoreTestCase(StoreTestCase, unittest.TestCase):
    store = "BerkeleyDB"

    def setUp(self):
        try:
            import berkeleydb

            assert berkeleydb
        except Exception:
            return unittest.skip("Skipping BerkeleyDB test, store unavailable")

        self.store = "BerkeleyDB"
        # self.path = mktemp(prefix="testbdb")
        StoreTestCase.setUp(self)


if __name__ == "__main__":
    if False:
        import cProfile

        cProfile.run("unittest.main()", "profile.out")
    else:
        unittest.main()
