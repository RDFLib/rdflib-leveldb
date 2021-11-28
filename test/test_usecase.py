# -*- coding: utf-8 -*-
import unittest
import tempfile
import os
from rdflib import URIRef
from rdflib.graph import Graph


class LevelDBStoreTestCase(unittest.TestCase):
    path = os.path.join(tempfile.gettempdir(), "test_leveldb")

    def test_create(self):
        g = Graph("LevelDB", URIRef("http://rdflib.net"))
        g.open(self.path, create=True)
        assert repr(g.identifier) == "rdflib.term.URIRef('http://rdflib.net')"
        assert (
            str(g)
            == "<http://rdflib.net> a rdfg:Graph;rdflib:storage [a rdflib:Store;rdfs:label 'LevelDBStore']."
        )
        g.close()
        g.destroy(configuration=self.path)

    def test_reuse(self):
        g = Graph("LevelDB", URIRef("http://rdflib.net"))
        g.open(self.path, create=True)
        assert repr(g.identifier) == "rdflib.term.URIRef('http://rdflib.net')"
        assert (
            str(g)
            == "<http://rdflib.net> a rdfg:Graph;rdflib:storage [a rdflib:Store;rdfs:label 'LevelDBStore']."
        )
        g.close()

        g = Graph("LevelDB", URIRef("http://rdflib.net"))
        g.open(self.path, create=False)
        assert repr(g.identifier) == "rdflib.term.URIRef('http://rdflib.net')"
        assert (
            str(g)
            == "<http://rdflib.net> a rdfg:Graph;rdflib:storage [a rdflib:Store;rdfs:label 'LevelDBStore']."
        )
        g.close()
        g.destroy(configuration=self.path)

    def test_example(self):
        g = Graph("LevelDB", URIRef("http://rdflib.net"))
        g.open(self.path, create=True)
        # Parse in an RDF file hosted on the Internet
        g.parse("http://www.w3.org/People/Berners-Lee/card")

        # Loop through each triple in the graph (subj, pred, obj)
        for subj, pred, obj in g:
            # Check if there is at least one triple in the Graph
            if (subj, pred, obj) not in g:
                raise Exception("It better be!")

        assert len(g) == 86, len(g)

        # Print out the entire Graph in the RDF Turtle format
        # print(g.serialize(format="turtle"))
        g.close()
        g.destroy(configuration=self.path)


if __name__ == "__main__":
    unittest.main()
