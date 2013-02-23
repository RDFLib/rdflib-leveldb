import unittest
import tempfile
import os
import shutil
import rdflib
from rdflib_leveldb.leveldbstore import readable_index, NoopMethods
from rdflib.graph import Graph, ConjunctiveGraph
storename = "LevelDB"
storetest = True
configString = tempfile.mktemp(prefix='test', dir='/tmp')


class TestLevelDBGraphCore(unittest.TestCase):

    def setUp(self):
        store = "LevelDB"
        self.graph = Graph(store=store)
        self.path = configString
        self.graph.open(self.path, create=True)

    def tearDown(self):
        self.graph.destroy(self.path)
        try:
            self.graph.close()
        except:
            pass
        if getattr(self, 'path', False) and self.path is not None:
            if os.path.exists(self.path):
                if os.path.isdir(self.path):
                    shutil.rmtree(self.path)
                elif len(self.path.split(':')) == 1:
                    os.unlink(self.path)
                else:
                    os.remove(self.path)

    def test_namespaces(self):
        self.graph.bind("dc", "http://http://purl.org/dc/elements/1.1/")
        self.graph.bind("foaf", "http://xmlns.com/foaf/0.1/")
        self.assert_(len(list(self.graph.namespaces())) == 5)
        self.assert_(
            ('foaf', rdflib.term.URIRef(u'http://xmlns.com/foaf/0.1/'))
            in list(self.graph.namespaces()))

    def test_readable_index(self):
        print(readable_index(111))

    def test_create_db(self):
        michel = rdflib.URIRef(u'michel')
        likes = rdflib.URIRef(u'likes')
        pizza = rdflib.URIRef(u'pizza')
        cheese = rdflib.URIRef(u'cheese')
        self.graph.add((michel, likes, pizza))
        self.graph.add((michel, likes, cheese))
        self.graph.commit()
        self.graph.store.close()
        if getattr(self, 'path', False) and self.path is not None:
            if os.path.exists(self.path):
                if os.path.isdir(self.path):
                    shutil.rmtree(self.path)
                elif len(self.path.split(':')) == 1:
                    os.unlink(self.path)
                else:
                    os.remove(self.path)
        self.graph.store.open(self.path, create=True)
        ntriples = self.graph.triples((None, None, None))
        self.assert_(len(list(ntriples)) == 0)

    def test_missing_db_exception(self):
        self.graph.store.close()
        if getattr(self, 'path', False) and self.path is not None:
            if os.path.exists(self.path):
                if os.path.isdir(self.path):
                    shutil.rmtree(self.path)
                elif len(self.path.split(':')) == 1:
                    os.unlink(self.path)
                else:
                    os.remove(self.path)
        self.graph.store.open(self.path, create=True)
        ntriples = self.graph.triples((None, None, None))
        self.assert_(len(list(ntriples)) == 0)

    def test_reopening_db(self):
        michel = rdflib.URIRef(u'michel')
        likes = rdflib.URIRef(u'likes')
        pizza = rdflib.URIRef(u'pizza')
        cheese = rdflib.URIRef(u'cheese')
        self.graph.add((michel, likes, pizza))
        self.graph.add((michel, likes, cheese))
        self.graph.commit()
        self.graph.store.close()
        self.graph.store.open(self.path, create=False)
        ntriples = self.graph.triples((None, None, None))
        self.assert_(len(list(ntriples)) == 2)

    def test_reopening_missing_db(self):
        self.graph.store.close()
        self.assertRaises(ValueError, self.graph.store.open, (
            '/tmp/NotAnExistingDB'), create=False)

    def test_isopen_db(self):
        self.assert_(self.graph.store.is_open() == True)
        self.graph.store.close()
        self.assert_(self.graph.store.is_open() == False)


class TestLevelDBConjunctiveGraphCore(unittest.TestCase):
    def setUp(self):
        store = "LevelDB"
        self.graph = ConjunctiveGraph(store=store)
        self.path = configString
        self.graph.open(self.path, create=True)

    def tearDown(self):
        self.graph.destroy(self.path)
        try:
            self.graph.close()
        except:
            pass
        if getattr(self, 'path', False) and self.path is not None:
            if os.path.exists(self.path):
                if os.path.isdir(self.path):
                    for f in os.listdir(self.path):
                        os.unlink(self.path + '/' + f)
                    os.rmdir(self.path)
                elif len(self.path.split(':')) == 1:
                    os.unlink(self.path)
                else:
                    os.remove(self.path)

    def test_namespaces(self):
        self.graph.bind("dc", "http://http://purl.org/dc/elements/1.1/")
        self.graph.bind("foaf", "http://xmlns.com/foaf/0.1/")
        self.assert_(len(list(self.graph.namespaces())) == 5)
        self.assert_(
            ('foaf', rdflib.term.URIRef(u'http://xmlns.com/foaf/0.1/'))
            in list(self.graph.namespaces()))

    def test_readable_index(self):
        print(readable_index(111))

    def test_triples_context_reset(self):
        michel = rdflib.URIRef(u'michel')
        likes = rdflib.URIRef(u'likes')
        pizza = rdflib.URIRef(u'pizza')
        cheese = rdflib.URIRef(u'cheese')
        self.graph.add((michel, likes, pizza))
        self.graph.add((michel, likes, cheese))
        self.graph.commit()
        ntriples = self.graph.triples(
            (None, None, None), context=self.graph.store)
        self.assert_(len(list(ntriples)) == 2)

    def test_remove_context_reset(self):
        michel = rdflib.URIRef(u'michel')
        likes = rdflib.URIRef(u'likes')
        pizza = rdflib.URIRef(u'pizza')
        cheese = rdflib.URIRef(u'cheese')
        self.graph.add((michel, likes, pizza))
        self.graph.add((michel, likes, cheese))
        self.graph.commit()
        self.graph.store.remove((michel, likes, cheese), self.graph.store)
        self.graph.commit()
        self.assert_(len(list(self.graph.triples(
            (None, None, None), context=self.graph.store))) == 1)

    def test_remove_db_exception(self):
        michel = rdflib.URIRef(u'michel')
        likes = rdflib.URIRef(u'likes')
        pizza = rdflib.URIRef(u'pizza')
        cheese = rdflib.URIRef(u'cheese')
        self.graph.add((michel, likes, pizza))
        self.graph.add((michel, likes, cheese))
        self.graph.commit()
        self.graph.store.__len__(context=self.graph.store)
        self.assert_(len(list(
            self.graph.triples(
                (None, None, None), context=self.graph.store))) == 2)


def test_NoopMethods():
    obj = NoopMethods()
    res = obj.__getattr__('amethod')
    assert res() is None
