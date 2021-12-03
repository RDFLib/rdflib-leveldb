# -*- coding: utf-8 -*-
import unittest
import gc
import os
import logging
from time import time
import tempfile
from rdflib import Graph

logging.basicConfig(level=logging.ERROR, format="%(message)s")
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


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

        self.graph = Graph(store=self.store)

        self.path = os.path.join(
            tempfile.gettempdir(), f"test_{self.store.lower()}"
        )
        self.graph.open(self.path, create=True)
        self.input = Graph()

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

    def testTime(self):
        fixturelist = {
            "500triples": 691,
            "1ktriples": 1285,
            "2ktriples": 2006,
            "3ktriples": 3095,
            "5ktriples": 5223,
            "10ktriples": 10303,
            "25ktriples": 25161,
            "50ktriples": 50168,
        }
        log.debug(f"{self.store}: ")
        for i in fixturelist.keys():
            inputloc = os.getcwd() + f"/test/sp2b/{i}.n3"
            # Clean up graphs so that BNodes in input data
            # won't create random results
            self.input = Graph()
            self.graph.remove((None, None, None))

            res = self._testInput(inputloc)

            log.debug(f"Loaded {len(self.graph):5d} triples in {res.strip()}s")

            self.assertEqual(len(self.graph), fixturelist[i], len(self.graph))

        # Read triples back into memory from store
        self.graph.close()
        self.graph.open(self.path, create=False)

        t0 = time()
        for _i in self.graph.triples((None, None, None)):
            pass

        t1 = time()
        log.debug(f"Re-reading: {t1 - t0:.3f}s")

        self.assertEqual(
            len(self.graph), sorted(fixturelist.values())[-1], len(self.graph)
        )

        # Delete the store by removing triples
        t0 = time()
        self.graph.remove((None, None, None))
        self.assertEqual(len(self.graph), 0)
        t1 = time()
        log.debug(f"Deleting  : {t1 - t0:.3f}s")

    def _testInput(self, inputloc):
        # number = 1
        store = self.graph
        self.input.parse(location=inputloc, format="n3")

        # def add_from_input():
        #     for t in self.input:
        #         store.add(t)

        # it = itertools.repeat(None, number)
        t0 = time()
        # for _i in it:
        #     add_from_input()
        #     for s in store.subjects(RDF.type, None):
        #         for t in store.triples((s, None, None)):
        #             pass

        store.addN(tuple(t) + (store,) for t in self.input)
        t1 = time()
        return f"{t1 - t0:.3f}"


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
