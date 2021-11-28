try:
    import plyvel

    assert plyvel
except ImportError:
    from nose import SkipTest

    raise SkipTest("LevelDB not installed")

# import unittest
from . import context_case
from . import graph_case
import tempfile
import os

storename = "LevelDB"
storetest = True
configString = os.path.join(tempfile.gettempdir(), "test_leveldb")


# @unittest.skip("WIP")
class LevelDBGraphTestCase(graph_case.GraphTestCase):
    store_name = storename
    path = configString
    storetest = True


# @unittest.skip("WIP")
class LevelDBContextTestCase(context_case.ContextTestCase):
    store_name = storename
    path = configString
    storetest = True
