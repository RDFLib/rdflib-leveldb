try:
    import rdflib_leveldb.leveldb
except ImportError:
    from nose import SkipTest
    raise SkipTest("LevelDB not installed")

import context_case
import graph_case
import tempfile
from n3_2_case import testN3Store

storename = "LevelDB"
storetest = True
configString = tempfile.mktemp(prefix='test',dir='/tmp')

class LevelDBGraphTestCase(graph_case.GraphTestCase):
    store_name = storename
    path = configString
    storetest = True

class LevelDBContextTestCase(context_case.ContextTestCase):
    store_name = storename
    path = configString
    storetest = True

testN3Store(storename, configString)

