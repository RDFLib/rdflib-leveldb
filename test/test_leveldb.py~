try:
    import kyotocabinet
except ImportError:
    from nose import SkipTest
    raise SkipTest("KyotoCabinet not installed")

import context_case
import graph_case
import tempfile
from n3_2_case import testN3Store

storename = "KyotoCabinet"
storetest = True
configString = tempfile.mktemp(prefix='test',dir='/tmp')

class KyotoCabinetGraphTestCase(graph_case.GraphTestCase):
    store_name = storename
    path = configString
    storetest = True

class KyotoCabinetContextTestCase(context_case.ContextTestCase):
    store_name = storename
    path = configString
    storetest = True

testN3Store(storename, configString)

