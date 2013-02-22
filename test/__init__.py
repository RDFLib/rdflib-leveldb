from rdflib import plugin
from rdflib import store


plugin.register(
        'LevelDB', store.Store,
        'rdflib_leveldb.leveldbstore', 'LevelDBStore')
