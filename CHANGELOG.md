2021/11/16 RELEASE 0.2
======================
- Migrated to Python 3, dropped support for Python 2.
- Removed LRU implementation (leveldb uses its own)
- Migrated the remaining Python-coded LFU cache to `__init__.py` 
- Removed the (unused) Cython Picklr extension (Python3
  uses cPickle if available)
- Added some additional tests
- Added some documentation
- Cargo-culted RDFLib's devops config files (e.g. `drone.yml`, `black.toml`)
- Cloned RDFLib's `berkeleydb_example.py` as `leveldb_example.py` 

2021/11/16 RELEASE 0.1
======================

Based on Drew Perttula's original `TokyoCabinet Store` contribution.
And then a Kyoto Cabinet version by Graham Higgins.
And this one by Gunnar Grimnes.
