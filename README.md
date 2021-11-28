# A leveldb-backed persistence plugin store for RDFLib

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black) ![Validation: install and test](https://github.com/RDFLib/rdflib-leveldb/actions/workflows/validate.yaml/badge.svg) [action](https://github.com/RDFLib/rdflib-leveldb/actions/workflows/validate.yaml)

An adaptation of RDFLib BerkeleyDB Store’s key-value approach, using LevelDB as a back-end.

Implemented by Gunnar Grimnes, based on an original contribution by Drew Perttula.

Migrated to Python 3 by Graham Higgins.

Example usage:

```python
from rdflib import plugin, Graph, URIRef
from rdflib.store import Store
import tempfile
import os


def example():
    path = os.path.join(tempfile.gettempdir(), "testleveldb")
    store = plugin.get("LevelDB", Store)(identifier=URIRef("rdflib_leveldb_test"))

    g = Graph(store)
    g.open(path, create=True)

    # Parse in an RDF file hosted on the Internet
    g.parse("http://www.w3.org/People/Berners-Lee/card")

    # Loop through each triple in the graph (subj, pred, obj)
    for subj, pred, obj in g:
        # Check if there is at least one triple in the Graph
        if (subj, pred, obj) not in g:
            raise Exception("It better be!")
    assert len(g) == 86, len(g)
    g.close()

    g.destroy(configuration=path)
```

## Dependencies:

### Linux

The implementation of the rdflib-leveldb “LevelDB” Store depends on:

1. The C++ [leveldb library](https://github.com/google/leveldb/)
2. The [Plyvel](https://pypi.org/project/plyvel/) Python-to-leveldb interface.

The leveldb library is installed using the appropriate package manager.

`sudo apt install leveldb-dev`

### Windows / MacOS
The implementation of the rdflib-leveldb “LevelDB” Store depends on a
Python wheels package [plyvel-wheels](https://github.com/AustEcon/plyvel-wheels)
which includes platform-specific binaries for the leveldb library.


The task of installing a platform-specific `Plyvel` wrapper is handled with:

`pip install -r requirements.txt` (for standard use of this RDFLib Store)

or

`pip install -r requirements.dev.txt` (for module development)
