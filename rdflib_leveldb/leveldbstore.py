# -*- coding: utf-8 -*-
"""
An adaptation of the BerkeleyDB Store's key-value approach to use LevelDB
as a back-end.

Based on an original contribution by Drew Perttula: `TokyoCabinet Store
<http://bigasterisk.com/darcs/?r=tokyo;a=tree>`_.

and then a Kyoto Cabinet version by Graham Higgins <gjh@bel-epa.com>

this one by Gunnar Grimnes

Subsequently updated to Python3 by Graham Higgins

berkeleydb uses the default API get and put, so has to handle
string-to-bytes conversion in the args provided to every call
on get/put. By using a store-specific _get/_put which takes an
additional "dbname" argument, not only can store-specific
differences in get/put call be coded for but it is also offers
the opportunity to do the string-bytes conversion at the point
of db API and so the calls can be expunged of conversion cruft.

The cost is a difference of model:

Berkeleydb:

# def namespace(self, prefix):
#     prefix = prefix.encode("utf-8")
#     ns = self.__namespace.get(prefix, None)
#     if ns is not None:
#         return URIRef(ns.decode("utf-8"))
#     return None
vs.

# def namespace(self, prefix):
#     ns = _get(self.__namespace, prefix)
#     if ns is not None:
#         return URIRef(ns)
#     return None

There is also a difference in the API w.r.t. accessing a range.
BerkeleyDB takes a cursor-based approach:

# index = self.__indicies[0]
# cursor = index.cursor()
# current = cursor.set_range(prefix)
# count = 0
# while current:
#    key, value = current
#    if key.startswith(prefix):
#        count += 1
#        # Hack to stop 2to3 converting this to next(cursor)
#        current = getattr(cursor, "next")()
#    else:
#        break
# cursor.close()
# return count

whereas Plyvel offers an interator:

# return len([key for key in self.__indices[0].iterator(
#     start=prefix, include_value=False)
#     if key.startswith(prefix)])

"""
import os
import logging
from functools import lru_cache
from rdflib.store import Store, VALID_STORE, NO_STORE
from rdflib.term import URIRef
from urllib.request import pathname2url

try:
    from plyvel import DB as LevelDB

    has_wrapper = True
except ImportError:  # pragma: NO COVER
    has_wrapper = False

logging.basicConfig(level=logging.ERROR, format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class NoopMethods(object):
    def __getattr__(self, methodName):
        return lambda *args: None


__all__ = ["LevelDB"]


class LevelDBStore(Store):
    """\
    A store that allows for on-disk persistent using LevelDB, a fast
    key/value DB.

    This store allows for quads as well as triples. See examples of use
    in both the `examples.leveldb_example` and `test.test_leveldb_store`
    files.

    **NOTE on installation**:

    To use this store, you must have leveldb installed on your system
    separately to Python (`brew install leveldb` on a Mac) and also have
    the Plyvel leveldb Python wrapper installed (`pip install plyvel`).

    Windows users should use the Plyvel-wheels distribution which includes
    Windows-specifc leveldb library binaries: (`pip install plyvel-wheels`).

    """

    context_aware = True
    formula_aware = True
    transaction_aware = False
    graph_aware = True
    db_env = None
    should_create = True

    def __init__(self, configuration=None, identifier=None):
        if not has_wrapper:
            raise ImportError("Unable to import plyvel, store is unusable.")
        self.__open = False
        self._terms = 0
        self.__identifier = identifier
        super(LevelDBStore, self).__init__(configuration)
        self._loads = self.node_pickler.loads
        self._dumps = self.node_pickler.dumps

    def __get_identifier(self):
        return self.__identifier

    identifier = property(__get_identifier)

    def is_open(self):
        return self.__open

    def open(self, path, create=False):
        if not has_wrapper:
            return NO_STORE

        self.should_create = create
        self.path = path

        if self.__identifier is None:
            self.__identifier = URIRef(pathname2url(os.path.abspath(path)))

        # Create a prefixed database
        dbpathname = os.path.abspath(self.path)
        # Help the user to avoid writing over an existing leveldb database
        if self.should_create is True:
            if os.path.exists(dbpathname):
                raise Exception(
                    f"Database file {dbpathname} aready exists, please move or delete it."
                )
            else:
                self.db = LevelDB(
                    dbpathname, create_if_missing=True, error_if_exists=True
                )
        else:
            if not os.path.exists(dbpathname):
                return NO_STORE
            else:
                self.db = LevelDB(
                    dbpathname, create_if_missing=False, error_if_exists=False
                )

        # create and open the DBs
        self.__indices = [
            None,
        ] * 3
        self.__indices_info = [
            None,
        ] * 3
        for i in range(0, 3):
            index_name = to_key_func(i)(
                (
                    "s".encode("latin-1"),
                    "p".encode("latin-1"),
                    "o".encode("latin-1"),
                ),
                "c".encode("latin-1"),
            )
            index = self.db.prefixed_db(index_name)
            self.__indices[i] = index
            self.__indices_info[i] = (index, to_key_func(i), from_key_func(i))

        lookup = {}
        for i in range(0, 8):
            results = []
            for start in range(0, 3):
                score = 1
                len = 0
                for j in range(start, start + 3):
                    if i & (1 << (j % 3)):
                        score = score << 1
                        len += 1
                    else:
                        break
                tie_break = 2 - start
                results.append(((score, tie_break), start, len))

            results.sort()
            score, start, len = results[-1]

            def get_prefix_func(start, end):
                def get_prefix(triple, context):
                    if context is None:
                        yield ""
                    else:
                        yield context
                    i = start
                    while i < end:
                        yield triple[i % 3]
                        i += 1
                    yield ""

                return get_prefix

            lookup[i] = (
                self.__indices[start],
                get_prefix_func(start, start + len),
                from_key_func(start),
                results_from_key_func(start, self._from_string),
            )

        self.__lookup_dict = lookup
        self.__contexts = self.db.prefixed_db(b"contexts")
        self.__namespace = self.db.prefixed_db(b"namespace")
        self.__prefix = self.db.prefixed_db(b"prefix")
        self.__k2i = self.db.prefixed_db(b"k2i")
        self.__i2k = self.db.prefixed_db(b"i2k")

        try:
            self._terms = int(self.__k2i.get(b"__terms__"))
            assert isinstance(self._terms, int)
        except TypeError:
            pass  # new store, no problem

        self.__open = True

        return VALID_STORE

    def dumpdb(self):
        from pprint import pformat

        dbs = {
            "self.__indices": self.__indices,
            "self.__indices_info": self.__indices_info,
            "self.__lookup_dict": self.__lookup_dict,
            "self.__contexts": self.__contexts,
            "self.__namespace": self.__namespace,
            "self.__prefix": self.__prefix,
            "self.__k2i": self.__k2i,
            "self.__i2k": self.__i2k,
        }
        logger.debug("\n**** Dumping database:\n")
        for k, v in dbs.items():
            if isinstance(v, (list, dict)):
                logger.debug(f"{k} {type(v)}:\n{pformat(v, indent=4)}")
            else:
                logger.debug(f"db: {k} {type(v)}")
                for (key, val) in list(v.iterator()):
                    logger.debug(f"\t{key}: {val}")

    def close(self, commit_pending_transaction=False):
        self.__open = False
        # Closing the database also closes the prefixed databases
        self.db.close()

    def destroy(self, configuration=""):
        assert self.__open is False, "The Store must be closed."
        import os

        path = configuration or self.path
        if os.path.exists(path):
            import shutil

            shutil.rmtree(path)

    def add(self, triple, context, quoted=False):
        """
        Add a triple to the store of triples.
        """
        (subject, predicate, object) = triple
        assert self.__open, "The Store must be open."
        assert context != self, "Can not add triple directly to store"
        # Add the triple to the Store, triggering TripleAdded events
        Store.add(self, (subject, predicate, object), context, quoted)

        _to_string = self._to_string

        s = _to_string(subject)
        p = _to_string(predicate)
        o = _to_string(object)
        c = _to_string(context)

        cspo, cpos, cosp = self.__indices

        value = cspo.get(f"{c}^{s}^{p}^{o}^".encode())

        if value is None:
            self.__contexts.put(c.encode(), b"")

            contexts_value = cspo.get(
                f"{''}^{s}^{p}^{o}^".encode()
            ) or "".encode("latin-1")

            contexts = set(contexts_value.split("^".encode("latin-1")))
            contexts.add(c.encode())

            contexts_value = "^".encode("latin-1").join(contexts)
            assert contexts_value is not None

            cspo.put(f"{c}^{s}^{p}^{o}^".encode(), b"")
            cpos.put(f"{c}^{p}^{o}^{s}^".encode(), b"")
            cosp.put(f"{c}^{o}^{s}^{p}^".encode(), b"")
            if not quoted:
                cspo.put(f"^{s}^{p}^{o}^".encode(), contexts_value)
                cpos.put(f"^{p}^{o}^{s}^".encode(), contexts_value)
                cosp.put(f"^{o}^{s}^{p}^".encode(), contexts_value)

            # self.__needs_sync = True

        else:
            pass  # already have this triple, ignoring")

    def __remove(self, spo, c, quoted=False):
        s, p, o = spo
        cspo, cpos, cosp = self.__indices
        contexts_value = (
            cspo.get(
                "^".encode("latin-1").join(
                    ["".encode("latin-1"), s, p, o, "".encode("latin-1")]
                ),
            )
            or "".encode("latin-1")
        )
        contexts = set(contexts_value.split("^".encode("latin-1")))
        contexts.discard(c)
        contexts_value = "^".encode("latin-1").join(contexts)
        for i, _to_key, _from_key in self.__indices_info:
            i.delete(_to_key((s, p, o), c))
        if not quoted:
            if contexts_value:
                for i, _to_key, _from_key in self.__indices_info:
                    i.put(
                        _to_key((s, p, o), "".encode("latin-1")),
                        contexts_value,
                    )

            else:
                for i, _to_key, _from_key in self.__indices_info:
                    try:
                        i.delete(_to_key((s, p, o), "".encode("latin-1")))
                    except Exception:
                        pass  # FIXME okay to ignore these?

    def remove(self, spo, context):
        subject, predicate, object = spo
        assert self.__open, "The Store must be open."
        # Add the triple to the Store, triggering TripleRemoved events
        Store.remove(self, (subject, predicate, object), context)
        _to_string = self._to_string

        if context is not None:
            if context == self:
                context = None

        if (
            subject is not None
            and predicate is not None
            and object is not None
            and context is not None
        ):
            s = _to_string(subject)
            p = _to_string(predicate)
            o = _to_string(object)
            c = _to_string(context)
            value = self.__indices[0].get(f"{c}^{s}^{p}^{o}^".encode())
            if value is not None:
                self.__remove((s.encode(), p.encode(), o.encode()), c.encode())

                # self.__needs_sync = True

        else:
            cspo, cpos, cosp = self.__indices
            index, prefix, from_key, results_from_key = self.__lookup(
                (subject, predicate, object), context
            )
            for key in index.iterator(start=prefix, include_value=False):
                if key.startswith(prefix):
                    c, s, p, o = from_key(key)
                    if context is None:
                        contexts_value = index.get(key) or "".encode("latin-1")
                        # remove triple from all non quoted contexts
                        contexts = set(
                            contexts_value.split("^".encode("latin-1"))
                        )
                        # and from the conjunctive index
                        contexts.add("".encode("latin-1"))
                        for c in contexts:
                            for i, _to_key, _ in self.__indices_info:
                                i.delete(_to_key((s, p, o), c))
                    else:
                        self.__remove((s, p, o), c)
                else:
                    break

            if context is not None:
                if subject is None and predicate is None and object is None:
                    # TODO: also if context becomes empty and not just on
                    # remove((None, None, None), c)
                    try:
                        self.__contexts.delete(_to_string(context).encode())
                    except Exception as e:  # pragma: NO COVER
                        print(
                            "%s, Failed to delete %s" % (e, context)
                        )  # pragma: NO COVER
                        pass  # pragma: NO COVER

            # self.__needs_sync = needs_sync

    def triples(self, spo, context=None):
        """A generator over all the triples matching"""
        assert self.__open, "The Store must be open."

        subject, predicate, object = spo

        if context is not None:
            if context == self:
                context = None

        # _from_string = self._from_string ## UNUSED
        index, prefix, from_key, results_from_key = self.__lookup(
            (subject, predicate, object), context
        )

        for key, value in index.iterator(start=prefix, include_value=True):
            if key.startswith(prefix):
                yield results_from_key(key, subject, predicate, object, value)
            else:
                break

    def __len__(self, context=None):
        assert self.__open, "The Store must be open."
        if context is not None:
            if context == self:
                context = None

        if context is None:
            prefix = "^".encode("latin-1")
        else:
            prefix = f"{self._to_string(context)}^".encode()

        return len(
            [
                key
                for key in self.__indices[0].iterator(
                    start=prefix, include_value=False
                )
                if key.startswith(prefix)
            ]
        )

    def bind(self, prefix, namespace):
        prefix = prefix.encode("utf-8")
        namespace = namespace.encode("utf-8")
        bound_prefix = self.__prefix.get(namespace)
        if bound_prefix:
            self.__namespace.delete(bound_prefix)
        self.__prefix.put(namespace, prefix)
        self.__namespace.put(prefix, namespace)

    def namespace(self, prefix):
        prefix = prefix.encode("utf-8")
        ns = self.__namespace.get(prefix, None)
        if ns is not None:
            return URIRef(ns.decode("utf-8"))
        return None

    def prefix(self, namespace):
        namespace = namespace.encode("utf-8")
        prefix = self.__prefix.get(namespace, None)
        if prefix is not None:
            return prefix.decode("utf-8")
        return None

    def namespaces(self):
        for prefix, namespace in [
            (k.decode(), v.decode())
            for k, v in self.__namespace.iterator(include_value=True)
        ]:
            yield prefix, URIRef(namespace)

    @lru_cache(maxsize=5000)
    def __get_context(self, ident):
        logger.debug(f"get context {ident}")
        return self.__contexts.get(ident, {})
        # return self.db_env.get(ident, {})

    def __set_context(self, ident, g):
        logger.debug(f"set context {ident} for {g}")
        self.__contexts.put(ident.encode(), g)
        # self.db_env[ident] = g

    def contexts(self, triple=None):
        _from_string = self._from_string
        _to_string = self._to_string

        if triple:
            s, p, o = triple
            s = _to_string(s)
            p = _to_string(p)
            o = _to_string(o)
            contexts = self.__indices[0].get(f"^{s}^{p}^{o}^".encode())

            if contexts:
                for c in contexts.split("^".encode("latin-1")):
                    if c:
                        yield _from_string(c)

        else:
            for k in self.__contexts.iterator(include_value=False):
                yield _from_string(k)

    @lru_cache(maxsize=5000)
    def add_graph(self, graph):
        self.__contexts.put(self._to_string(graph).encode(), b"")

    def remove_graph(self, graph):
        self.remove((None, None, None), graph)

    @lru_cache(maxsize=5000)
    def _from_string(self, i):
        """
        rdflib term from index number (as a string)
        """
        k = self.__i2k.get(str(int(i)).encode())
        if k is not None:
            val = self._loads(k)
            return val
        else:
            raise Exception(f"Key for {i} is None")

    @lru_cache(maxsize=5000)
    def _to_string(self, term):
        """
        index number (as a string) from rdflib term
        """
        k = self._dumps(term)
        i = self.__k2i.get(k)

        if i is None:  # (from BdbApi)
            # Does not yet exist, increment refcounter and create
            self._terms += 1
            i = str(self._terms)
            self.__i2k.put(i.encode(), k)
            self.__k2i.put(k, i.encode())
            self.__k2i.put(b"__terms__", str(self._terms).encode())
        else:
            i = i.decode()
        return i

    def __lookup(self, spo, context):
        subject, predicate, object = spo
        _to_string = self._to_string
        if context is not None:
            context = _to_string(context)
        i = 0
        if subject is not None:
            i += 1
            subject = _to_string(subject)
        if predicate is not None:
            i += 2
            predicate = _to_string(predicate)
        if object is not None:
            i += 4
            object = _to_string(object)
        index, prefix_func, from_key, results_from_key = self.__lookup_dict[i]
        # DEBUG
        try:
            prefix = "^".join(
                prefix_func((subject, predicate, object), context)
            ).encode("utf-8")
        except Exception as e:
            raise Exception(
                "{}: {} {} - {} {} - {} {} - {} {}".format(
                    e,
                    subject,
                    type(subject),
                    predicate,
                    type(predicate),
                    object,
                    type(object),
                    context,
                    type(context),
                )
            )
        return index, prefix, from_key, results_from_key


def to_key_func(i):
    def to_key(triple, context):
        "Takes a string; returns key"
        return "^".encode("latin-1").join(
            (
                context,
                triple[i % 3],
                triple[(i + 1) % 3],
                triple[(i + 2) % 3],
                "".encode("latin-1"),
            )
        )  # "" to tac on the trailing ^

    return to_key


def from_key_func(i):
    def from_key(key):
        "Takes a key; returns string"
        parts = key.split("^".encode("latin-1"))
        return (
            parts[0],
            parts[(3 - i + 0) % 3 + 1],
            parts[(3 - i + 1) % 3 + 1],
            parts[(3 - i + 2) % 3 + 1],
        )

    return from_key


def results_from_key_func(i, from_string):
    def from_key(key, subject, predicate, object, contexts_value):
        "Takes a key and subject, predicate, object; returns tuple for yield"
        parts = key.split("^".encode("latin-1"))
        if subject is None:
            # TODO: i & 1: # dis assemble and/or measure to see which is faster
            # subject is None or i & 1
            s = from_string(parts[(3 - i + 0) % 3 + 1])
        else:
            s = subject
        if predicate is None:  # i & 2:
            p = from_string(parts[(3 - i + 1) % 3 + 1])
        else:
            p = predicate
        if object is None:  # i & 4:
            o = from_string(parts[(3 - i + 2) % 3 + 1])
        else:
            o = object
        return (
            (s, p, o),
            (
                from_string(c)
                for c in contexts_value.split("^".encode("latin-1"))
                if c
            ),
        )

    return from_key


def readable_index(i):
    s, p, o = "?" * 3
    if i & 1:
        s = "s"
    if i & 2:
        p = "p"
    if i & 4:
        o = "o"
    return f"{s},{p},{o}"


# # To facilitate TDD :)
# # ====================
# storename = "LevelDB"
# storetest = True
# configString = tempfile.mktemp(prefix='leveldbstoretest')


# @unittest.skip("WIP")
# class LevelDBTDD(unittest.TestCase):
#     def setUp(self):
#         from rdflib import Graph
#         store = "LevelDB"
#         self.graph = Graph(store=store)
#         self.path = configString
#         self.graph.open(self.path, create=True)

#     def tearDown(self):
#         self.graph.close()
#         self.graph.destroy(self.path)

#     def test_namespaces(self):
#         self.graph.bind("dc", "http://http://purl.org/dc/elements/1.1/")
#         self.graph.bind("foaf", "http://xmlns.com/foaf/0.1/")
#         self.assertTrue(len(list(self.graph.namespaces())) == 6)
#         self.assertIn(
#             ('foaf', URIRef(u'http://xmlns.com/foaf/0.1/')),
#             list(self.graph.namespaces()))


# if __name__ == '__main__':
#     unittest.main()
