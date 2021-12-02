# -*- coding: utf-8 -*-
import os
import gc
from time import time
from rdflib.namespace import Namespace
from rdflib import Graph, RDF, RDFS, FOAF, OWL
from rdflib.term import URIRef
import cProfile

REDO_FROM_START = True

ukparl = f"""{os.path.join(os.path.dirname(__file__), 'ukparl')}"""
ukppdbpath = os.path.join(ukparl, "ukparl-working")
doacc = f"""{os.path.join(os.path.dirname(__file__), 'doacc')}"""
doaccdbpath = os.path.join(ukparl, "doacc-working")
graph = Graph("LevelDB", URIRef("http://rdflib.net"))

# doacc_tbox = "https://raw.githubusercontent.com/DOACC/doacc/master/doacc.owl"
# doacc_abox = "https://raw.githubusercontent.com/DOACC/individuals/master/cryptocurrency.nt"

doacc_tbox = f"""{os.path.join(doacc, 'doacc.owl')}"""
doacc_abox = f"""{os.path.join(doacc, 'cryptocurrency.nt')}"""


def create_database_from_memorygraph():
    graph.open(ukppdbpath, create=True)
    memgraph = Graph("Memory", URIRef("http://rdflib.net"))

    gcold = gc.isenabled()
    gc.collect()
    gc.disable()

    t0 = time()
    memgraph.parse(
        f"""{os.path.join(ukparl, 'ukparl-tbox.xml')}""", format="xml"
    )
    memgraph.parse(
        f"""{os.path.join(ukparl, 'ukparl-abox.xml')}""", format="xml"
    )
    t1 = time()
    print(f"Parse time: {t1 - t0:.3f}s")  # Parse time: 10.284s

    t0 = time()
    for triple in memgraph.triples((None, None, None)):
        graph.add(triple)
    t1 = time()
    assert len(graph) == 113545, len(graph)
    print(f"Number of triples loaded {len(graph)}")
    memgraph.close()
    graph.close()
    print(f"Add to graph: {t1 - t0:.3f}s")  # Add to graph: 9.481s
    if gcold:
        gc.enable()


def create_database_from_parse():
    graph.open(ukppdbpath, create=True)

    gcold = gc.isenabled()
    gc.collect()
    gc.disable()

    t0 = time()
    graph.parse(f"""{os.path.join(ukparl, 'ukparl-tbox.xml')}""", format="xml")
    graph.parse(f"""{os.path.join(ukparl, 'ukparl-abox.xml')}""", format="xml")
    t1 = time()
    assert len(graph) == 113545, len(graph)
    graph.close()
    print(f"Load into to graph: {t1 - t0:.3f}s")  # Load into to graph: 17.815s
    if gcold:
        gc.enable()


def query_database_from_parse():
    graph.open(ukppdbpath, create=False)

    gcold = gc.isenabled()
    gc.collect()
    gc.disable()

    classquery = """prefix owl: <http://www.w3.org/2002/07/owl#>
prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?class ?label ?description
WHERE {
  ?class a owl:Class.
  OPTIONAL { ?class rdfs:label ?label}
  OPTIONAL { ?class rdfs:comment ?description}
}
"""
    t0 = time()
    r = graph.query(classquery)
    t1 = time()
    print(len(r))
    print(f"Run SPARQL class query: {t1 - t0:.5f}s")  # Run SPARQL query:

    triplesquery = """prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
prefix owl: <http://www.w3.org/2002/07/owl#>

SELECT ?subject ?predicate ?object
WHERE {
  ?subject ?predicate ?object
}
LIMIT 100000
"""
    t0 = time()
    r = graph.query(triplesquery)
    t1 = time()
    print(len(r))
    print(f"Run SPARQL triples query: {t1 - t0:.5f}s")  # Run SPARQL query:
    if gcold:
        gc.enable()


def reload_database():
    gcold = gc.isenabled()
    gc.collect()
    gc.disable()
    t0 = time()
    graph.open(ukppdbpath, create=False)
    t1 = time()
    assert len(graph) == 113545, len(graph)
    # with open(os.path.join(ukparl, "ukparl-working.n3"), "w") as fp:
    #     fp.write(graph.serialize(format="n3"))
    graph.close()
    print(
        f"Read from persistence: {t1 - t0:.3f}s"
    )  # Read from persistence: 0.017s
    if gcold:
        gc.enable()


def query_database():
    graph.open(ukppdbpath, create=False)
    assert len(graph) == 113545, len(graph)

    # ukparl = Namespace(URIRef("http://bel-epa.com/ont/2007/6/ukpp.owl#"))
    # foaf = Namespace(
    #     URIRef("http://daml.umbc.edu/ontologies/cobra/0.4/foaf-basic#")
    # )
    # graph.bind("ukparl", str(ukparl))

    # things = {}
    # for subj, pred, obj in graph.triples((None, RDF.type, None)):
    #     if "ukpp" in obj:
    #         thing = obj.n3().split("#")[-1][:-1]
    #         if obj in things:
    #             things[thing] += 1
    #         else:
    #             things[thing] = 1

    # print(pformat(sorted(list(things.keys())), compact=True))

    # ['Area', 'Constituency', 'Department', 'HouseOfCommons', 'HouseOfLords',
    #  'LordOfParliament', 'LordOfParliamentRole', 'MemberOfParliament',
    #  'MemberOfParliamentRole', 'ParliamentaryRole', 'PartyAffiliation',
    #  'Region', 'UKGBNIParliament', 'UKParliament', 'UKPoliticalParty']

    # things = {}
    # for pred in graph.predicates():
    #     if "ukpp" in pred:
    #         thing = pred.n3().split("#")[-1][:-1]
    #         if pred in things:
    #             things[thing] += 1
    #         else:
    #             things[thing] = 1

    # print(pformat(sorted(list(things.keys())), compact=True))

    # ['abolitionDate', 'abolitionFromDate', 'abolitionToDate', 'area', 'assembled',
    #  'context', 'country', 'countyName', 'dbpediaEntry', 'dissolved', 'duration',
    #  'elected', 'end', 'endingDate', 'establishedDate', 'familyName', 'foreNames',
    #  'foreNamesInFull', 'fromDate', 'fromWhy', 'givenName', 'hasConstituency',
    #  'hasMemberOfParliament', 'lordName', 'lordOfName', 'lordOfNameInFull',
    #  'majorityInSeat', 'name', 'note', 'number', 'parliament_number',
    #  'parliamentaryRole', 'party', 'partyAffiliation', 'peerageType',
    #  'prime_minister', 'region', 'reign', 'roleTaken', 'sessions', 'speaker',
    #  'start', 'startingDate', 'summoned', 'swingToLoseSeat', 'toDate', 'toWhy',
    #  'wikipediaEntry']

    for s, o in list(
        set(
            graph.subject_objects(
                predicate=URIRef(
                    "http://bel-epa.com/ont/2007/6/ukpp.owl#familyName"
                )
                # predicate=ukparl.party
            )
        )
    )[:12]:
        print(f"{s.n3(), o.value}")

    # for s, p, o in graph.triples((None, OWL.Subclass, ukparl.party)):
    #     print(f"{s} is a party")

    # for s, p, o in graph.triples(
    #     (
    #         URIRef("http://bel-epa.com/ont/2007/6/ukpp.owl#ukpp-member-1"),
    #         RDF.type,
    #         ukparl.MemberOfParliament,
    #     )
    # ):
    #     print(f"{s} is a person")

    # for s, p, o in graph.triples(
    #     (
    #         URIRef("http://bel-epa.com/ont/2007/6/ukpp.owl#ukpp-member-1"),
    #         DFOAF.name,  # RDFS.label,
    #         None,
    #     )
    # ):
    #     print(f"{o}")

    graph.close()


def read_doacc_into_memory_and_create_database():
    graph.open(doaccdbpath, create=True)
    memgraph = Graph("Memory", URIRef("http://rdflib.net"))

    gcold = gc.isenabled()
    gc.collect()
    gc.disable()

    t0 = time()
    memgraph.parse(doacc_tbox, format="xml")
    memgraph.parse(doacc_abox, format="nt")
    t1 = time()
    print(f"Parse time: {t1 - t0:.3f}s")  # Parse time: 4.796s

    t0 = time()
    for triple in memgraph.triples((None, None, None)):
        graph.add(triple)
    t1 = time()
    assert len(graph) == 45498, len(graph)
    print(f"no of doacc triples {len(graph)}")
    memgraph.close()
    graph.close()
    print(f"Add to graph: {t1 - t0:.3f}s")  # Add to graph: 2.896s
    if gcold:
        gc.enable()


def read_doacc():
    graph.open(doaccdbpath, create=True)

    gcold = gc.isenabled()
    gc.collect()
    gc.disable()

    t0 = time()
    graph.parse(doacc_tbox, format="xml")
    graph.parse(doacc_abox, format="xml")
    t1 = time()
    # assert len(graph) == 113545, len(graph)
    print(f"no of doacc triples {len(graph)}")
    graph.close()
    print(f"Load into to graph: {t1 - t0:.3f}s")  # Load into to graph: 17.815s
    if gcold:
        gc.enable()


def show_doacc():
    from rdflib.extras.visualizegraph import visualize_graph

    graph = Graph()
    graph.parse(doacc_tbox, format="xml")
    visualize_graph(graph, "DOACC", shortMode=True, format1="png")


def show_ukparl():
    from rdflib.extras.visualizegraph import visualize_graph

    graph = Graph()
    graph.parse(os.path.join(ukparl, "ukparl-tbox.xml"), format="xml")
    visualize_graph(graph, "DOACC", shortMode=True, format1="png")


if __name__ == "__main__":
    if REDO_FROM_START is True:
        import shutil

        if not os.path.exists(ukppdbpath):
            create_database_from_memorygraph()

        reload_database()
        shutil.rmtree(ukppdbpath)

        # with cProfile.Profile() as pr:
        #     create_leveldb_database_from_parse()
        # pr.print_stats()

    # reload_database()
    # query_database()
    # read_doacc_into_memory_and_create_database()
    # show_ukparl()
    # with cProfile.Profile() as pr:
    #     # create_database_from_parse()
    #     query_database_from_parse()
    # pr.print_stats()
