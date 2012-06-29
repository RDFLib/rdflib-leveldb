from rdflib import URIRef,BNode,Literal,Graph

def loads(object _s, object store):
    cdef char l
    cdef char *s

    __s=_s.decode("utf-8")
    s=__s
    l=s[0]

    if l==c'U':
        return URIRef(s[1:])
    elif l==c'B':
        return BNode(s[1:])
    elif l==c'P':
        return Literal(s[1:])
    elif l==c'D':
        i=s.index("|")
        dt=URIRef(s[1:i])
        return Literal(s[i+1:],datatype=dt)
    elif l==c'L':
        i=s.index("|")
        language=s[1:i]
        return Literal(s[i+1:], language=language)
    elif l==c'G':
        return Graph(store, identifier=s[1:])
    else:
        raise Exception("Type %s not supported!"%l)
        

def dumps(object t):
    if isinstance(t, URIRef):
        res="U%s"%t
    elif isinstance(t, BNode):
        res="B%s"%t
    elif isinstance(t, Literal):
        if t.datatype:
            res="D%s|%s"%(t.datatype, t)
        elif t.language:
            res="L%s|%s"%(t.language, t)
        else:
            res="P%s"%t
    elif isinstance(t, Graph): 
        res="G%s"%t.identifier
    else:
        raise Exception("Type %s not supported!"%type(t))

    return res.encode("utf-8")

