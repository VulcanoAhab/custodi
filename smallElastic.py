from elasticsearch import Elasticsearch

class Basics:
    """
    obs
    ---
    index and docType can be set
    by class or by each action method
    """

    _conn=None
    _index=None
    _docType=None

    @classmethod
    def setConn(cls, *args, **kwargs):
        """
        connection options
        ------------------
        elasticsearch documentation
        """
        cls._conn=Elasticsearch(*args, **kwargs)

    @classmethod
    def setIndex(cls, indexString):
        """
        """
        cls._index=indexString

    @classmethod
    def setDocType(cls, docTypeString):
        """
        """
        cls._docType=docTypeString

    @classmethod
    def save(cls, doc_id, doc, index=None, docType=None):
        """
        """
        if not index:
            index=cls._index
        if not docType:
            docType=cls._docType
        self._conn.index(index=index,doc_type=docType,
                        id=doc_id,body=doc)
    @classmethod
    def search(cls, query, index=None, docType=None):
        """
        """
        return self._conn.search(index=index, doc_type=docType, body=query)

    @classmethod
    def delete(cls, doc_id, index=None, docType=None):
        """
        """
        return self._conn.delete(index=index, doc_type=docType, id=doc_id)

    @classmethod
    def deleteByMatch(cls, macthQuery, index=None, docType=None):
        """
        """
        return self._conn.delete_by_query(index=index,
                                          doc_type=docType,
                                          body={'query':{"match":macthQuery}})

    @classmethod
    def deleteAllbyIndex(cls, index=None, docType=None):
        """
        """
        return self._conn.delete_by_query(index=index, doc_type=docType,
                                          body={'query':{"match_all":{}}})

    @classmethod
    def getAllbyIndex(cls, index=None):
        """
        """
        _all = self._conn.search(index=index, body={"query": {"match_all": {}}})
        _total=_all['hits']['total']
        _count=0
        while True:
            for doc in _all['hits']['hits']:yield doc
            if _count >=_total:break
            _all = self._conn.search(index=index,
                                     body={"query": {"match_all": {}}},
                                     from_=_count+1)

            _snaps=_all['hits']['hits']
            if not _snaps:break
