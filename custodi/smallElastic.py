from aws_requests_auth.aws_auth import AWSRequestsAuth
from elasticsearch import Elasticsearch,RequestsHttpConnection

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
        try:
            cls._conn=Elasticsearch(*args, **kwargs)
        except Exception as e:
            print("[FAIL TO CONNECT TO ES] :: {}".format(e))
            exit(3)

    @classmethod
    def setAWsConn(cls, aws_key, aws_secret,
                   esEndPoint, esRegion, esService):
        """
        """
        try:
            auth=AWSRequestsAuth(aws_access_key=aws_key,
                       aws_secret_access_key=aws_secret,
                       aws_host=esEndPoint,
                       aws_region=esRegion,
                       aws_service=esService)
            cls._conn=Elasticsearch(
                            hosts=[{"host": esEndPoint, "port": 443}],
                            use_ssl=True,
                            verify_certs=True,
                            connection_class=RequestsHttpConnection,
                            http_auth=auth)
        except Exception as e:
            print("[FAIL TO CONNECT TO ES] :: {}".format(esEndPoint))
            print(e)
            exit(3)

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
    def createIndex(cls, *args, **kwargs):
        """
        """
        #ignore 400 :: if already exists
        if "ignore" not in kwargs:kwargs.update({"ignore":400})
        cls.indices.create(*args, **kwargs) 

    @classmethod
    def save(cls, doc_id, doc, index=None, docType=None):
        """
        """
        if not index:
            index=cls._index
        if not docType:
            docType=cls._docType
        cls._conn.index(index=index,doc_type=docType,
                        id=doc_id,body=doc)
    @classmethod
    def search(cls, query, index=None, docType=None):
        """
        """
        return cls._conn.search(index=index, doc_type=docType, body=query)

    @classmethod
    def scroll(cls, query, index=None, docType=None, scrollTime=None):
        """
        """
        if not scrollTime:
            scrollTime="1m"
        firstHit=cls._conn.search(index=index, doc_type=docType,
                                  body=query, scroll=scrollTime)
        scrollIt=firstHit["_scroll_id"]
        #send first hit
        for doc in firstHit["hits"]["hits"]:yield doc
        #iter the rest
        while True:
            try:
                manyHits=self._conn.scroll(scroll_id=scrollIt,
                                           scroll=scrollTime)
                if not len(manyHits["hits"]["hits"]):break
                for doc in manyHits["hits"]["hits"]:yield doc
                scrollIt=manyHits["_scroll_id"]
            except Exception as e:
                print("[SCROLL FAIL] :: {}".format(e))
                break


    @classmethod
    def delete(cls, doc_id, index=None, docType=None):
        """
        """
        return cls._conn.delete(index=index, doc_type=docType, id=doc_id)

    @classmethod
    def deleteByMatch(cls, macthQuery, index=None, docType=None):
        """
        """
        return cls._conn.delete_by_query(index=index,
                                          doc_type=docType,
                                          body={'query':{"match":macthQuery}})

    @classmethod
    def deleteAllbyIndex(cls, index=None, docType=None):
        """
        """
        return cls._conn.delete_by_query(index=index, doc_type=docType,
                                          body={'query':{"match_all":{}}})

    @classmethod
    def getAllbyIndex(cls, index):
        """
        """
        cls.scroll({"query":{"match_all":{}}}, index=index) 
