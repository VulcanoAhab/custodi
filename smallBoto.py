import boto
import boto.s3.connection

class S3Bucket:
    """
    """
    _conn=None

    @classmethod
    def basic_conn(cls, host, access_key, secret_key):
        """
        """
        cls._conn=boto.connect_s3(
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key,
            host = host,
            calling_format = boto.s3.connection.OrdinaryCallingFormat())


    def __init__(self, bucketName):
        """
        """
        self._btName=bucketName
        self._bt=self._conn.get_bucket(self._btName)
        self._files=(k.name for k in self._bt.list())

    @property
    def files(self):
        """
        """
        return list(self._files)

    @property
    def iterFiles(self):
        """
        """
        return self._files

    @property
    def deleteBucket(self):
        """
        """
        self._bt.delete()

    def getFilesFromDir(self, dirPath):
        """
        """
        return self._bt.list(prefix=dirPath)

    def getFile(self, key):
        """
        """
        return self._bt.get_key(key)

    def deleteFile(self, key):
        """
        """
        self._bt.delete_key(key)

    def deleteFilesFromDir(self, dirPath):
        """
        """
        for key in self._bt.list(prefix=dirPath):key.delete()

    def transferToLocalFile(self, key, localFile):
        """
        """
        content=self._bt.get_key(key)
        if not content:
            print("[-] No content to save locally")
        content.get_contents_to_filename(localFile)
