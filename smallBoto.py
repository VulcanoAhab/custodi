from boto3.session import Session


class S3Bucket:
    """
    """
    _conn=None

    @classmethod
    def basic_conn(cls, access_key, secret_key, use_ssl=True):
        """
        """
        session=Session(aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key)
        self._s3=session.resource("s3", use_ssl=use_ssl)

    def __init__(self, bucketName):
        """
        """
        self._btName=bucketName
        if (not self._s3.Bucket(self.bucketName)
            in list(self._s3.buckets.all())):
            self._s3.create_bucket(Bucket=self._btName)
        self._bt=self._s3.Bucket(self.bucketName)
        self._files=self._bt.objects.all()

    @property
    def size(self):
        """
        """
        return self._bt.size

    @property
    def files(self):
        """
        """
        return [obj.key for obj in self._files]

    @property
    def deleteBucket(self):
        """
        """
        self._bt.delete()

    def getFilesFromDir(self, dirPath):
        """
        """
        return self._bt.objects.filter(Prefix=dirPath)

    def getFile(self, key):
        """
        """
        return self._bt.Object(key).get()

    def deleteFile(self, key):
        """
        """
        self._bt.Object(key).delete()

    def deleteFilesFromDir(self, dirPath):
        """
        """
        self._bt.delete_objects()

    def transferToLocalFile(self, key, localFile):
        """
        """
        self._bt.download_file(key, localFile)

    def uploadFileData(self, fileData, key):
        """
        """
        self._bt.upload_fileobj(fileData, key)
