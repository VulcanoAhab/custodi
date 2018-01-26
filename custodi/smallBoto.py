from boto3.session import Session
from botocore.exceptions import ClientError, WaiterError


class BasicSession:
    """
    """
    _resource=None
    _region_name="us-west-2"

    @classmethod
    def set_region(cls, region_name):
        """
        """
        cls._region_name=region_name

    @classmethod
    def basic_conn(cls, access_key, secret_key, 
                        use_ssl=True, region_name=None):
        """
        """
        if not region_name:
            region_name=cls._region_name
        session=Session(aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key)
        setattr(cls, cls._resource, session.client(cls._resource, 
                                                    use_ssl=use_ssl, 
                                                    region_name=region_name))


class S3Bucket(BasicSession):
    """
    """
    _resource="s3"

    def __init__(self, bucketName, basePath=None):
        """
        """
        self._bucketName=bucketName
        try:
            self.s3.head_bucket(Bucket=self._bucketName)
        except ClientError:
            self.s3.create_bucket(Bucket=self._bucketName)
        paginator = cls.s3.get_paginator("list_objects_v2")
        if not basePath:
        
            pages = paginator.paginate(Bucket=self._bucketName)
        else:
            pages = paginator.paginate(Bucket=self._bucketName, Prefix=basePath)
        self._files=(item["Key"] for item in pages.search("Contents"))
        
    @property
    def files(self):
        """
        """
        return self._files

    @property
    def nextFile(self):
        """
        """
        return next(self._files)

    @property
    def deleteBucket(self):
        """
        """
        self.s3.delete_bucket(Bucket=self._bucketName)

    def getFilesFromDir(self, dirPath):
        """
        """
        return self.s3.list_objects(Prefix=dirPath)

    def getFile(self, key):
        """
        """
        return self.s3.get_object(Bucket=self._bucketName, Key=key)

    def deleteFile(self, key):
        """
        """
        self.s3.delete_object(Bucket=self._bucketName, Key=key)

    def deleteFilesFromDir(self, dirPath):
        """
        """
        self.s3.delete_objects(Bucket=self._bucketName, Prefix=dirPath)

    def transferToLocalFile(self, key, localFile):
        """
        """
        fd=open(localFile, "w")
        self.s3.download_fileobj(self._bucketName, key, fd)
        fd.close()

    def uploadFileData(self, fileData, key):
        """
        """
        self.s3.upload_fileobj(fileData, self._bucketName, key)



class Ec2ByName(BasicSession):
    """
    """
    _resource="ec2"
    
    def __init__(self, instance_name):
        """
        """
        self.instance_name=instance_name

    def exists(self):
        """
        """
        filters = [{  
            "Name": "tag:Name",
            "Values": [self.instance_name,]
         }]
        waiter=self.ec2.get_waiter("instance_exists") 
        waiter.config.delay=3
        waiter.config.max_attempts=1
        #wait
        try:
            waiter.wait(Filters=filters)
        except WaiterError:
            return False
        return True

    def create_instance(self, **kwargs):
        """
        image i.e. ami-aa5ebdd2

        """
        instances = self.ec2.run_instances(**kwargs)
        instance=instances["Instances"][0]
        #get id
        insID=instance["InstanceId"]
        #wait set up
        waiter=self.ec2.get_waiter("instance_exists") 
        waiter.config.delay=10
        waiter.config.max_attempts=10
        #wait
        print("[+] Waiting on instance...")
        waiter.wait(InstanceIds=[insID])

        print("[+] Adding Tag Name: {}".format(self.instance_name))
        bc2.ec2.create_tags(
            Resources=[insID],
            Tags=[
                {
                    "Key": "Name",
                    "Value": self.instance_name,
                }
            ]
        )
        print("[+] Instance {} is ready.".format(self.instance_name))
        return instance
    
