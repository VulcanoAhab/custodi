import os
import io
import json
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
        paginator = self.s3.get_paginator("list_objects_v2")
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

    def uploadJson(self, jsonObj, key):
        """
        """
        obj_dump=json.dumps(jsonObj)
        content=io.BytesIO()
        content.write(obj_dump.encode())
        content.seek(0)
        self.uploadFileData(content, key)



class Ec2ByName(BasicSession):
    """
    """
    _resource="ec2"

    def __init__(self, instance_name):
        """
        """
        self.instance_name=instance_name
        self.instance={}

    def exists(self):
        """
        """
        filters = [{
            "Name": "tag:Name",
            "Values": [self.instance_name,]
         }]
        reservations=self.ec2.describe_instances(Filters=filters)
        if not reservations or not reservations["Reservations"]:return False
        instances=reservations["Reservations"][0]["Instances"]
        if not instances:return False
        print("[+] Found {} instance(s).".format(len(instances)))
        self.instance=instances[0]
        return True

    def create_instance(self, **kwargs):
        """
        image i.e. ami-55ef662f

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
        self.ec2.create_tags(
            Resources=[insID],
            Tags=[
                {
                    "Key": "Name",
                    "Value": self.instance_name,
                }
            ]
        )
        print("[+] Instance {} is ready.".format(self.instance_name))
        self.instance=instance

    @property
    def publicIP(self):
        """
        """
        return self.instance.get("PublicIpAddress")


class LambdaByName(BasicSession):
    """
    """
    _resource="lambda"

    def __init__(self, lambda_name):
        """
        """
        self.lambda_name=lambda_name
        self.confs={}

    def set_confs(self, **confs):
        """
        """
        self.confs.update(confs)

    def set_zip_content(self, zip_content):
        """
        """
        self.zip_content=zip_content

    def create_lambda(self):
        """
        """
        self._lambda.create_function(**self.confs)
        self._lambda.update_function_code(
            FunctionName=self.lambda_name,
            ZipFile=self.zip_content,
            Publish=True
        )


class RDSPostgre(BasicSession):
    """
    """
    _resource="rds"

    def __init__(self, dataBase):
        """
        """
        self.dataBase=dataBase
        self.confs={}

    def create_db(self, username, passWord):
        """
        """
        try:
            self.rds.create_db_instance(
                DBInstanceIdentifier=db_identifier,
                AllocatedStorage=200,
               DBName=self.dataBase,
               Engine='postgres',
               # General purpose SSD
               StorageType='gp2',
               StorageEncrypted=True,
               AutoMinorVersionUpgrade=True,
               # Set this to true later?
               MultiAZ=False,
               MasterUsername=username,
               MasterUserPassword=passWord,
               VpcSecurityGroupIds=[self.confs["securityGroup"],]
               DBInstanceClass=self.confs["dbType"],
               Tags=self.confs["tags"]
               )
            print ("Starting RDS instance with ID: {}".format(db_identifier))
    except botocore.exceptions.ClientError as e:
        if 'DBInstanceAlreadyExists' in e.message:
            print 'DB instance %s exists already, continuing to poll ...' % db_identifier
        else:
            raise
