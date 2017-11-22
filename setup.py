from distutils.core import setup

setup(
    name="custodi",
    version='0.1.0',
    author="VulcanoAhab",
    packages=["custodi"],
    url="https://github.com/VulcanoAhab/custodi.git",
    description="Simple high level API's for Elastic Search, S3 and others",
    install_requires=[
        "boto3==1.4.7",
        "elasticsearch==5.4.0",
        ]
)
