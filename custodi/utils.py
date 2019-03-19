import os
import json
import zipfile

class Ziping:
    """
    """
    def __init__(self, zipFile):
        """
        """
        self.zipis=zipfile.ZipFile(zipFile, "w", zipfile.ZIP_DEFLATED)

    def add_file(self, filePath):
        """
        """
        self.zipis.write(filePath)

    def add_files(self, filesList):
        """
        """
        for filePath in filesList:
            self.add_file(filePath)

    def add_content(self, filePath, content):
        """
        """
        self.zipis.writestr(filePath, content)

    def __del__(self):
        """
        """
        self.zipis.close()

def _save_bucket(outputFile, bucket):
    """
    """
    zipis=Ziping(outputFile+".zip")
    dump_str=json.dumps(bucket)
    zipis.add_content(outputFile+".txt", dump_str)
