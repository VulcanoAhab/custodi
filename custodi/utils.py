import os
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

    def __del__(self):
        """
        """
        self.zipis.close()