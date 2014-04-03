from ExecProcessor import *
from FolderCleaner import *
from UploadProcessor import *

class AvalonServer:
    def __init__(self, uploadThreds = 1, execThreads = 1):
        self.uploadThreadNum = uploadThreds
        self.execThreadsNum = execThreads
    def serve(self):
        tUpload = UploadProcessor();
        tUpload.start()
        
        tExecProcessor = ExecProcessor()
        tExecProcessor.start()
        
        tFolderCleaner = FolderCleaner()
        tFolderCleaner.start()

def startServer():
    myServer = AvalonServer();
    myServer.serve()

if __name__ == "__main__":
    startServer()
        