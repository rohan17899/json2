import os
import json
import sys
class kvDataStore:
    jsonData={}
    def __init__(self,filePath="E:\\python",dsName="kvDataStore.json"):
        self.filePath=filePath
        self.dsName=dsName
        if os.path.isdir(filePath):
            if os.path.exists(filePath+"\\"+dsName):
                with open(filePath+"\\"+dsName,'r') as dsFile:
                 self.jsondata=json.load(dsFile)
                 print("Datastore initialised")
            else:
              with open(filePath+"\\"+dsName,'w') as dsFile:
                 dsFile.write("{}")
                 self.jsonData={}
        else:
           raise Exception("file does not exist")
     
    def create(self,key,value,ttl=-1):
        self.key=str(key)
        self.value=str(value)
        if(len(key)>32):
            print("given key has more than 32 chars")
        if sys.getsizeof(value)>16384:
            print("Given value is more than 16KB in size")
        for dskey in self.jsonData:
            if dskey==(key):
                print("key already exists")      
        self.jsonData[self.key]=self.value
        with open(self.filePath+"\\"+self.dsName,'w') as dsFile:
            json.dump(self.jsonData,dsFile)
        print("Key -"+self.key+"added successfully")
    
    def read(self,key):
        with open(self.filePath+"\\"+self.dsName,'r') as dsFile:
            self.jsonData=json.load(dsFile)
        for dskey in self.jsonData:
          if dskey==key:
              return self.jsonData[dskey]
        print("Given key not found")       
    
    def delete(self,key):
        keyNotFound=True
        with open(self.filePath+"\\"+self.dsName,'r') as dsFile:
            self.jsonData=json.load(dsFile)
        for dskey in self.jsonData.copy():
           if dskey==key:
            self.jsonData.pop(key,None)
            keyNotFound=False
            print("Given key -"+key+"has been removed")
        if keyNotFound:
            print("Given key -"+key+"not found")
        else:
            with open(self.filePath+"\\"+self.dsName,'w') as dsFile:
                json.dump(self.jsonData,dsFile)
a=kvDataStore("E:\\python")
a.create("key4","valued")
print(a.read("key4"))
a.delete("key4")        


      