from pymongo import MongoClient
import json

def loadMongoDB(fileName):
    client = MongoClient("mongodb://localhost:27017")
    db = client.test

    with open(fileName) as f:
        data = json.loads(f.read())
        db.OSM.insert(data)
        print f.read()

if __name__ == '__main__':
    fileName = "birmingham_alabama_output.txt"
    #fileName = "output.txt"

    loadMongoDB(fileName)
