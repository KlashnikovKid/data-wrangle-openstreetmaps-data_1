from pymongo import MongoClient
import pprint

def get_db(db_name):
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def query_statistics(db):
    numberOfDocs = db.OSM.find().count()
    numberOfTypeNode = db.OSM.find({"type":"node"}).count()
    numberOfTypeWay = db.OSM.find({"type":"way"}).count()
    numberOfDistinctUsers = len(db.OSM.distinct("created.user"))

    topContributingUser = db.OSM.aggregate([
        { "$group" : { "_id" : "$created.user", "count" : {"$sum" : 1 } } },
        { "$sort" : { "count" : -1 } },
        { "$limit" : 1 }
    ])
    topContributingUser = list(topContributingUser)

    numberOfOneTimeUsers = db.OSM.aggregate([
        { "$group" : { "_id" : "$created.user", "count" : { "$sum" : 1 } } },
        { "$match" : { "count" : 1 } }
    ])
    numberOfOneTimeUsers = len(list(numberOfOneTimeUsers))

    topStreetNames = db.OSM.aggregate([
        { "$match" : { "address.street" : { "$exists" : True } } },
        { "$group" : { "_id" : "$address.street", "count" : { "$sum" : 1 } } },
        { "$sort" : { "count" : -1 } }, # Sort by count
        #{ "$sort" : { "_id" : 1 } } # Sort by streetname
        { "$limit" : 20 }
    ])
    topStreetNames = list(topStreetNames)

    topCityNames = db.OSM.aggregate([
        { "$match" : { "address.city" : { "$exists" : True } } },
        { "$group" : { "_id" : "$address.city", "count" : { "$sum" : 1 } } },
        { "$sort" : { "count" : -1 } }, # Sort by count
        #{ "$sort" : { "_id" : 1 } } # Sort by city
        { "$limit" : 20 }
    ])
    topCityNames = list(topCityNames)

    topZipCodes = db.OSM.aggregate([
        { "$match" : { "address.postcode" : { "$exists" : True } } },
        { "$group" : { "_id" : "$address.postcode", "count" : { "$sum" : 1 } } },
        { "$sort" : { "count" : -1 } }, # Sort by count
        #{ "$sort" : { "_id" : 1 } } # Sort by postcode
        { "$limit" : 20 }
    ])
    topZipCodes = list(topZipCodes)

    topReligions = db.OSM.aggregate([
        { "$match" : { "religion" : { "$exists" : True } } },
        { "$group" : { "_id" : "$religion", "count" : { "$sum" : 1 } } },
        { "$sort" : { "count" : -1 } }, # Sort by count
        #{ "$sort" : { "_id" : 1 } } # Sort by postcode
        { "$limit" : 20 }
    ])
    topReligions = list(topReligions)

    topAmenities = db.OSM.aggregate([
        { "$match" : { "amenity" : { "$exists" : True } } },
        { "$group" : { "_id" : "$amenity", "count" : { "$sum" : 1 } } },
        { "$sort" : { "count" : -1 } }, # Sort by count
        #{ "$sort" : { "_id" : 1 } } # Sort by postcode
        { "$limit" : 20 }
    ])
    topAmenities = list(topAmenities)

    topShops = db.OSM.aggregate([
        { "$match" : { "shop" : { "$exists" : True } } },
        { "$group" : { "_id" : "$shop", "count" : { "$sum" : 1 } } },
        { "$sort" : { "count" : -1 } }, # Sort by count
        #{ "$sort" : { "_id" : 1 } } # Sort by postcode
        { "$limit" : 20 }
    ])
    topShops = list(topShops)

    #2348 door code
    #housenumber, postcode, state
    print "Number of documents - " + str(numberOfDocs)
    print 'Number of "nodes" - ' + str(numberOfTypeNode)
    print 'Number of "way" - ' + str(numberOfTypeWay)
    print 'Number of distinct users - ' + str(numberOfDistinctUsers)
    print 'Number of one time users - ' + str(numberOfOneTimeUsers)

    print 'Top contributing user - ' + str(topContributingUser)

    print "Top street street names -"
    pprint.pprint(topStreetNames)
    print

    print "Top city names -"
    pprint.pprint(topCityNames)
    print

    print "Top zipcodes -"
    pprint.pprint(topZipCodes)
    print
	
    print "Top religions -"
    pprint.pprint(topReligions)
    print

    print "Top amenities -"
    pprint.pprint(topAmenities)
    print

    print "Top shops -"
    pprint.pprint(topShops)
    print

def audit_queries(db):
    streetNames = db.OSM.aggregate([
        { "$match" : { "address.street" : { "$exists" : True } } },
        { "$group" : { "_id" : "$address.street", "count" : { "$sum" : 1 } } },
        #{ "$sort" : { "count" : -1 } }, # Sort by count
        { "$sort" : { "_id" : 1 } } # Sort by name
        #{ "$limit" : 20 }
    ])
    streetNames = list(streetNames)

    zipCodes = db.OSM.aggregate([
        { "$match" : { "address.postcode" : { "$exists" : True } } },
        { "$group" : { "_id" : "$address.postcode", "count" : { "$sum" : 1 } } },
        #{ "$sort" : { "count" : -1 } }, # Sort by count
        { "$sort" : { "_id" : 1 } } # Sort by name
        #{ "$limit" : 20 }
    ])
    zipCodes = list(zipCodes)

    cities = db.OSM.aggregate([
        { "$match" : { "address.city" : { "$exists" : True } } },
        { "$group" : { "_id" : "$address.city", "count" : { "$sum" : 1 } } },
        #{ "$sort" : { "count" : -1 } }, # Sort by count
        { "$sort" : { "_id" : 1 } } # Sort by name
        #{ "$limit" : 20 }
    ])
    cities = list(cities)

    amenities = db.OSM.aggregate([
        { "$match" : { "amenity" : { "$exists" : True } } },
        { "$group" : { "_id" : "$amenity", "count" : { "$sum" : 1 } } },
        #{ "$sort" : { "count" : -1 } }, # Sort by count
        { "$sort" : { "_id" : 1 } } # Sort by name
        #{ "$limit" : 20 }
    ])
    amenities = list(amenities)

    print "Street names -"
    pprint.pprint(streetNames)
    print
	
    print "Zipcodes -"
    pprint.pprint(zipCodes)
    print

    print "Cities -"
    pprint.pprint(cities)
    print

    print "Amenities -"
    pprint.pprint(amenities)
    print

def update_data(label, where, key, value):

    db.OSM.update(
        { label : where },
        { '$set' : { key : value } }
    )

def clean_data(db):

    update_data('address.street', '18th St S', 'address.street', '18th Street South')
    update_data('address.street', 'Green Springs Highway South', 'address.street', 'Green Springs Highway')
    update_data('address.street', 'Montgomery Hwy. S', 'address.street', 'Montgomery Highway')
    update_data('address.street', 'Odum Rd', 'address.street', 'Odum Road')
    update_data('address.street', 'University Blvd', 'address.street', 'University Boulevard')

if __name__ == '__main__':
    db = get_db("test")

    #clean_data(db)
    query_statistics(db)	
    audit_queries(db)
