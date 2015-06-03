import xml.etree.ElementTree as ET
import pprint
import re
import json

# Tags that need special processing. Any other tags are included in the top layer of the node JSON data.
specialTags = set([
    "changeset", "uid", "timestamp", "version", "user", "lat", "lon"
])

cityAudit = []

cityMappings = {
    "Birmingham, Alabama": "Birmingham",
	"Brimingham": "Birmingham"
}

streetTypeAudit = []

streetMappings = {
    "St": "Street",
    "St.": "Street",
    "Ave": "Avenue",
    "Rd.": "Road",
	"Blvd": "Boulevard",
	"Pkwy": "Parkway",
	"Rd": "Road",
	"Dr": "Drive"
}

# Count of key/value pairs for the key values grabbed from a node.
nodeKeyCount = {}

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

def audit_map_ET(filename, outputFileTarget):    

	# Counts nodes. Use to only parse a portion of the file. Set to -1 to parse entire file.
    nodeCount = 0
    processLimit = -1

	# iterparse approach for large xml files
	# http://effbot.org/zone/element-iterparse.htm#incremental-

    # get an iterable
    context = ET.iterparse(fileName, events=("start", "end"))

    # turn it into an iterator
    context = iter(context)

    # get the root element
    event, root = context.next()

	# Used to prevent redundant XML nodes from being audited.
    previousNodeId = -1
    print "Auditing XML"
    for event, elem in context:
        if nodeCount <= processLimit or processLimit <= 0:
            node = process_node_ET(elem)
            if node != None and previousNodeId != node["id"]:
                previousNodeId = node["id"]
                audit_node_data(node)
        else:
            break
		
        if nodeCount >= 0:
			print "Auditing element " + str(nodeCount)
			nodeCount = nodeCount + 1

        root.clear()

    print "Finished auditing XML"

    fileOutput = str(nodeKeyCount) + "\n\n"
    fileOutput = fileOutput + str(streetTypeAudit) + "\n\n"
    fileOutput = fileOutput + str(cityAudit) + "\n\n"
	
    write_json(outputFileTarget, fileOutput)
    pprint.pprint(fileOutput)

def audit_node_data(node):
    for key in node:
        if key not in nodeKeyCount:
            nodeKeyCount[key] = 0
        nodeKeyCount[key] = nodeKeyCount[key] + 1

    audit_streetType(node)
    audit_city(node)

def audit_city(node):
    if "address" in node and "city" in node['address']:
        if node['address']['city'] not in cityAudit:
            cityAudit.append(node['address']['city'])

def audit_streetType(node):
    if "address" in node and "street" in node['address']:
        streetType = street_type_re.search(node['address']['street']).group()
        if streetType not in streetTypeAudit:
            streetTypeAudit.append(streetType)

def process_map_ET(fileName, outputFileTarget):
	# Clear file target
    write_json(outputFileTarget, "[")

	# Counts nodes. Use to only parse a portion of the file. Set to -1 to parse entire file.
    nodeCount = 0
    processedCount = 0
    processLimit = -1

	# iterparse approach for large xml files
	# http://effbot.org/zone/element-iterparse.htm#incremental-
	# also http://boscoh.com/programming/reading-xml-serially.html

    # get an iterable
    context = ET.iterparse(fileName, events=("start", "end"))

    # turn it into an iterator
    context = iter(context)

    # get the root element
    event, root = context.next()

    previousNodeId = -1
    print "Processing XML"
    for event, elem in context:
        outputNode = None
        if nodeCount <= processLimit or processLimit <= 0:				
			outputNode = process_node_ET(elem)
        else:
            break

        print "Processing element " + str(nodeCount)
        nodeCount = nodeCount + 1

        if outputNode != None and previousNodeId != outputNode['id']:
            processedCount = processedCount + 1
            previousNodeId = outputNode['id']
            conjunctionString = ""
            if processedCount > 1:
                conjunctionString = ", "

            append_JSON(outputFileTarget, conjunctionString + str(json.dumps(outputNode)))
			
        root.clear()
    append_JSON(outputFileTarget, "]")
    print "Finished processing XML"

def process_node_ET(element):

    if element.tag == "node" or element.tag == "way":
        
        node = {} 

        for attrib in element.attrib:
            if attrib not in specialTags:
                node[attrib] = element.attrib[attrib]
            if problemchars.match(attrib) or problemchars.match(element.attrib[attrib]):
                return None

        node['type'] = element.tag

        node['created'] = {}
        node['created']['changeset'] = element.attrib['changeset']
        node['created']['uid'] = element.attrib['uid']
        node['created']['timestamp'] = element.attrib['timestamp']
        node['created']['version'] = element.attrib['version']
        node['created']['user'] = element.attrib['user']

        if "lat" in element.attrib and "lon" in element.attrib:
            node['pos'] = [float(element.attrib['lat']), float(element.attrib['lon'])]

        # Search for address data
        for child in element:
            if 'k' in child.attrib and not problemchars.match(child.attrib['k']):
                stringArray = child.attrib['k'].split(':')
                if (stringArray[0] == "addr" or stringArray[0] == "") and len(stringArray) <= 2:
                    if "address" not in node:
                        node['address'] = {}
                    node['address'][stringArray[1]] = child.attrib['v']
                else: # Catch any other children key/value pairs.
                    node[child.attrib['k']] = child.attrib['v']
            elif child.tag == "nd" and "ref" in child.attrib:
                if "node_refs" not in node:
                    node['node_refs'] = []
                node['node_refs'].append(child.attrib['ref'])

        return clean_data(node)
    else:
        return None

def clean_data(node):

    if "address" in node:
        if "street" in node['address']:
            node['address']['street'] = clean_street(node['address']['street'])

        if "city" in node['address']:
            node['address']['city'] = clean_city(node['address']['city'])

    return node
	
def clean_street(street):
    streetType = street_type_re.search(street).group()

    if streetType in streetMappings:
        street = street.replace(streetType, streetMappings[streetType])

    return street

def clean_city(city):
    if city in cityMappings:
        city = cityMappings[city]

    return city

def append_JSON(fileTarget, data):
	f = open(fileTarget, 'a')
	f.write(data + "\n")
	f.close()
	#print data

def write_json(fileTarget, data):
	f = open(fileTarget, 'w')
	f.write(data)
	f.close()

if __name__ == '__main__':
	#fileName = "atlanta_georgia.osm"
	fileName = "birmingham_alabama.osm"
	#fileName = "macon_georgia.osm"
	#fileName = "example.osm"

	#audit_map_ET(fileName, "output_attrib_count.txt")
	process_map_ET(fileName, "output.txt")
