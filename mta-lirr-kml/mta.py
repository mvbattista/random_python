import json
import urllib2
import pprint
import requests

def demo():
	url = 'http://web.mta.info/developers/data/lirr/lirr_gtfs.json'
	resp = requests.get(url)
	return resp.json()
	#resp = urllib2.urlopen(url)
	#return json.loads(''.join(resp.readlines()))
	
pp = pprint.PrettyPrinter(indent=4)
print 'Starting urlopen'
raw_data = demo()
print 'urlopen complete'
# pp.pprint(raw_data)
# print( json.dumps(raw_data, sort_keys=True, indent=2) )
# pp.pprint(raw_data['gtfs']['stops'])
# print (json.dumps(raw_data['gtfs']['stops'], sort_keys=True, indent=2))

f = open('output.kml', 'w')
f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
	<kml xmlns=\"http://earth.google.com/kml/2.0\"> <Document>\n")

for stop in raw_data['gtfs']['stops']:
	print stop['stop']['stop_name']+' - '+stop['stop']['stop_lat']+','+stop['stop']['stop_lon']
	f.write("<Placemark>\n\
<name>" + stop['stop']['stop_name'] +"</name>\n\
  <Point>\n\
  <coordinates>\n\
	    "+stop['stop']['stop_lon']+','+stop['stop']['stop_lat']+",0\n\
  </coordinates>\n\
 </Point>\n\
</Placemark>\n")

f.write("</Document> </kml>")
f.close()
