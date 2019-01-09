from google.transit import gtfs_realtime_pb2
from protobuf_to_dict import protobuf_to_dict
import simplejson as json
import pandas as pd
import requests
import calendar
import sched
import time
import csv
import sys

mta_api_key = 'a628ade898c8be80e2d069f519ca3ee3'
risi_api_key = '8162319f-e74d-4e7e-a632-75a8fdca95cd'
railRoad_url = "http://lirr42.mta.info/stationInfo.php?id=13"

subway_stations = "subway_stations.csv"
subway_request = 'http://datamine.mta.info/mta_esi.php?key={}' #&feed_id='+str(id)

bus_stops = "bus_stops.csv"
bus_request = 'http://gtfsrt.prod.obanyc.com/tripUpdates?key=' #+risi_api_key


bus_to_track = {}
sub_stations_to_track = {}

"""
	Read the csv file and return a DataFrame
"""
def read_file(fileName):
	return pd.read_csv(fileName, header=None)

def get_station_from_id(selected_id, df):
	# get the GTFS stop ID and names columns from Dataframe File:
	ids = df[2]
	selected_id = selected_id[:-1]
	index = 0
	for ID in ids:
		if ID == str(selected_id):
			return df[5][index]
		index += 1
	#print("corresponding station name for the given ID, "+selected_id+", is not found.")
	return False


def epoch_to_realtime(epoch):
	if epoch == None:
		return "UNKN"
	epoch -= 21600 #changing from Berlin time to New york time ( 6hour difference )
	#TODO: not a good way because of time shifting. change it in a better way.
	return (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch)))

def get_bus_schedule(feed, line_id, result):
	departure = None
	vehicleId = ''
	line_id = str(line_id).capitalize()
	result[line_id] = {}
	if 'entity' in feed:
		for en in feed["entity"]:
			if "trip_update" in en:
				if 'vehicle' in en["trip_update"]:
					vehicleId = en["trip_update"]['vehicle']['id']
					result[line_id][vehicleId] = {}
				if 'trip' in en["trip_update"]:
					if 'trip_id' in en["trip_update"]["trip"]:
						if en["trip_update"]["trip"]["route_id"] == line_id:
							for update in en["trip_update"]["stop_time_update"]:
								if 'departure' in update:
									departure = update["departure"]["time"]
								stp_id = update["stop_id"]
								#if stp_id in result[line_id][vehicleId]:
								#	result[line_id][vehicleId][stp_id].append(epoch_to_realtime(departure))
								#else:
								result[line_id][vehicleId][stp_id] = epoch_to_realtime(departure)
					else:
						print("field trip_ID is not found")
				else:
					print("field trip is not found")
			else:
				if 'alert' in en:
					if 'header_text' in en['alert']:
						print(en['alert']['header_text'])
						result["alerts"] = str(en['alert']['header_text'])+'-at-'+str(int(time.time()-21600))+'for '+line_id
						continue
					else:
						result["alerts"] = en['alert']+'-at-'+str(int(time.time()-21600))+'for '+line_id
						continue
	else:
		print('could not find entity in the reply feed from API.')
		return
	return result

def get_schedule(feed, line_id, result):
	#df = read_file(subway_stations)
	departure = None
	result[str(line_id)] = {}
	if 'entity' in feed:
		for en in feed["entity"]:
			#if "vehicle" in en:
			#	continue
			if "trip_update" in en:
				if 'trip' in en["trip_update"]:
					if 'trip_id' in en["trip_update"]["trip"]:
						if en["trip_update"]["trip"]["route_id"] == str(line_id):
							for update in en["trip_update"]["stop_time_update"]:
								if 'departure' in update:
									departure = update["departure"]["time"]
								stp_id = update["stop_id"]
								#if stp_id == "400337":
									#result[str(line_id)][stp_id] = {"departure":epoch_to_realtime(departure)}

								if stp_id in result[str(line_id)]:
									result[str(line_id)][stp_id].append(epoch_to_realtime(departure))
								else:
									result[str(line_id)][stp_id] = [epoch_to_realtime(departure)]
					else:
						print("field trip_ID is not found")
				else:
					print("field trip is not found")
			else:
				if 'alert' in en:
					if 'header_text' in en['alert']:
						print(en['alert']['header_text'])
						result["alerts"] = str(en['alert']['header_text'])+'-at-'+str(int(time.time()-21600))+'for '+line_id
						continue
					else:
						result["alerts"] = en['alert']+'-at-'+str(int(time.time()-21600))+'for '+line_id
						continue
	else:
		print('could not find entity in the reply feed from API.')
		return
	return result

def log_one_hour_bus(feed, id):
	pattern = '%Y-%m-%d %H:%M:%S'
	ID = str(id).capitalize()
	if (feed == None):
		return
	if ID not in feed:
		return
	if ID not in bus_to_track:
		bus_to_track[ID] = {}
	departs = feed[ID]
	for bus in departs:
		if len(bus) == 0:
			continue

		if bus not in bus_to_track[ID]:
			bus_to_track[ID][bus] = []
		for stop, when in departs[bus].items():
			if when == 'UNKN':
				continue
			station_epoch = int(time.mktime(time.strptime(when, pattern)))
			now = int(time.time() - 21600) #shifting our time to NewYork's time
			if (station_epoch - now) <= 3600:
				bus_to_track[ID][bus].append(station_epoch)
		bus_to_track[ID][bus].append("-")
	print("bus_to_track: ",bus_to_track)

def log_one_hour(feed, id):
	pattern = '%Y-%m-%d %H:%M:%S'
	ID = str(id).capitalize()
	#id = id.capitalize()
	if (feed == None):
		return
	if ID not in feed:
		return
	if ID not in sub_stations_to_track:
		sub_stations_to_track[ID] = {}
	departs = feed[ID]
	for station in departs:
		if station not in sub_stations_to_track[ID]:
			sub_stations_to_track[ID][station] = []
		if len(departs[station]) == 0:
			return
		for t in departs[station]:
			if t == "UNKN":
				continue
			date_time = t
		#changing departure time into epoch time:
			station_epoch = int(time.mktime(time.strptime(date_time, pattern)))
		#Now checking if the departure time is less than one hour to current time:
		#no need to check for > 0 ( sometimes a departue time which might have passed, appears ):
			now = int(time.time() - 21600) #shifting our time to NewYork's time
			if (station_epoch - now) <= 3600:
				sub_stations_to_track[ID][station].append(station_epoch)
		sub_stations_to_track[ID][station].append("-") # to realize one round is complete
	print("sub_stations_to_track: ", sub_stations_to_track)




def get_feeds(line_id, request, key, feed_id):
	line_id = line_id.capitalize() #in case it is not in capital case
	raw_feed = gtfs_realtime_pb2.FeedMessage()

	#request = railRoad_url
	#request = 'http://gtfsrt.prod.obanyc.com/tripUpdates?key='+risi_api_key
	if feed_id == None:
		params = {
			'key':key,
		}
		serviceType = "bus"
	else:
		params = {
			'key':key,
			'feed_id': feed_id
		}
		serviceType = "subway"
	response = requests.get(request, params)
	try:
		raw_feed.ParseFromString(response.content)
	except:
		print("could not parse the response.")
		return False
	feed = protobuf_to_dict(raw_feed)
	result = {}
	#f = open("demofile.txt", "w")
	#f.write(str(feed))
	if line_id != None:
		result[line_id] = {}
		if serviceType == 'subway':
			return get_schedule(feed, line_id, result[line_id])
		else:
			return get_bus_schedule(feed, line_id, result[line_id])
	else:
		return
		#result = feed
		#print(result)
		#return result


def get_bus_feed():
	url = 'http://gtfsrt.prod.obanyc.com/tripUpdates?key='+risi_api_key
	raw_feed = gtfs_realtime_pb2.FeedMessage()
	#url = "http://bustime.mta.info/api/siri/vehicle-monitoring.json"
	params = {
		'key':risi_api_key,
		#'key':mta_api_key,
		'feed_id': 'M2'
	}
	#r = requests.get(url, params)
	r = requests.get(bus_request, params)
	raw_feed.ParseFromString(r.content)
	feed = protobuf_to_dict(raw_feed)
	print(feed)

def getSomeFeeds():
	url = 'https://mnorth.prod.acquia-sites.com/wse/LIRR/gtfsrt/realtime/'+mta_api_key+'/json'
	r = requests.get(railRoad_url, json={"key": mta_api_key})
	print(r.content) #access denied

def rightNow():
	return (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())).split(' ')[1]

def log_station(s_id, logs, deps):
	if "stops" in logs:
		stops = logs["stops"]
		station = {}
		if s_id in stops:
			#depTime = stops[s_id]['departure'].split(' ')[1]
			depTime = '15:58:00'
			if rightNow() < depTime:
				deps.append(stops[s_id])
				print("found the station")
				return -1
			else:
				return
		else:
			print("could not found the station")
		return -1
	else:
		print("data log does not have the right format:")
		print(logs)



#get_bus_feed()
##############################################
#get_feeds(31, 'MQ_O8-Weekday-042000_M20_3')
##############################################
"""
starttime=time.time()
deps = []
index = 1
line_ids = [1,26]
#result = get_feeds("M2", bus_request, risi_api_key, None)
result = get_feeds("C", subway_request, mta_api_key, 26)
print("RESULT: ",result)
#get_bus_feed()
liness = {"1":"1","26":"C"}
"""
starttime=time.time()
busFile = open("output_bus.json","w")
subFile = open("output_sub.json","w")
index = 1
counter = 0
sub_line_ids = [1,26]
bus_line_ids = ["M2","M10"]
subways = {"1":"1","26":"A"}
#liness = {"1":"1","26":"C"}
#while int(time.time()) < 1546897926:

while counter < 3:
	index = index % 2
	sub_id = sub_line_ids[index]
	bus_id = bus_line_ids[index]
	print("Getting Query..")
	subResult = get_feeds(subways[str(sub_id)],subway_request,mta_api_key,sub_id)
	busResult = get_feeds(bus_id,bus_request,risi_api_key,None)
	if subResult != False:
		#log_one_hour_bus(result, l_id)
		log_one_hour(subResult, subways[str(sub_id)])


	if busResult != False:
		log_one_hour_bus(busResult, bus_id)
		#log_one_hour(result, subways[str(sub_id)])
	index += 1
	counter += 1
	time.sleep(60.0 - ((time.time() - starttime) % 30.0))

subJsonFile = json.dumps(sub_stations_to_track)
busJsonFile = json.dumps(bus_to_track)
busFile.write(str(busJsonFile))
subFile.write(str(subJsonFile))
busFile.close()
subFile.close()
"""
result = get_feeds("M2",bus_request, risi_api_key, None)
print("M2 result: ",result)
log_one_hour(result, "M2")
print(stations_to_track)
#time.sleep(30.0 - ((time.time() - starttime) % 30.0))
#result = get_feeds(1,"1")
#print("1 result: ",result)
#log_one_hour(result, "1")
#print(stations_to_track)


subResult = get_feeds("1",subway_request,mta_api_key,1)
if subResult != False:
	#log_one_hour_bus(result, l_id)
	log_one_hour(subResult, "1")
"""
##############################################
#df = read_file("stations.csv")
##############################################
#station = get_station_from_id("G05", df)
#if station != False:
#    print(station)
#print(epoch_to_realtime(1545570438))


#getSomeFeeds()
