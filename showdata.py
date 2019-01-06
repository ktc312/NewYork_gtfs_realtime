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

stations_to_track = {}

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
	#epoch -= 21600 #changing from Berlin time to New york time ( 6hour difference )
	#TODO: not a good way because of time shifting. change it in a better way.
	if epoch == None:
		return "UNKN"
	return (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch)))

def get_schedule(feed, line_id, result):
	df = read_file("stations.csv")
	arrival = None
	departure = None
	if 'entity' in feed:
		for en in feed["entity"]:
			#if "vehicle" in en:
			#	continue
			if "trip_update" in en:
				if 'trip' in en["trip_update"]:
					if 'trip_id' in en["trip_update"]["trip"]:
						if en["trip_update"]["trip"]["route_id"] == str(line_id):
							result["stops"] = {}
							for update in en["trip_update"]["stop_time_update"]:
								if 'departure' in update:
									departure = update["departure"]["time"]
								#station_name = get_station_from_id(update["stop_id"], df) # no need to know the names for now
								stp_id = update["stop_id"][:-1] # sometimes id has N or S at the end ( southbound and northbound ) we don't need to check that.
								result["stops"][stp_id] = {"departure":epoch_to_realtime(departure)}
								#print("loop result ",result["stops"][stp_id])
					else:
						print("field trip_ID is not found")
				else:
					print("field trip is not found")
			else:
				if 'alert' in en:
					if 'header_text' in en['alert']:
						print(en['alert']['header_text'])
						result["alerts"] = en['alert']['header_text']
						continue
					else:
						result["alerts"] = en['alert']
						continue
	else:
		print('could not find entity in the reply feed from API.')
		return
	return result

def log_one_hour(feed):
	pattern = '%Y-%m-%d %H:%M:%S'
	departs = feed['stops']
	now = int(time.time())
	for station in departs:
		if station not in stations_to_track:
			stations_to_track[station] = []
		date_time = departs[station]['departure']
		station_epoch = int(time.mktime(time.strptime(date_time, pattern)))
		print(now - station_epoch)
		if station_epoch - now <= 3600: #no need to check for > 0 ( sometimes dep time a bit passed might appear )
			stations_to_track[station].append(station_epoch)
	print(stations_to_track)




def get_feeds(id, line_id):
	line_id = line_id.capitalize() #in case it is not in capital case
	raw_feed = gtfs_realtime_pb2.FeedMessage()
	request = 'http://datamine.mta.info/mta_esi.php?key={}&feed_id='+str(id)
	#request = 'http://gtfsrt.prod.obanyc.com/tripUpdates?key='+risi_api_key
	response = requests.get(request.format(mta_api_key))
	raw_feed.ParseFromString(response.content)
	feed = protobuf_to_dict(raw_feed)
	result = {}
	#f = open("demofile.txt", "w")
	#f.write(str(feed))
	if line_id != None:
		result[line_id] = {}
		return get_schedule(feed, line_id, result[line_id])
	else:
		result = feed
		#print(result)
		return result


def get_bus_feed():
	url = 'http://gtfsrt.prod.obanyc.com/tripUpdates?key='+risi_api_key
	#params = {
	#	'key':risi_api_key
	#}
	r = requests.get(url)
	print(r.content)

def getSomeFeeds():
	url = 'https://mnorth.prod.acquia-sites.com/wse/LIRR/gtfsrt/realtime/'+mta_api_key+'/json'
	r = requests.get(url, json={"key": mta_api_key})
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
starttime=time.time()
deps = []
while int(time.time()) < 1546795056:
	print("Getting Query..")
	result = get_feeds(26,"a")
	print("result: ",result)
	log_one_hour(result)
	time.sleep(60.0 - ((time.time() - starttime) % 30.0))
	"""temp = log_station('H03', result, deps)
	if temp != -1:
		break
	else:
		time.sleep(60.0 - ((time.time() - starttime) % 30.0))
		print(deps)
		print("------")
		continue"""

print(stations_to_track)

##############################################
#df = read_file("stations.csv")
##############################################
#station = get_station_from_id("G05", df)
#if station != False:
#    print(station)
#print(epoch_to_realtime(1545570438))


#getSomeFeeds()
