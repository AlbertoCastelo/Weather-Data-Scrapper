import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from dateutil.rrule import rrule, DAILY
import requests
import json
import matplotlib.pyplot as plt

def load_parameters(filename):
	with open(filename) as json_data:
		data = json.load(json_data)
	return data

def main():
	periodSampling = 15
	startDate = date(2014, 12, 31)
	endDate = date(2017, 7, 1)
	

	
	parameters = load_parameters("configuration/configuration_weather_underground.json")
	
	KEY = parameters["userKEY"]
	state = "TX"
	city = "Austin"
	
	scraper = ScraperWU(KEY, state, city)
	
	data = scraper.readHistoricalData(startDate, endDate)
	data = scraper.interpolate()
	
	print(data)
	

class ScraperWU:
	def __init__(self, key, state, city):
		self.key = key
		self.state = state
		self.city = city
		self.weather_data = pd.DataFrame([])
		
	def resampling(self, data, period):
		rule = str(period) + 'T'
		dataOut = data.resample(rule).mean()
		print(dataOut.head())
		return dataOut
		
	def interpolate(self):
		# resample to 1 minute data
		ts1 = self.weather_data.resample('T', how='mean')
		ts = ts1.interpolate(method='time')
		
		data15 = self.resampling(ts, 15)
		data15.to_csv("preparedData/weather15.csv")
		
		data30 = self.resampling(ts, 30)
		data30.to_csv("preparedData/weather30.csv")
		
		data60 = self.resampling(ts, 60)
		data60.to_csv("preparedData/weather60.csv")
		return ts
		
	def readHistoricalData(self, startDate, endDate):
		#date = "20160131"
		
		startDate = startDate - timedelta(days=1)
		endDate = endDate + timedelta(days=1)
		
		while startDate <= endDate:
			dateSt = self.transformDate(startDate)
			print("\n"+dateSt)
			
			# get raw data
			data_raw = self.requestDayData(dateSt)
			
			# parse data
			self.parseData(data_raw)
			
			# update time
			startDate = startDate + timedelta(days=1)
		
		self.weather_data['datetime'] = pd.to_datetime(self.weather_data['datetime'])
		self.weather_data.index = self.weather_data['datetime']
		del self.weather_data['datetime']
		#print(self.weather_data.head())
		
		self.weather_data.to_csv("data/weather.csv")
	
	def getMeasurement(self, measurements, field):
		value = measurements[field]
		print(value)
		if value == 'N/A':
			value = np.NaN
		else:
			value = float(measurements[field])
			if value < -100.0:
				value = np.NaN
		return value
	
	def parseData(self, data_raw):
		obs = data_raw['history']['observations']
		for measurements in obs:
			tempC = self.getMeasurement(measurements, 'tempm')
			hum = self.getMeasurement(measurements, 'hum')
			localdate = measurements['date']
			utcdate = measurements['utcdate']
			dateLocal = self.getDatefromUTC(localdate)
			
			# add value to timeseries
			dataList = [dateLocal, tempC, hum]

			df2 = pd.DataFrame(data=dataList)
			df = pd.DataFrame({"datetime" : [dateLocal],
								"tempC" : [tempC],
								"hum" : [hum]})
			
			self.weather_data = self.weather_data.append(df)
			
	def getDatefromUTC(self, utcdate):
		dateUTC = datetime(int(utcdate['year']), int(utcdate['mon']), int(utcdate['mday']),
					   int(utcdate['hour']), int(utcdate['min']), 0)
		return dateUTC
		
	def requestDayData(self, date):
		urlRequest = "http://api.wunderground.com/api/" + self.key +"/history_" + date + "/q/" + self.state +"/" + self.city + ".json"
		data = requests.get(urlRequest).json()
		return data
	
	def transformDate(self, date):
		dateSt = date.strftime('%Y%m%d')
		#dateSt = ""
		return dateSt
	
main()
