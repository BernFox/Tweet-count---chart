#!/usr/bin/env python

import json
from operator import itemgetter
from pprint import pprint
import pandas as pd
import numpy as np

#The original JSON data can be found @ https://www.dropbox.com/s/pmdi6kmh8k0v2tq/tweets.json.zip
injson = 'tweets.json'
outcsv = 'Syria_tweets.csv'

fields = ('_id','created_at')

tweets = pd.read_csv(outcsv, delimiter = ',')

 
def tweet_parse(jdata = injson, cdata = outcsv, get = fields):
	"""this function parses tweets from tweets.json and outputs it's content into a csv"""

	with open(jdata, 'r') as f:
		
		#Use a list comprehension to load the json contents into memory, this makes it fast. 
		data = [json.loads(line) for line in f]

	pprint(data[0:10])

	with open(cdata, 'w') as g:
		
		#Write the field names as the header row
		g.write(','.join(get) + '\n')
		
		#Use nested map()'s to write the data to a file on the hard-drive, we do this to avoid a for loop / for speed 
		#Note: we are only grabbing the '_id', and the 'created_at' column, we really only need the 'created_at' column
		map(lambda k: g.write(','.join(map(str,itemgetter(*get)(k))) + '\n'), data)

def day_id(i):
	#This function is the helper function for the day indexer in the day_count() function. 
	if 'Aug' in i: return 800
	if 'Sep' in i: return 900
	if 'Oct' in i: return 1000

def day_count(data = tweets):
	
	print 'Mapping data...', '\n'
	#Grab only the month and date in the 'created_at' column. We do this so that we can do operations on the string more easily
	data['created_at'] = map(lambda day: day[4:10], data['created_at'])
	
	#Create an index for dates in the data, all data with the same date has the same unique index, use map() to make it fast
	data['dayid'] = map(lambda day: day_id(day) + int(day[4:6]), data['created_at'])
	
	print 'Sorting data...', '\n'
	#Create a set of the day indexes in order to capture the unique values, then make a sorted list from the set. Then make a DF from the list
	day_ids = sorted(set(data['dayid']))
	data2 = pd.DataFrame(day_ids)
	
	print 'Counting data...', '\n'
	#Populate the rest of the new dataframe with the dates and tweets counts per day from the old dataset
	data2['day'] = [data['created_at'][data['dayid'] == dayid].iloc[0] for dayid in day_ids]
	data2['day_count'] = [len(data['dayid'][data['dayid'] == dayid]) for dayid in day_ids]
	data2 = data2.rename(columns={0 : 'day_id'})

	return data, data2

def to_csv(name = 'tweet_count.csv'):
	
	#Send the data to csv format
	tweets, count = day_count()

	print 'Converting data to csv...', '\n'
	count.to_csv(name)	

if __name__ == '__main__':
	to_csv()


