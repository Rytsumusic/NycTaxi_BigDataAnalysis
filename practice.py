import pandas as pd
import datetime
import glob
import sys
df = None
for file in sorted(glob.glob('/data/nyctaxi/yellow_tripdata_2023-11.parquet')):
    print("reading", file)
    if df is None:
        df = pd.read_parquet(file)
    else:
        df = pd.concat([df, pd.read_parquet(file)])
#df = pd.read_parquet("/data/nyctaxi/yellow_tripdata_2023-11.parquet")
print(df['tpep_pickup_datetime'])
df['date'] = df['tpep_pickup_datetime'].dt.date
df = df[(df['date'] > datetime.date(2023,10,31)) & (df['date'] < datetime.date(2023,12,1))]
#print(df.columns)
df['hour_of_day'] = df['tpep_pickup_datetime'].dt.hour
df['day_of_week'] = df['tpep_pickup_datetime'].dt.dayofweek



#find average trip distance
#print("AVG trip distance",df['trip_distance'].mean())
#find min and max fare amount
#print('Min Fare Amount',df['fare_amount'].min())
#print('Max Fare Amount',df['fare_amount'].max())
##find average fare amount per # of passengers
#print('Average fair per customer',df.groupby('passenger_count')['fare_amount'].mean())
#find average fare amount for trips from the airport
#print(df.query('Airport_fee > 0')['fare_amount'].mean())
#find average congestion surcharge for each hour of the day: and for each  day of the week
#df ['hour_of_day']= df['tpep_pickup_datetime'].dt.hour
#df['day_of_week']=df['tpep_pickup_datetime'].dt.dayofweek
#print('Average Congestion Surchage for each hour',df['hour_of_day'].mean())
#print('Average Congestion Surcharge for each day of week',df['day_of_week'].mean())


# find the most frequent pick up and drop off locations
#print('Most frequent pick up location',df['PULocationID'].value_counts().idxmax())
# find the most frequent pick up and drop off pair
#print('Most frequent pick up and drop off pair',df.groupby(['PULocationID','DOLocationID']).size().idxmax())
# find the most frequent pick up locations for night hours on weekends
#print('Most frequent pick up locations for night hours on weekends',df.query('hour_of_day > 18 and day_of_week > 4')['PULocationID'].value_counts().idxmax())
# It's 3:35 on a saturday. I am at the Met. How much will it cost me and my two friends to get to the world trade center?
#print('Cost from MET to WTC',df.query('PULocationID == 236 and DOLocationID == 261')['fare_amount'].mean())
 #MET:#236
 #WTC:#261
#graphs:
#trips per day
import matplotlib.pyplot as plt

# Trips per day
#xy = df['tpep_pickup_datetime'].dt.date.value_counts().sort_index()
#x = xy.index
#y = xy.values
#print(x)
#print(y)

#xy = df.groupby('date')['VendorID'].count()
#x = xy.index
#y = xy.values
#print(x)
#print(y)

#df['week'] = df['tpep_pickup_datetime'].dt.isocalendar().week
#xy_byweek = df.groupby('week')['VendorID'].count()
#x_byweek = xy_byweek.index
#y_byweek = xy_byweek.values
#print(x_byweek)
#print(y_byweek)

#plt.plot(x, y)
#plt.axvline(datetime.date(2023,11,23), color='r', linestyle='--')
# add a vertical at each Monday
#for i in range(len(x)):
#    if x[i].weekday() == 0:
#        plt.axvline(x[i], color='g', linestyle=':')
# plot mean
#plt.axhline(y.mean(), color='k', linestyle='-.')
# plot rectangle of largest week
#max_week = y_byweek.argmax()
#plt.axvspan(datetime.date.fromisocalendar(2023, x_byweek[max_week], 1), datetime.date.fromisocalendar(2023, x_byweek[max_week], 7), alpha=0.3, color='y')
#plt.xlabel('Date')
#plt.ylabel('Trips')
#plt.title('Trips per day')
#plt.xticks(rotation=45)
#plt.ylim(0, y.max() + 10000)
# vertical line at thanksgiving
#plt.tight_layout()
#plt.savefig('trips_per_day.png')

#graphs
#Trips per day
#xy = df['tpep_pickup_datetime'].dt.date.value_counts().sort_index()
#x = xy.index
#y = xy.values
#plt.plot(x, y)
#plt.savefig('trips_per_day.png')

#Average trip distance per hour of the day
#xy = df.groupby('hour_of_day')['trip_distance'].mean()
#x = xy.index
#y = xy.values
#plt.plot(x, y)
#plt.savefig('avg_trip_distance_per_hour.png')

#average fare amount per number of passengers
#xy = df.groupby('passenger_count')['fare_amount'].mean()
#x = xy.index
#y = xy.values
#plt.bar(x, y)
#plt.savefig('avg_fare_amount_per_passenger.png')
#average fare amount for trips from the airport vs non-airport
plt.clf()
df['airport_pickup'] = df['Airport_fee'] > 0
airport_fare = df.query('airport_pickup')['fare_amount'].mean()
non_airport_fare = df.query('~airport_pickup')['fare_amount'].mean()
plt.bar(['airport', 'non-airport'], [airport_fare, non_airport_fare])
plt.savefig('avg_fare_amount_airport_vs_non.png')

#median congestion surcharge per hour of the day; and per day of the week(grid)
plt.clf()
df ['day_of_week'] = df['tpep_pickup_datetime'].dt.dayofweek
df['hour_of_day'] = df['tpep_pickup_datetime'].dt.hour
xy = df.groupby(('hour_of_day','hour_of_day'))['congestion_surcharge'].mean()
print(xy)
xy = xy.unstack()
print(xy)
plt.imshow(xy,cmap = 'viridis')
plt.colorbar()
plt.xlabel('Hour of Day')
plt.ylabel('Day of Week')
plt.xticks(range(24), range(24))
plt.yticks(range(7), ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
plt.savefig('median_congestion_surcharge.png')
#overlay on map: most frequent pick up and drop off locations




