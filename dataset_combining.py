import pandas as pd
from hdfs import InsecureClient

required_columns = ['stays_in_weekend_nights', 'stays_in_week_nights', 'lead_time', 'arrival_year', 'arrival_month', 'avg_price_per_room', 'market_segment_type', 'booking_status']

hotel_bookings = pd.read_csv('C:\CS236_MapReduce_Project\hotel-booking.csv' , usecols=required_columns)
customer_reservations = pd.read_csv('C:\CS236_MapReduce_Project\customer-reservations.csv' , usecols=required_columns)

dataset_combined = pd.concat([hotel_bookings, customer_reservations],axis=0, ignore_index=True)
print(dataset_combined)

dataset_combined.to_csv('C:\CS236_MapReduce_Project\dataset_combined.csv', index=False)
print("created the combined csv file")

client = InsecureClient('hdfs://localhost:900', user='fs.defaulterFS')

# Upload the combined CSV file to HDFS
client.upload('/input/hdfs', 'dataset_combined.csv')