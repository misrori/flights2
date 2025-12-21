from dotenv import load_dotenv
import os
from datetime import datetime
import pandas as pd
import requests
from pandas import json_normalize
from datetime import datetime, timedelta
import json
import os
load_dotenv()



KIWI_API_KEY = os.environ.get('KIWI_TOKEN')
START_LOCATION_ID = "BUD"
MAX_PLAN_FORWARD_DAYS = 180
MAX_RADIUS=6000
MIN_STAY = 0
MAX_STAY = 7
JSON_DATA = 'json_data'
CSV_DATA ='csv_data'
LAST_PRICE = CSV_DATA + '/last_prices.csv'
TOP3_FLIGHTS = CSV_DATA + '/top3_flights.csv'
TODATE = datetime.now().strftime("%Y_%m_%d_")
NEW_FILE = CSV_DATA + '/' + TODATE + 'BUD'



def get_one_dest(destination_id):
    if destination_id in locations_bali['id'].tolist():
        MIN_STAY = 14
        MAX_STAY = 35
    else:
        MIN_STAY = 0
        MAX_STAY = 7
    
    params = {
        'fly_from': START_LOCATION_ID,
        'fly_to': destination_id,
        'date_from': current_date_string ,
        'date_to': future_date_string,
        'adults': '1',
        'nights_in_dst_from':MIN_STAY,
        'nights_in_dst_to':MAX_STAY,
        'curr':'HUF'
    }

    response = requests.get('https://api.tequila.kiwi.com/v2/search', params=params, headers=headers)

    data = response.json()

#   file_path = JSON_DATA + '/' + datetime.now().strftime("%Y_%m_%d_") + START_LOCATION_ID + '-' +destination_id + '.csv'
#    with open(file_path, 'w') as json_file:
#        # Step 4: Use json.dump() to write the dictionary to the file
#        json.dump(data, json_file, indent=4)

    df = pd.DataFrame(list(map(lambda x: {
        'from': x['cityFrom'], 
        'to':x['cityTo'],
        'price': x['price'], 
        'outgoing_start' : x['route'][0] ['local_departure'], 
        'outgoing_arrival' : [fl for fl in x['route'] if fl['flyTo']==destination_id][0]['local_arrival'], 
        'incomming_start': [fl for fl in x['route'] if fl['flyFrom']==destination_id][0]['local_departure'],
        'incomming_arrival': [fl for fl in x['route'] if fl['flyTo']=='BUD'][0]['local_departure'],
        'night_in_dest': x['nightsInDest'],
        'link':x['deep_link'] ,
        'stop_oda': [fl['flyTo'] for fl in x['route'] ].index(destination_id),
        'stop_vissza': (len([fl['flyTo'] for fl in x['route'] ]) -([fl['flyTo'] for fl in x['route'] ].index(destination_id) +1))-1
        },data['data'] )))
    
    df['price'] = df['price'].astype(int)
    df['dest_id'] =  destination_id 
    return(df)


if not os.path.exists(JSON_DATA):
    os.makedirs(JSON_DATA)

if not os.path.exists(CSV_DATA):
    os.makedirs(CSV_DATA)

headers = {
    'accept': 'application/json',
    'apikey': KIWI_API_KEY,
}

# get locations
params = {
    'lat': '47',
    'lon': '19',
    'radius': MAX_RADIUS,
    'locale': 'en-US',
    'location_types': 'airport',
    'limit': '500',
    'sort':'rank',
    'active_only': 'true',
}

response = requests.get('https://api.tequila.kiwi.com/locations/radius', params=params, headers=headers)

data = response.json()
locations = pd.DataFrame(list(map(lambda x:{
    'id': x['id'],
    'name': x['name'],
    'city': x['city']['name'],
    'country': x['city']['country']['name'],
    'continent': x['city']['continent']['name'],
    'region': x['city']['region']['name'],
    'lon': x['location']['lon'],
    'lat': x['location']['lat'], 
    'rank':x['rank']
},  data['locations'] )))


# bali
# get locations
params_bali = {
    'lat': '-8.3',
    'lon': '115',
    'radius': 800,
    'locale': 'en-US',
    'location_types': 'airport',
    'limit': '500',
    'sort':'rank',
    'active_only': 'true',
}

response = requests.get('https://api.tequila.kiwi.com/locations/radius', params=params_bali, headers=headers)

data_bali = response.json()
locations_bali = pd.DataFrame(list(map(lambda x:{
    'id': x['id'],
    'name': x['name'],
    'city': x['city']['name'],
    'country': x['city']['country']['name'],
    'continent': x['city']['continent']['name'],
    'region': x['city']['region']['name'],
    'lon': x['location']['lon'],
    'lat': x['location']['lat'], 
    'rank':x['rank']
},  data_bali['locations'] )))



locations = pd.concat([locations, locations_bali], ignore_index=True).reset_index(drop=True)

locations.to_csv(f'{CSV_DATA}/airportrs.csv', index=False)

airport_ids = list(locations[locations['continent']== 'Europe']['id'])

# add bali
airport_ids.extend(locations_bali['id'])
#airport_ids= airport_ids[0:10]

current_date_string = datetime.now().strftime("%d/%m/%Y")
future_date = datetime.now() + timedelta(days = MAX_PLAN_FORWARD_DAYS)
future_date_string = future_date.strftime("%d/%m/%Y")


data_frames = []


for airport_id in airport_ids:
    try:
        df = get_one_dest(airport_id)
        data_frames.append(df)
    except:
        print(f'error: {airport_id}')

combined_df = pd.concat(data_frames, ignore_index=True)

bali_data = combined_df[combined_df['dest_id'].isin(locations_bali['id'].tolist())]

cheap_combined_df = combined_df[combined_df['price']<50000]

# add back the bali data
df = pd.concat([cheap_combined_df, bali_data], ignore_index=True)



# process_data
def classify_time(hour):
    if hour < 10:
        return 'reggel'
    elif hour >= 17:
        return 'este'
    else:
        return 'napközben'

df['outgoing_start'] = pd.to_datetime(df['outgoing_start'])
df['out_date'] = df['outgoing_start'].dt.date
df['out_time'] = df['outgoing_start'].dt.time
df['out_day'] = df['outgoing_start'].dt.strftime('%A')
df['out_day_time'] = df['outgoing_start'].apply(lambda x: classify_time(x.hour))


df['incomming_start'] = pd.to_datetime(df['incomming_start'])
df['incomming_date'] = df['incomming_start'].dt.date
df['incomming_time'] = df['incomming_start'].dt.time
df['incomming_day'] = df['incomming_start'].dt.strftime('%A')
df['incomming_day_time'] = df['incomming_start'].apply(lambda x: classify_time(x.hour))


df = df.merge(locations[['id', 'country']], left_on='dest_id', right_on='id')


important_columns = {
    'to': 'varos',
    'price': 'ar',
    'night_in_dest' : 'napok',
    'country': 'orszag',
    'out_date': 'indulas',
    'out_day': 'indulas_nap',
    'out_time': 'indulas_ido',
    'out_day_time':'indulas_napszak',
    'incomming_date': 'vissza',
    'incomming_day':'vissza_nap',
    'incomming_time': 'vissza_ido',
    'incomming_day_time': 'vissza_napszak',
    'stop_oda':'atszallas_oda',
    'stop_vissza':'atszallas_vissza',
    'dest_id': 'repter_id',
    'link':'link'
}

# rename
flights = df[list(important_columns.keys())].rename(columns=important_columns)
flights.sort_values(by=['ar', 'varos'],inplace=True)

flights.to_csv(LAST_PRICE, index=False)
flights.to_csv(NEW_FILE, index=False)


# processed data
top3_flights = flights.groupby('varos').head(3)
top3_flights.to_csv(TOP3_FLIGHTS, index=False)
