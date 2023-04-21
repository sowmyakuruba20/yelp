#!/usr/bin/env python
# coding: utf-8

# In[ ]:


pip install redis


# In[1]:


import json
import redis


# In[3]:


import pandas as pd
import json

with open('/Users/admin/Desktop/yelp/yelp_academic_dataset_business.json') as f:
    data = [json.loads(line) for line in f]

business = pd.DataFrame(data, index=range(len(data)))
business


# In[40]:


import json
import redis

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Load the data from the JSON file
with open('/Users/admin/Desktop/yelp/yelp_academic_dataset_business.json', 'r') as f:
    for line in f:
        # Load the JSON object from the line
        item = json.loads(line)
        # Use the business ID as the key
        key = item['business_id']
        # Convert the item to a JSON string and store it
        value = json.dumps(item)
        r.set(key, value)


# In[5]:


import redis
import json

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Retrieve data from Redis and decode from JSON
for key in r.scan_iter():
    value = json.loads(r.get(key))
    print(value)


# In[9]:


import json
import random
import string

# Generate a random ID
id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=22))

# Create a dictionary for the new business entry
new_business = {
    "business_id": id,
    "name": "New Business",
    "address": {
        "street": "123 Main St",
        "city": "Anytown",
        "state": "CA",
        "postal_code": "12345"
    },
    "stars": 4.5,
    "review_count": 0,
    "categories": ["Restaurants"]
}

# Convert the dictionary to a JSON string
new_business_json = json.dumps(new_business)

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Store the new entry in Redis
r.set(id, new_business_json)


# In[41]:


import redis

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Get the total number of businesses
total_businesses = r.dbsize()
print(f'Total businesses: {total_businesses}')

# Get the number of businesses in each city
city_counts = {}
for key in r.scan_iter('*city*'):
    city = r.hget(key, 'city').decode('utf-8')
    if city in city_counts:
        city_counts[city] += 1
    else:
        city_counts[city] = 1

print('Number of businesses in each city:')
for city, count in city_counts.items():
    print(f'{city}: {count}')

# Get the number of businesses in each state
state_counts = {}
for key in r.scan_iter('*state*'):
    state = r.hget(key, 'state').decode('utf-8')
    if state in state_counts:
        state_counts[state] += 1
    else:
        state_counts[state] = 1

print('Number of businesses in each state:')
for state, count in state_counts.items():
    print(f'{state}: {count}')


# In[42]:


import redis

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Get the number of businesses in the database
num_businesses = r.dbsize()
print("Number of businesses:", num_businesses)

# Count the number of businesses in a specific state
state = 'AZ'
num_businesses_in_state = 0
for key in r.scan_iter("business:*"):
    business_data = r.hgetall(key)
    if business_data[b'state'].decode('utf-8') == state:
        num_businesses_in_state += 1
print("Number of businesses in", state, ":", num_businesses_in_state)

# Count the number of businesses in a specific city
city = 'Phoenix'
num_businesses_in_city = 0
for key in r.scan_iter("business:*"):
    business_data = r.hgetall(key)
    if business_data[b'city'].decode('utf-8') == city:
        num_businesses_in_city += 1
print("Number of businesses in", city, ":", num_businesses_in_city)


# In[ ]:


import redis
import requests
import time

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Set up Alpha Vantage API key and symbol
api_key = 'SO6CTEA99HG2780W'
symbol = 'GOOGL'

# Loop forever, fetching real-time stock prices and storing them in Redis
while True:
    # Fetch real-time stock prices from Alpha Vantage API
    url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={api_key}'
    response = requests.get(url)
    data = response.json()

    # Store the stock price in Redis
    price = float(data['Global Quote']['05. price'])
    r.set('stock_price', price)

    # Perform some simple operations on the data
    print(f"Stock price for {symbol}: {price}")
    if r.exists('stock_price'):
        avg_price = float(r.get('stock_price')) / float(r.get('num_prices') or 1)
        print(f"Average stock price: {avg_price}")
    else:
        print("No data in Redis yet.")

    # Increment the number of prices stored in Redis
    r.incr('num_prices')

    # Wait for 1 minute before fetching the next data point
    time.sleep(60)


# In[20]:


import redis

# create a Redis client
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# specify the key of the record to delete
key = 'business_id:random1234'

# delete the record
redis_client.delete(key)


# In[21]:


import redis

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Update multiple fields
r.hmset('business:random1234', {'name': 'Nikhil Business', 'city': 'Hyderabad'})


# In[27]:


import redis

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Retrieve the value of a single field
value = r.hget('business:random1234','name')
print(value)

# Retrieve the values of multiple fields
values = r.hmget('business:random1234', 'city','name')
print(values)


# In[29]:


import redis

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Create a pipeline
pipe = r.pipeline()

# Set a key-value pair
pipe.set('key', 'value')

# Execute the pipeline
pipe.execute()

# Close the connection
r.connection_pool.disconnect()


# In[32]:


import redis

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Retrieve the value of a single field
value = r.hget('business:random1234','name')
print(value)

# Retrieve the values of multiple fields
values = r.hmget('business:random1234', 'city','name')
print(values)

pipe = r.pipeline()
pipe.execute()
r.connection_pool.disconnect()


# In[35]:


import json

# Load the business data from the JSON file
with open('yelp_academic_dataset_business.json', 'r') as f:
    businesses = json.load(f)

# Print the number of businesses loaded
print(f"Loaded {len(businesses)} businesses")

# Insert the data into Redis
for business in businesses:
    r.hset('businesses', business['business_id'], json.dumps(business))
    r.sadd('cities', business['city'])


# In[39]:


import json

# Load the business data from the JSON file
with open('yelp_academic_dataset_business.json', 'r') as f:
    data = f.read()
    businesses = json.loads(data)

# Print the number of businesses loaded
print(f"Loaded {len(businesses)} businesses")

# Insert the data into Redis
for business in businesses:
    r.hset('businesses', business['business_id'], json.dumps(business))
    r.sadd('cities', business['city'])


# In[ ]:




