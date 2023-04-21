#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pymongo

from pymongo import MongoClient


# In[3]:


client = MongoClient("mongodb://localhost:27017/")


# In[4]:


db = client['yelp']


# In[5]:


db


# In[6]:


collection = db["reviews"]


# In[7]:


collection


# In[16]:


#update:
collection = db['reviews']
collection.update_one({'review_id': 'l3Wk_mvAog6XANIuGQ9C7Q'}, {'$set': {'stars': 5}})


# In[17]:


#updated review:

collection = db['reviews']
updated_review = collection.find_one({'review_id': 'l3Wk_mvAog6XANIuGQ9C7Q'})
print(updated_review)


# In[54]:


#insert one:

new = {
    "review_id": "12345",
    "text": "Good Service!",
    "stars": 4,
    "business_id": "abcdefgh"
}

result = collection.insert_one(new)
print(result.inserted_id)


# In[55]:


#Retrieve the inserted document:

new_doc = collection.find_one({"_id": result.inserted_id})
print(new_doc)


# In[56]:


#insert multiple:

new = [  
    {"review_id": "188", "text": "Great food", "stars": 5, "business_id": "gdgfu"}, 
    {"review_id": "787",  "text": "Average food", "stars": 3, "business_id": "hbjcbu"},   
    {"review_id": "222", "text": "Terrible food.", "stars": 1, "business_id": "ceugj"}]

result = collection.insert_many(new)
print(result.inserted_ids)


# In[58]:


#Retrieve the inserted documents:

ids = result.inserted_ids
docs = collection.find({"_id": {"$in": ids}})
for doc in docs:
    print(doc)


# In[34]:


#Delete:

result = collection.delete_one({"review_id": "l3Wk_mvAog6XANIuGQ9C7Q"})
print(result.deleted_count)


# In[39]:


#Replace:

new = {
    "review_id": "12345",
    "text": "The patio here looks amazing!",
    "stars": 5,
    "business_id": "abcdefgh"
}

result = collection.replace_one({"review_id": "12345"}, new)
print(result.modified_count)


# In[40]:


#Display the replaced document

updated_review = collection.find_one({'review_id': '12345'})
print(updated_review)


# In[ ]:




