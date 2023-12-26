import requests
import json
import os
import csv

# get JSON file describing the datasets available
url = "https://data.medicaid.gov/api/1/metastore/schemas/dataset/items/"
response = requests.request("GET", url)

# parse JSON and store as python list
response_dict = response.json()

# store indented JSON in text file
with open("metastore_dataset", "w") as f:
    json.dump(response_dict, f, indent = 4)

# write dataset titles to file for ease of browsing and save to list
try:
   with open('dataset_titles.txt'): pass
except IOError:
   print('File not found...new file will be created')

with open('dataset_titles.txt', "w") as f:
    for count, value in enumerate(response_dict):
        print(f"{count}: {value['title']}", file=f)

print("Medicare datset list file created")

# search through response list to find API links for all NADAC years
response_nadac = list(filter(lambda d: "NADAC (National Average Drug Acquisition Cost)" in d['title'], response_dict))

for d in response_nadac:
    d['year'] = int(d['title'].split()[-1])

response_nadac = sorted(response_nadac, key = lambda x: x['year'], reverse = True)

# pull one row so we can get number of rows from API reponse
# for some reason, I could only find total rows after making this request and extracting 'count'
url = "https://data.medicaid.gov/api/1/datastore/query/" + response_nadac[0]['identifier'] + "/0"
response = requests.request("GET", url)
print(f"There are {response.json()['count']} total records")

# pull either everything or set num_rows and save as csv
num_rows = 100000
with open('NADAC_from_api.csv', 'a', newline = '') as f:
    csv_writer = csv.writer(f)
    i = 0
    while i < num_rows:
        size = 10000
        offset_url = "https://data.medicaid.gov/api/1/datastore/query/" + response_nadac[0]['identifier'] + f"/0?offset={i}"
        offset_response = requests.request("GET", offset_url)
        print(f"Made request for {size} results at offset {i}")

        if i == 0:
            csv_writer.writerow(response.json()['results'][0].keys())
            
        for record in response.json()['results']:
            csv_writer.writerow(record.values())
        
        i = i + size

print("Done")