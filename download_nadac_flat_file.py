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

# make request for csv for most recent NADAC year and write to file via streaming
url = response_nadac[0]['distribution'][0]['downloadURL']

with requests.get(url, stream=True, allow_redirects=True) as r:
    r.raise_for_status()
    with open(f"NADAC_{response_nadac[0]['year']}.csv", "wb") as f:
        for chunk in r.iter_content(chunk_size=8192): 
            f.write(chunk)