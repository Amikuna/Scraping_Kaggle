import requests
import json
import time
import bs4 as bs
import pandas as pd
from config import token, cookie

url = "https://www.kaggle.com/api/i/datasets.DatasetService/SearchDatasets"


#change link to other tags like "computer science", "data visualization"
#we have to change "categoryIds", and to change pages change "page" and link &page=<pg_number>
#when changing link add link &page=1 and change "1" with page number

urls = []
owners = []
dateCreated = []
dateUpdated = []
totalVotes = []
fileType = []
numFiles = []
titles = []
thumbnailImage = []
usability = []
totalSize = []


counter = 1
cond = True
while cond:
    voteButton = []
    fileTypes = []
    dataSource = []
    print("###############################################")
    print(f"page: {counter}")
    print("###############################################")
    payload = json.dumps({
    "page": counter,
    "group": "PUBLIC",
    "size": "ALL",
    "fileType": "ALL",
    "license": "ALL",
    "viewed": "ALL",
    "categoryIds": [
        13208
    ],
    "search": "",
    "sortBy": "HOTTEST",
    "hasTasks": False,
    "includeTopicalDatasets": False
    })
    headers = {
    'authority': 'www.kaggle.com',
    'accept': 'application/json',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json',
    'cookie': cookie,
    'origin': 'https://www.kaggle.com',
    'referer': f'https://www.kaggle.com/datasets?tags=13208-Data+Visualization&page={counter}',
    'sec-ch-ua': '"Opera";v="95", "Chromium";v="109", "Not;A=Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 OPR/95.0.0.0',
    'x-xsrf-token': token
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    data = response.json()
    lists = data["datasetList"]
    dataset = lists['items']
    totalResults = lists["totalResults"]
    if "hasMore" not in data:
        break


    for d in dataset:
        urls.append("https://kaggle.com"+d['datasetUrl'])
        owners.append(d["ownerName"])
        dateCreated.append(d["dateCreated"])
        dateUpdated.append(d["dateUpdated"])
        voteButton.append(d["voteButton"])
        if "commonFileTypes" in d:
            fileTypes.append(d["commonFileTypes"])
        else:
            fileTypes.append(None)
        
        dataSource.append(d["datasource"])
        if "usabilityRating" in d:
            if 'score' in d["usabilityRating"]:
                usability.append(round(float(d["usabilityRating"]['score'])*10,1))
            else:
                usability.append(None)
        else:
            usability.append(None)
    for vote in voteButton:
        if "totalVotes" in vote:
            totalVotes.append(int(vote["totalVotes"]))
        else:
            totalVotes.append(0)

    for file in fileTypes:
        
        if file is not None:
            f_type = ""
            num = 0
            size = 0
            
            for f in file:
                if "fileType" in f:
                    f_type+=f["fileType"]+","
                else:
                    f_type=None
                if "count" in f:
                    num+=int(f["count"])
                else:
                    num=None
                if "totalSize" in f:
                    size+=int(f["totalSize"])
                else:
                    size=None
                
            fileType.append(f_type[:-1])
            numFiles.append(num)
            totalSize.append(size)
        else:
            fileType.append(None)
            numFiles.append(None)
            totalSize.append(None)

    for data in dataSource:
        titles.append(data["title"])
        thumbnailImage.append(data["thumbnailImageUrl"])
    
    if len(usability) == totalResults:
        break
    else:
        counter+=1


dt = {}

dt["date_created"] = dateCreated
dt["date_updated"] = dateUpdated
dt["title"] = titles
dt["owner"] = owners
dt["vote"] = totalVotes
dt["file_type"] = fileType
dt["amount_of_files"] = numFiles
dt["total_size(bytes)"] = totalSize
dt["usability"] = usability
dt["url"] = urls
dt["thumbnail_image"] = thumbnailImage

df = pd.DataFrame(dt)
df = df.set_index("date_created")
df.to_csv(f"data_{int(time.time())}.csv")
