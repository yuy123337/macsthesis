import os
import shutil

# List of paths to delete
paths = ["/content/data", "/content/result", "/content/history.log"]

for path in paths:
    if os.path.exists(path):
        # If it's a directory, use shutil.rmtree to delete it
        if os.path.isdir(path):
            shutil.rmtree(path)
            print(f"Directory {path} has been deleted.")
        # If it's a file, use os.remove to delete it
        elif os.path.isfile(path):
            os.remove(path)
            print(f"File {path} has been deleted.")
    else:
        print(f"{path} does not exist and was not deleted.")

import os

# Create directories
os.makedirs('data', exist_ok=True)
os.makedirs('result', exist_ok=True)

# Create a log file
with open('history.log', 'w') as f:
    f.write('Log file created.\n')



# -*- coding: utf-8 -*-
"""
Created on Mon Feb  5 10:23:27 2024

@author: Admin
"""

import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor
import csv
from urllib import parse
from threading import Lock

from langdetect import detect, DetectorFactory
DetectorFactory.seed = 0

lock = Lock()

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

def get_comments(para, para2=''):
    count = 0
    url = "https://www.google.com/maps/rpc/listugcposts"
    while 1:
        params = {
            "authuser": "0",
            "hl": "en-US",
            "gl": "us",
            "pb": f"!1m7!1s{para}!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s{para2}!3e1!5m2!1sdUXAZZXwC4HE0PEP_NSiqA8!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3sen-US!4sus!6m1!1i2"
        }
        response = requests.get(url, headers=headers, params=params, timeout=(7, 15))
        res0 = response.text.split('\n')[1]
        data_dict = json.loads(res0)
        para2 = data_dict[1]
        comments_list = data_dict[2]
        for comments in comments_list:
            try:
                text = comments[0][2][-1][0][0]
                text = text.replace('\n','')
            except:
                text = ''
            print(text)
            try:
                date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(comments[0][1][2]/1000000))
            except:
                date = ''
            print(date)
            try:
                img_list = comments[0][2][2]
                for img in img_list:
                    img_url = f'https://lh3.ggpht.com/p/{img[0]}'
            except:
                img_list = []
            print('-'*50)
            count += 1
        if para2 == None:
            break
        print(count)

def get_search_list(keyword, lat, lon, file_path):
    global history_list
    keyword_str = parse.quote(keyword)
    page = 0
    z = 448792
    url = f"https://www.google.com/search?tbm=map&authuser=0&hl=en-US&gl=us&pb=!4m12!1m3!1d{z}!2d{lon}!3d{lat}!2m3!1f0!2f0!3f0!3m2!1i1920!2i637!4f13.1!7i20!8i0!10b1!12m16!1m1!18b1!2m3!5m1!6e2!20e3!10b1!12b1!13b1!16b1!17m1!3e1!20m3!5e2!6b1!14b1!19m4!2m3!1i360!2i120!4i8!20m57!2m2!1i203!2i100!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m42!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e9!2b1!3e2!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!2b1!4b1!9b0!22m6!1sSlrAZfGlA8-S0PEPk52koA8%3A3007!2s1i%3A0%2Ct%3A20588%2Cp%3ASlrAZfGlA8-S0PEPk52koA8%3A3007!4m1!2i20588!7e81!12e3!24m94!1m29!13m9!2b1!3b1!4b1!6i1!8b1!9b1!14b1!20b1!25b1!18m18!3b1!4b1!5b1!6b1!9b1!12b1!13b1!14b1!15b1!17b1!20b1!21b1!22b1!25b1!27m1!1b0!28b0!31b0!10m1!8e3!11m1!3e1!14m1!3b1!17b1!20m2!1e3!1e6!24b1!25b1!26b1!29b1!30m1!2b1!36b1!39m3!2m2!2i1!3i1!43b1!52b1!54m1!1b1!55b1!56m2!1b1!3b1!65m5!3m4!1m3!1m2!1i224!2i298!71b1!72m17!1m5!1b1!2b1!3b1!5b1!7b1!4b1!8m8!1m6!4m1!1e1!4m1!1e3!4m1!1e4!3sother_user_reviews!9b1!89b1!103b1!113b1!114m3!1b1!2m1!1b1!117b1!122m1!1b1!26m4!2m3!1i80!2i92!4i8!30m28!1m6!1m2!1i0!2i0!2m2!1i530!2i637!1m6!1m2!1i1870!2i0!2m2!1i1920!2i637!1m6!1m2!1i0!2i0!2m2!1i1920!2i20!1m6!1m2!1i0!2i617!2m2!1i1920!2i637!31b1!34m18!2b1!3b1!4b1!6b1!8m6!1b1!3b1!4b1!5b1!6b1!7b1!9b1!12b1!14b1!20b1!23b1!25b1!26b1!37m1!1e81!42b1!46m1!1e10!47m0!49m7!3b1!6m2!1b1!2b1!7m2!1e3!2b1!50m26!1m21!2m7!1u3!4z6JCl5Lia5Lit!5e1!9s0ahUKEwiF2tqos5OEAxX8ADQIHZgiDgUQ_KkBCNUFKBY!10m2!3m1!1e1!2m7!1u2!4z6K-E5YiG5pyA6auY!5e1!9s0ahUKEwiF2tqos5OEAxX8ADQIHZgiDgUQ_KkBCNYFKBc!10m2!2m1!1e1!3m1!1u3!3m1!1u2!4BIAE!2e2!3m2!1b1!3b1!59BQ2dBd0Fn!61b1!67m2!7b1!10b1!69i680&q={keyword_str}&tch=1&ech=35&psi=SlrAZfGlA8-S0PEPk52koA8.1707104844998.1"
    result = 0

    while True:
        try:
            response = requests.get(url, headers=headers, timeout=(7, 15))
            res0 = response.text.split('/*""*/')[0]
            res0_dict = json.loads(res0)
            data_dict = json.loads(res0_dict["d"].split('\n')[1])

            search_list = data_dict[0][1]
            if not search_list or len(search_list) <= 1:
                break

            for search in search_list:
                if len(search) == 15:
                    para = search[14][10]
                    if para in history_list:
                        continue

                    name = search[14][11]
                    tags = '|'.join(search[14][13]) if search[14][13] else ''
                    
                    # Unified filter for both "chinese restaurant" and "japanese restaurant"
                    if keyword in ["chinese restaurant", "japanese restaurant"]:
                        if not (keyword.split()[0] in name.lower() or keyword.split()[0] in tags.lower()):
                            continue

                    # Extract other fields
                    try:
                        score = search[14][4][7]
                        comments_num = search[14][4][8]
                    except:
                        score = ''
                        comments_num = 0

                    addr = search[14][39]
                    country = search[14][30]
                    latt = search[14][9][2]
                    lonn = search[14][9][3]
                    score = search[14][4][7] if len(search[14]) > 4 and len(search[14][4]) > 7 else ''
                    comments_num = search[14][4][8] if len(search[14]) > 4 and len(search[14][4]) > 8 else 0

                    # Acquire lock and write to CSV file
                    lock.acquire()
                    
                    with open(file_path, mode='a', encoding='utf-8-sig', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow([keyword, lat, lon, page, para, name, score, comments_num,
                                          addr, country, latt, lonn, tags])
                    history_list.append(para)
                    with open('history.log', mode='a', encoding='utf-8-sig', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow([para])
                    lock.release()

                

            # Update URL to request next page
            url = url.replace(f'!8i{page * 20}!', f'!8i{page * 20 + 20}!') if f'!8i{page * 20}' in url else url.replace('!7i20', f'!7i20!8i{page * 20 + 20}')
            page += 1
            print('Processing page:', keyword, lat, lon, page)

        except Exception as e:
            result += 1
            print(f"Error processing {keyword}, {lat}, {lon}, page {page}: {e}")
            if result >= 20:
               break
            continue

        print('Completed:', keyword, lat, lon)


# revise to restaruant...
#keyword_list = ['church','cathedral','chapel','mosque','temple','place of worship']

keyword_list = ['chinese restaurant','japanese restaurant']

#keyword_list = ['concert halls','theater','cinema']


#Have to revise, it's for the city shapefile
#west_lon, south_lat, east_lon, north_lat = -88.463634, 39.879092, -87.92876, 40.400612 (champaign)
#lon_gap = 0.06  # You can adjust these gaps to fine-tune the granularity
#lat_gap = 0.025

#west_lon, south_lat, east_lon, north_lat = -88.463634, 39.879092, -87.92876, 40.400612 #champaign
west_lon, south_lat, east_lon, north_lat =-87.940101, 41.644335, -87.523993, 42.023131 #chicago
lon_gap = 0.06
lat_gap = 0.025
coor_list = []
while west_lon < east_lon + lon_gap:
    south_lat_temp = south_lat
    while south_lat_temp > north_lat:
        coor_list.append([south_lat_temp, west_lon])
        south_lat_temp -= lat_gap
    coor_list.append([north_lat, west_lon])
    west_lon += lon_gap

history_list = []
with open('history.log', mode='r', encoding='utf-8-sig') as f:
    lines = f.readlines()
for line in lines:
    history_list.append(line.strip())

with ThreadPoolExecutor(70) as t:
  for keyword in keyword_list:
        # Update file_path to include the specified directory
        file_path = f'/content/data/{keyword}_result.csv'

        # Open the CSV file for writing in the specified directory
        with open(file_path, mode='w', encoding='utf-8-sig', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['keyword', 'lat', 'lon', 'page', 'para', 'name', 'score', 'comments_num','addr', 'country', 'latt', 'lonn', 'tags'])
            

        # Submit tasks to the thread pool
        for lat, lon in coor_list:
            t.submit(get_search_list, keyword, lat, lon, file_path)

        print(f'Keyword processing completed for: {keyword}')

#from langdetect import detect, DetectorFactory
#DetectorFactory.seed = 0  # Optional: Set seed for reproducibility
#def detect_city(lat, lon):
    #for city, bounds in city_boundaries.items():
        #if bounds[0] <= lat <= bounds[2] and bounds[1] <= lon <= bounds[3]:
            #return city
    #return "Unknown"

import pandas as pd
import os

def combine_csv_files(directory, output_filename):
    # Create a list to store each DataFrame
    dfs = []

    # Iterate through each file in the specified directory
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            dfs.append(pd.read_csv(file_path))

    # Concatenate all DataFrames in the list
    combined_df = pd.concat(dfs, ignore_index=True)

    # Save the combined DataFrame to a new CSV file
    combined_df.to_csv(os.path.join(directory, output_filename), index=False)

def clean_data(file_path):
    # Load the combined CSV file
    data = pd.read_csv(file_path)

    # Drop duplicate rows based on specific columns
    columns_to_check = ['name','addr']
    data = data.drop_duplicates(subset=columns_to_check, keep='first')

    # Save the cleaned data back to the same file
    data.to_csv(file_path, index=False)

# Define the directory containing the CSV files and the name of the output file
directory = f'/content/data'
output_filename = f'/content/data/city.csv'

# Combine the CSV files
combine_csv_files(directory, output_filename)

# Path to the combined CSV file
combined_file_path = os.path.join(directory, output_filename)

# Clean the data
clean_data(combined_file_path)

import pandas as pd

def calculate_comments_sum(file_path):
    # Load the CSV file into a DataFrame
    data = pd.read_csv(file_path)

    # Ensure that the 'comments_num' column is treated as integers. Convert errors to NaN which will then be ignored in the sum
    data['comments_num'] = pd.to_numeric(data['comments_num'], errors='coerce')

    # Calculate the sum of the 'comments_num' column, ignoring NaN values
    total_comments = data['comments_num'].sum()

    return total_comments

# Define the path to the CSV file
file_path = f'/content/data/city.csv'

# Calculate the sum of comments
total_comments = calculate_comments_sum(file_path)
print(f"Total comments: {total_comments}")



# -*- coding: utf-8 -*-
"""
Created on Mon Feb  5 10:23:27 2024

@author: Admin
"""

import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import csv
import pandas as pd


lock = Lock()
headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}
#city




def get_comments(para, name, lat, lon, addr, keyword, para2=''): #lock
    # count = 0
    url = "https://www.google.com/maps/rpc/listugcposts"
    result = 0
    while 1:
        try:
            params = {
                "authuser": "0",
                "hl": "zh-CN",
                "gl": "us",
                "pb": f"!1m7!1s{para}!3s!6m4!4m1!1e1!4m1!1e3!2m2!1i10!2s{para2}!3e2!5m2!1sdUXAZZXwC4HE0PEP_NSiqA8!7e81!8m5!1b1!2b1!3b1!5b1!7b1!11m6!1e3!2e1!3szh-CN!4sus!6m1!1i2!13m1!1e1"
            }
            response = requests.get(url, headers=headers, params=params, timeout=(7, 15))
            res0 = response.text.split('\n')[1]
            data_dict = json.loads(res0)
            comments_list = data_dict[2]
            for comments in comments_list:
                try:
                    text = comments[0][2][-1][0][0]
                    text = text.replace('\n','')
                except:
                    text = ''
                # print(text)

                # Detect the language of the comment
                language = ''
                if text:
                    try:
                        language = detect(text)
                    except:
                        language = 'unknown'

                try:
                    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(comments[0][1][2]/1000000))
                except:
                    date = ''
                # print(date)
                try:
                    score = comments[0][2][0][0]
                except:
                    score = ''
                
                if text and name and lat and lon and addr:
                # print(score)
                # try:
                #     img_list = comments[0][2][2]
                #     for img in img_list:
                #         img_url = f'https://lh3.ggpht.com/p/{img[0]}'
                # except:
                #     img_list = []
                # print('-'*50)
                # count += 1
                  lock.acquire()
                  try:
                      with open(file_path, mode='a', encoding='utf-8-sig', newline='') as csvfile:
                          writer = csv.writer(csvfile, escapechar='\\', quoting=csv.QUOTE_MINIMAL)
                          writer.writerow([name, lat, lon, text, date, score, keyword, addr, language])
                  except Exception as e:
                      print(f"Error writing to file: {e}")
                  finally:
                      lock.release()
            para2 = data_dict[1]
            if para2 == None:
                break
            print(para, name, lat, lon, para2)
        except Exception as e:
            result += 1
            print(file_path, e)
            if result >= 20:
                break
            print(para, name, lat, lon, para2)
            continue
        # print(count)

df = pd.read_csv(f'/content/data/city.csv')
df.drop(columns=['lat', 'lon', 'page', 'comments_num'], inplace=True)
df.drop_duplicates(inplace=True)

file_path = f'/content/result/city.csv'

with open(file_path, mode='w', encoding='utf-8-sig', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['name', 'lat', 'lon', 'addr', 'text', 'date', 'score', 'keyword', 'language'])
with ThreadPoolExecutor(50) as t:
    for i in range(len(df)):
        para = df.at[i, 'para']
        name = df.at[i, 'name']
        lat = df.at[i, 'latt']
        lon = df.at[i, 'lonn']
        addr = df.at[i, 'addr']
        keyword = df.at[i, 'keyword']
        t.submit(get_comments, para, name, lat, lon, addr, keyword)

import pandas as pd
import os
import shutil

# Load the data from the provided file path
input_file_path = '/content/result/city.csv'
output_file_path = '/content/result/all_restaurants.csv'

# Load and clean the data
data = pd.read_csv(input_file_path)
data['text'] = data['text'].astype(str).fillna('')
data = data[data['text'].str.strip() != '']
data = data[data['text'].str.strip().replace('nan', '').str.len() > 0]
data = data[~data['text'].str.strip().replace('nan', '').str.startswith('AF1')]
data.to_csv(output_file_path, index=False)
print("Rows with empty 'text' have been removed and the cleaned data is saved.")

# Rename the original file
original_file_path = '/content/result/city.csv'
new_file_path = '/content/result/mid_all_restaruants.csv'
os.rename(original_file_path, new_file_path)
print("File has been renamed successfully.")

# Path to the source and destination folders
source_folder_path = '/content/result'
drive_folder_path = '/content/drive/MyDrive/thesisdata/all_cities/' # change the location of file.

# Ensure the destination directory exists (creates it if it doesn't)
os.makedirs(drive_folder_path, exist_ok=True)

# Copy the contents of the folder to Google Drive without overwriting existing files
for item in os.listdir(source_folder_path):
    source_path = os.path.join(source_folder_path, item)
    destination_path = os.path.join(drive_folder_path, item)
    if ".ipynb_checkpoints" in source_path:
        continue
    if os.path.isdir(source_path):
        if not os.path.exists(destination_path):
            shutil.copytree(source_path, destination_path)
    else:
        if not os.path.exists(destination_path):
            shutil.copy2(source_path, destination_path)

print("Files copied successfully.")