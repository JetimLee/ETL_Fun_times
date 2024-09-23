import requests
import zipfile
import os
import pandas as pd
import xml.etree.ElementTree as ET 
import glob 
from datetime import datetime 
target_file = "transformed_data_practice_etl.csv" 


# URL of the zip file
url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip"

# Folder to save the zip file and extract the content
output_dir = "extracted_data"
log_file = os.path.join(output_dir, "log_file_practice_etl.txt")

zip_file_path = os.path.join(output_dir, "datasource.zip")

# Step 1: Create the output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Step 2: Stream the file to avoid memory issues with large files
with requests.get(url, stream=True) as response:
    response.raise_for_status()  # Check if the download was successful
    with open(zip_file_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

# Step 3: Extract the zip file into the output directory
with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
    zip_ref.extractall(output_dir)

# Step 4: Optionally, remove the zip file after extraction
os.remove(zip_file_path)

print(f"Download and extraction complete! Files saved in {output_dir}.")


def extract_from_csv(file_to_process): 
    dataframe = pd.read_csv(file_to_process) 
    return dataframe 

def extract_from_json(file_to_process): 
    dataframe = pd.read_json(file_to_process, lines=True) 
    return dataframe 

def extract_from_xml(file_to_process): 
    dataframe = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price","fuel"]) 
    tree = ET.parse(file_to_process) 
    root = tree.getroot() 
    for person in root: 
        car_model = person.find("car_model").text 
        year_of_manufacture = float(person.find("year_of_manufacture").text) 
        price = float(person.find("price").text) 
        fuel = person.find("fuel").text

        dataframe = pd.concat([dataframe, pd.DataFrame([{"car_model":car_model, "year_of_manufacture":year_of_manufacture, "price":price,"fuel":fuel}])], ignore_index=True) 
    return dataframe 


def extract(): 
    extracted_data = pd.DataFrame(columns=['car_model','year_of_manufacture','price',"fuel"]) # create an empty data frame to hold extracted data 
     
    # process all csv files 
    for csvfile in glob.glob(os.path.join(output_dir, "*.csv")): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True) 
         
    # process all json files 
    for jsonfile in glob.glob(os.path.join(output_dir, "*.json")): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True) 
     
    # process all xml files 
    for xmlfile in glob.glob(os.path.join(output_dir, "*.xml")): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True) 
         
    return extracted_data 


def transform(data): 
    '''Round car price to 2 decimals '''

    data['price'] = round(data["price"],2) 
    
    return data 

def load_data(target_file, transformed_data): 
    transformed_data.to_csv(target_file) 

def log_progress(message): 
    timestamp_format = '%Y-%b-%d-%H:%M:%S'   
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n')


# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file,transformed_data) 
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 

