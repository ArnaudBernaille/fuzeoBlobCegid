# -*- coding: utf-8 -*-
"""
Created on Mon Sep 27 15:06:14 2021

@author: ArnaudBERNAILLE
"""
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__


print("Azure Blob Storage v" + __version__ + " - Python quickstart sample")

# Sas url est la clef obtenue sur cegid data access
sas_url = "https://dat831000sta002.blob.core.windows.net/90155565?sv=2019-07-07&sr=c&sig=iMqdai0%2FiI4eBoLZ18Lj4naEmMfzZdKpVkZ1dFoEAWs%3D&se=2021-09-28T13%3A23%3A26Z&sp=rl"
container = ContainerClient.from_container_url(sas_url)
print(container)

# Affichage des noms des blobs présent dans le container
blob_list = container.list_blobs()
#for blob in blob_list:
    #print("\t" + blob.name)



# Create a local directory to hold blob data
local_path = "./data"
#os.mkdir(local_path) # A decommenter lors de la premmière execution

# Create a file in the local data directory to upload and download
local_file_name = str(uuid.uuid4()) + ".txt"
upload_file_path = os.path.join(local_path, local_file_name)


# Download the blob to a local file
# Add 'DOWNLOAD' before the .txt extension so you can see both files in the data directory
download_file_path = os.path.join(local_path, str.replace(local_file_name ,'.txt', 'DOWNLOAD.txt'))
print("\nDownloading blob to \n\t" + download_file_path)

with open(download_file_path, "wb") as download_file:
    download_file.write(container.download_blob("DB618034/DB618034-cpa-ecriture-20210922210035.csv").readall())