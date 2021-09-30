# -*- coding: utf-8 -*-
"""
Created on Mon Sep 27 15:06:14 2021

@author: ArnaudBERNAILLE
L'ojectif du code est d'aller chercher dans le blob cegid les derniers fichiers qui ont été requétés.
"""
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from datetime import datetime
from dateutil.parser import parse
import pandas as pd


#_________
# La premiere étape conciste a récupérer la clef SAS d'impulsa à partir des clefs API.
# Le code a été fait à partir de la doc Cegid : CDA doc v2_Accès documents AzureStorage.pdf
# (Fuzeo\FUZEO - Documents\Projet_Interne\PYTHON_blobCegid\Documentation)
#Get token
import requests
url = "https://cda.cegid.cloud/tokenprovider/Token?api-key-Id=305Z75CIAOG9EG5ODAL4DSDD8"
payload={}
headers = {
  'api-key-secret': 'R+pPKG14PQt281hwkBpJBIr1YOgouX+LwOiSjy+uwlk='# La clef secrete de l'api impulsa
}
response = requests.request("GET", url, headers=headers, data=payload)
token = response.json()["accessToken"]

# Get sas key
url = "https://cda.cegid.cloud/storage/api/V1/storages/GetSASTokenLR"
payload={}
headers = {
  'X-TenantId': '90155565', # cf la doc pour savoir comment le récupérer
  'Authorization': 'Bearer ' + token
}

response = requests.request("GET", url, headers=headers, data=payload)
# La clef est composée de la somme de tous ces éléments
clef = response.json()["blobServiceUri"] + response.json()["containerName"] + response.json()["sasToken"]
print(clef)
#_________



print("Azure Blob Storage v" + __version__ + " - Python quickstart sample")

# Sas url est la clef obtenue sur cegid data access (cf doc FUZEO_documentationCodeBlobCegid.ppt)
sas_url = clef
container = ContainerClient.from_container_url(sas_url)
print(container)

# Affichage des noms des blobs présent dans le container
L_nom_blob = [] #La liste des noms des blobs dans le container
blob_list = container.list_blobs()
for blob in blob_list:
    #Le container contiend des blobs en provenance de toutes les DB
    #Il faut donc faire le trie : on ne prend que ceux en provenance de DB508603
    if(blob.name[0:8] == 'DB508603'):
        L_nom_blob.append(blob.name)


df_blobCegid = pd.DataFrame(columns=['nom_blob','fichier_cible','date'])
#nom_blob : le nom du blob tel quel dans le container de cegid
#fichier_cible : le nom de notre fichier par exemple : 	analytiq, journal...
#date : la date de la dernière requet

L_date = []
L_fichier_cible = []
# Nous avons tous les blobs de la BDD DB508603 dans la liste L_nom_blob
# A present nous devons faire apparaitre les dates
# En effet, nous avons un ensemble de fichiers (un par requete) au format par exemple : DB508603/DB508603-cpa-analytiq-20210830094213.csv
# Pour cela nous allons créer un dataframe avec le nom du blob dans le container, la date et le nom du fichier cible
# Le nom du 
for blob in L_nom_blob:
    date = parse(blob[-18:-4]) # On convertie en date la fin de chaque fichiers 
    L_date.append(date)
    #Parfois (merci Cegid) les noms des tables terminent par un ";" et parfois non.
    #Si jamais il termine par un ";" il faut le supprimer
    fc = blob[22:][:-19] # fc pour fichier cible
    if(fc[-1] == ";"): fc = fc[:-1] # On supprime le ";"
    L_fichier_cible.append(fc.upper()) #On ajoute upper pour faire en sorte qu'il n'y est pas de doublon dans les fichiers

# On remplis le dataFrame
df_blobCegid["nom_blob"] = L_nom_blob
df_blobCegid["date"] = L_date
df_blobCegid["fichier_cible"] = L_fichier_cible


#présent maintenant que nous avons un dataFrame plein nous devons récupérer les derniers fichiers (trié par date)
Dict_fichierADL = {} # Est un dictionnaire qui conporte [nom du fichier dans le blob] : [nom du fichier (ex : societe, tiers)]
for nom_fc in df_blobCegid['fichier_cible'].unique():
    Dict_fichierADL[df_blobCegid.loc[df_blobCegid['fichier_cible'] == nom_fc].sort_values(by = 'date', ascending = False).iloc[0,0]] = df_blobCegid.loc[df_blobCegid['fichier_cible'] == nom_fc].sort_values(by = 'date', ascending = False).iloc[0,1]

#Affichage des fichiers que nous allons télécharger :
for fichier in Dict_fichierADL:
    print(fichier)

# A présent que nous avons la liste avec les noms de tous les fichiers que nous cherchons à télécharger nous allons les télécharger
# Create a local directory to hold blob data
local_path = "./data"
#os.mkdir(local_path) # A decommenter lors de la premmière execution


#Avant de télécharger les fichiers dans le dossier cible nous allons supprimer
# tous les fichiers se trouvant dans le dossier
for nom_fichier in os.listdir("./data_rename"):
    os.remove("./data_rename/" + nom_fichier)
    
# Nous supprimons également les fichiers qui sont dans data
for nom_fichier in os.listdir("./data"):
    os.remove("./data/" + nom_fichier)

# Create a file in the local data directory to upload and download
local_file_name = str(uuid.uuid4()) + ".txt"
upload_file_path = os.path.join(local_path, local_file_name)

# Download the blob to a local file
# Add 'DOWNLOAD' before the .txt extension so you can see both files in the data directory
download_file_path = os.path.join(local_path, str.replace(local_file_name ,'.txt', 'DOWNLOAD.txt'))
print("\nDownloading blob to \n\t" + download_file_path)

# Nous allons chercher dans le dictionnaire le nom du fichiers dans le blob pour le télécharger.
# Ensuite nous le renommons pour pour coller à nos standards (format ex : 1_ANALYTIQ)
for fichier in Dict_fichierADL:
    print(fichier)
    print(Dict_fichierADL[fichier])
    with open(download_file_path, "wb") as download_file:
        download_file.write(container.download_blob(fichier).readall())
    os.rename(download_file_path, "data_rename/" + "1_" + Dict_fichierADL[fichier].upper() + ".txt")


#On ecrit dans un fichier la date de l'execution du script
os.remove("./dateDernierExecutionScriptPythonGet.txt")
fichier = open("./dateDernierExecutionScriptPythonGet.txt", "a")
fichier.write("Date de la dernière actualisation : " +  str(datetime.now()))
fichier.close()





