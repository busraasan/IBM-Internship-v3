import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext, HiveContext
from pyspark.sql.functions import *
from fpdf import FPDF
from PIL import Image
import glob
import matplotlib.cm as cm
import numpy as np

import pymongo
import pandas as pd
import json

def read_from_database(mongo_ip, sqlC):
    iris = sqlC.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", mongo_ip + "Iris").load()
    iris.createOrReplaceTempView("iris")
    iris = sqlC.sql("SELECT * FROM iris")
    #iris = iris.withColumn("Processor", iris["Processor"].cast("string"))
    return iris

def load_to_database(path_to_xlsx, database_name):
    client = pymongo.MongoClient("mongodb://localhost:27017") #connect to local server
    df = pd.read_excel(path_to_xlsx) #read from xlsx file
    data = df.to_dict(orient = "records") #insert data into a dictionary and choose the type of values as records
    db = client[database_name] #construct db
    db.Iris.insert_many(data) #insert data into db

#by creating RDD's manually, optimize the memory used and shorten execution time.
def create_partition(dataframe, sparkcontext):
    dataframe.repartition(6).createOrReplaceTempView('sf_view')
    sparkcontext.catalog.cacheTable('sf_view')
    dataframe = sparkcontext.table('sf_view')
    return dataframe

#Extract a dataframe for every client using _Total values for Processor
def extract_dataframes(dataframe):
    clients = dataframe.withColumn('Server_Name', dataframe['Server_Name'].substr(0, 3)).select('Server_Name').distinct().toPandas()["Server_Name"].values.tolist()
    client_dataframes = {}
    for client in clients:
        client_dataframes[client] = dataframe.filter(dataframe.Server_Name.contains(client)) #take every row for a client which includes total processor usage data.
    return clients, client_dataframes

def extract_dataframes_for_disk(dataframe):
    clients = dataframe.withColumn('Server_Name', dataframe['Server_Name'].substr(0, 3)).select('Server_Name').distinct().toPandas()["Server_Name"].values.tolist()
    client_dataframes = {}

    for client in clients:
        k = dataframe.filter(dataframe.Server_Name.contains(client)) #take every row for a client which includes total processor usage data.
        client_dataframes[client] = {}
        disks = k.withColumn('Disk_Name', k['Disk_Name'].substr(0, 1)).select('Disk_Name').distinct().toPandas()["Disk_Name"].values.tolist()
        for disk in disks:
            client_dataframes[client][disk] = dataframe.filter(dataframe.Disk_Name.contains(disk))
    return clients, client_dataframes