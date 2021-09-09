import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext, HiveContext
from pyspark.sql.functions import *
import pymongo
import pandas as pd

class Client:
    def __init__(self, client_dataframe, sparkcontext, client_name):
        self.client_dataframe = client_dataframe
        self.sparkcontext = sparkcontext

        self.client_name = client_name
        self.hostnames = self.client_dataframe.withColumn('Server_Name', self.client_dataframe['Server_Name'].substr(5, 1)).select('Server_Name').distinct().toPandas()["Server_Name"].values.tolist()
        self.hostnames = sorted(self.hostnames) #sort hostnames alphabetically
        
        self.hosts_dataframes = {} #Holds dataframes for every host of a client. Keys are hostnames.

        for host in self.hostnames:
            self.hosts_dataframes[host] = self.client_dataframe.filter(self.client_dataframe.Server_Name.contains(host)) #take every row includes the name of the host
        
class DiskClient:
    def __init__(self, client_dataframe, sparkcontext, client_name):
        self.client_dataframe = client_dataframe
        self.sparkcontext = sparkcontext

        self.client_name = client_name
        self.hostnames = self.client_dataframe.withColumn('Server_Name', self.client_dataframe['Server_Name'].substr(5, 1)).select('Server_Name').distinct().toPandas()["Server_Name"].values.tolist()
        self.hostnames = sorted(self.hostnames) #sort hostnames alphabetically
        print(client_name, self.hostnames)
        self.hosts_dataframes = {} #Holds dataframes for every host of a client. Keys are hostnames.
        self.hosts_dataframes_w = {}
        for host in self.hostnames:
            k = self.client_dataframe.filter(self.client_dataframe.Server_Name.contains(host)) #take every row includes the name of the host
            self.hosts_dataframes_w[host] = k
            self.hosts_dataframes[host] = {}
            disks = k.withColumn('Disk_Name', k['Disk_Name'].substr(0, 1)).select('Disk_Name').distinct().toPandas()["Disk_Name"].values.tolist()
            for disk in sorted(disks):
                self.hosts_dataframes[host][disk] = k.filter(k.Disk_Name.contains(disk))