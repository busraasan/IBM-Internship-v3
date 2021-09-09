import matplotlib.pyplot as plt
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext, HiveContext
from pyspark.sql.functions import *
from fpdf import FPDF
from PIL import Image
import glob
import matplotlib.cm as cm
import numpy as np

from pathlib import Path
import pymongo
import pandas as pd
import json
from pdf_template import PDF
from helper_funcs import *
from client_class import Client

WIDTH = 210
HEIGHT = 297
FORCE_GENERATE_CHARTS = True

cursor_y = 0

#HELPER FUNCTIONS
        
#Report template for CPU usage
class ClientReport:
    def __init__(self, client):

        #client object will be passed and graphs related to that client will be printed as a seperate pdf.
        self.client = client

        # self.chart_row = 1
        # self.figure_num = 0
        # self.page_num = 1
        # self.minus = 0
        # self.figure_per_page_num = 0

        self.total_usage = 0

        self.create_pdf()

    def page_for_every_host(self, pdf):
        for host in self.client.hostnames:
            pdf.add_small_title("Charts for "+self.client.client_name+" with host "+host)
            title = "Hourly CPU Usage for " + self.client.client_name + " with host " + host
            title2 = "Weekly CPU Usage for " + self.client.client_name + " with host " + host
            title3 = "Monthly CPU Usage for " + self.client.client_name + " with host " + host
            self.hourly_line_chart(title, self.client.hosts_dataframes[host], host, self.client.client_name)
            self.weekly_bar_chart(title2, self.client.hosts_dataframes[host], host, self.client.client_name)
            self.monthly_line_chart(title3, self.client.hosts_dataframes[host], host, self.client.client_name)
            path = "charts/"+self.client.client_name+host+'.jpg'
            path2 = "charts/weekly"+self.client.client_name+host+'.jpg'
            path3 = "charts/monthly"+self.client.client_name+host+'.jpg'
            pdf.add_two_figures(path, path2)
            pdf.add_single_figure(path3)
            pdf.ln(10)
            pdf.add_info("Total Processor Time", str(self.total_usage))
            self.total_usage = 0
            
    def create_pdf(self):
        pdf = PDF()
        pdf.add_page()
        pdf.add_titre(self.client.client_name, "07/09/2021")
        self.page_for_every_host(pdf)
        pdf.output("cpu-reports/" + self.client.client_name+"-example1.pdf")   

    #a chart that shows weekly CPU usage for an host of a single client. (7 days)
    #Hangi gunler daha cok kullanilmis
    def weekly_bar_chart(self, title, df, hostname, clientname):
        savepath = './charts/weekly'+clientname+hostname+'.jpg'
        
        plt.clf()
        client_days = df.select(dayofweek('HR_Time')).distinct().orderBy('dayofweek(HR_Time)')
        #empty arrays to be filled (will be used in matplotlib graphs)
        labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        x =[]
        y = []
        temp = []
        weeks = 4*24
        
        for aday in client_days.collect():
            x.append(aday[0])
            #her saat icin avg processor time columni topla
            df_for_day = df.filter(dayofweek('HR_Time') == lit(aday[0])).groupBy().avg('AVG_%_Processor_Time')
            temp.append(df_for_day.toPandas()["avg(AVG_%_Processor_Time)"].values.tolist())
            
        for i in range(0,len(temp)):
            y.append(temp[i][0])
            
        y1 = [value for value in y]
            
        plt.rcParams['axes.edgecolor']='#333F4B'
        plt.rcParams['axes.linewidth']=0.8
        plt.rcParams['xtick.color']='#333F4B'
        plt.rcParams['ytick.color']='#333F4B'
    
        plt.xticks(x, labels)
        plt.title(title)
        plt.bar(x,y1,color=(0.2, 0.4, 0.6, 0.6))
        #ax = sns.barplot(y= "Deaths", x = "Causes", data = deaths_pd, palette=("Blues_d"))
        #sns.set_context("poster")
        plt.savefig(savepath)
    
    #a chart that shows hourly CPU usage for an host of a single client (24 hours).
    #Hangi saatler daha cok kullanilmis
    def hourly_line_chart(self, title, df, hostname, clientname):
        savepath = './charts/'+clientname+hostname+'.jpg'
        
        plt.clf()
        client_hours = df.select(hour('HR_Time')).distinct().orderBy('hour(HR_Time)')
        #empty arrays to be filled (will be used in matplotlib graphs)
        x = []
        y = []
        temp = []
        days = 31
        
        for anhour in client_hours.collect():
            x.append(anhour[0]) #insert hour names into array to be used in x axis
            #her saat icin avg processor time columni topla
            df_for_hour = df.filter(hour('HR_Time') == lit(anhour[0])).groupBy().avg('AVG_%_Processor_Time')
            temp.append(df_for_hour.toPandas()["avg(AVG_%_Processor_Time)"].values.tolist())
            
        for i in range(0,len(temp)):
            y.append(temp[i][0])
            
        y1 = [value for value in y]
            
        plt.rcParams['axes.edgecolor']='#333F4B'
        plt.rcParams['axes.linewidth']=0.8
        plt.rcParams['xtick.color']='#333F4B'
        plt.rcParams['ytick.color']='#333F4B'
    
        plt.xticks(x)
        plt.title(title)
        plt.plot(x,y1)
        plt.grid(True)
        plt.savefig(savepath)

    def monthly_line_chart(self, title, df, hostname, clientname):
        savepath = './charts/monthly'+clientname+hostname+'.jpg'
       
        plt.clf()
        client_days = df.select(dayofmonth('HR_Time')).distinct().orderBy('dayofmonth(HR_Time)')
        #empty arrays to be filled (will be used in matplotlib graphs)
        x = []
        y = []
        temp = []
        hours = 24
        
        for aday in client_days.collect():
            x.append(aday[0]) #insert hour names into array to be used in x axis
            #her saat icin avg processor time columni topla
            df_for_day = df.filter(dayofmonth('HR_Time') == lit(aday[0])).groupBy().avg('AVG_%_Processor_Time')
            temp.append(df_for_day.toPandas()["avg(AVG_%_Processor_Time)"].values.tolist())
            
        for i in range(0,len(temp)):
            y.append(temp[i][0])

        for i in range(0, len(y)):
            self.total_usage += y[i]
            
        y1 = [value for value in y]
            
        plt.rcParams['axes.edgecolor']='#333F4B'
        plt.rcParams['axes.linewidth']=0.8
        plt.rcParams['xtick.color']='#333F4B'
        plt.rcParams['ytick.color']='#333F4B'
    
        plt.xticks(x)
        plt.title(title)
        plt.plot(x,y1)
        plt.grid(True)
        fig = plt.gcf()
        fig.set_size_inches(10,3.5)

        plt.savefig(savepath)
        fig.set_size_inches(6.4,4.8, forward=True)

    #a chart that shows total percentage usage of CPU of every host
    #Hangi host daha cok kullanmis
    def all_hosts_pie_chart(self, pdf):
        savepath = "./charts/"+self.client.client_name+'allhosts.jpg'
       
        plt.clf()
        labels = self.client.hostnames
        sizes = []
        d2_sizes = []
        for host in self.client.hostnames:
            df = self.client.hosts_dataframes[host].select('AVG_%_Processor_Time').groupBy().sum()
            d2_sizes.append(df.toPandas()["sum(AVG_%_Processor_Time)"].values.tolist())
        
        for i in range(0,len(d2_sizes)):
            sizes.append(d2_sizes[i][0])

        #fig1, ax1 = plt.subplots(figsize=(3, 3))
        #fig1, ax1 = plt.subplots()
        #fig1.subplots_adjust(0.1,0,1,1)

        colors = cm.rainbow(np.linspace(0, 1, len(sizes)))
        plt.gca().axis("equal")
        plt.pie(sizes, labels=labels, colors=colors, autopct = '%1.1f%%', pctdistance=1.25, labeldistance=0.9, textprops={'fontsize': 8})
        plt.title("Total Percentage Usage per Host in 1 month")
        path = savepath
        #legend_labels = ['%s, %1.1f %%' % (l, s) for l, s in zip(labels, sizes)]
        #plt.legend(pie[0], labels=labels, bbox_to_anchor=(0.5,0.5), loc='center right', fontsize=8)
        #plt.subplots_adjust(left=0.1, bottom=0.1, right=0.11)
        plt.savefig(path)
        width = 3*WIDTH/5
        return path, width
        
class OverallReport:
    def __init__(self, dataframe, clients):
        self.dataframe = dataframe
        self.clients = clients
        self.client_usage()

    def client_usage(self):
        client_hours = []
        cli = []
        client_dataframes = []

        for i in self.clients:
            client_dataframes.append(self.dataframe.filter(self.dataframe.Server_Name.contains(i) & self.dataframe.Processor.contains('_Total')))

        for client in client_dataframes:
            df = client.select('AVG_%_Processor_Time').groupBy().sum()
            client_hours.append(df.toPandas()["sum(AVG_%_Processor_Time)"].values.tolist())

        for c in client_hours:
            cli.append(c[0])

        sizes = cli
        labels = ['aig', 'eti', 'tuv', 'aho', 'zor']

        colors = cm.rainbow(np.linspace(0, 1, len(sizes)))
        plt.gca().axis("equal")
        plt.pie(sizes, labels=labels, colors=colors, autopct = '%1.1f%%', pctdistance=1.25, labeldistance=0.9, textprops={'fontsize': 8})
        plt.title("Total Percentage Usage per Host in 1 month")
        path = "./charts/allclients.jpg"
        plt.savefig(path)
        width = WIDTH/2
        return path, width

    # def client_usage(self):
    #     client_hours = []
    #     cli = []
    #     client_dataframes = []

    #     for i in self.clients:
    #         client_dataframes.append(self.dataframe.filter(self.dataframe.Server_Name.contains(i) & self.dataframe.Processor.contains('_Total')))

    #     for client in client_dataframes:
    #         df = client.select('AVG_%_Processor_Time').groupBy().sum()
    #         client_hours.append(df.toPandas()["sum(AVG_%_Processor_Time)"].values.tolist())

    #     for c in client_hours:
    #         cli.append(c[0])

    #     sizes = cli
    #     labels = ['aig', 'eti', 'tuv', 'aho', 'zor']

    #     fig1, ax1 = plt.subplots(figsize=(3, 3))
    #     fig1.subplots_adjust(0.1,0,1,1)

    #     theme = plt.get_cmap('flag')
    #     ax1.set_prop_cycle("color", [theme(1. * i / len(sizes)) for i in range(len(sizes))])

    #     ax1.pie(sizes, labels=labels, autopct='%1.1f%%', shadow=True, startangle=90, radius=100)
    #     ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    #     plt.title("Total Client Usage")
    #     plt.savefig('client_usage.jpg')


if __name__ == "__main__":
    conf = pyspark.SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").setMaster("local").setAppName("newApp").setAll([("spark.driver.memory", "15g"), ("spark.executer.memory", "20g")])
    sc = SparkContext(conf=conf)

    sqlC = SQLContext(sc)

    spark = SparkSession.builder \
    .master('local[*]') \
    .config("spark.driver.memory", "15g") \
    .appName('newApp') \
    .getOrCreate()

    #load_to_database("/home/basan/Downloads/datdat/data/cpu.xlsx", "basancpu")
    mongo_ip = "mongodb://localhost:27017/basancpu."
    iris = read_from_database(mongo_ip, sqlC)
    dataframe = create_partition(iris, spark)

    clients = [] #names of clients
    client_dataframes = {} #dataframes of clients
    clients, client_dataframes = extract_dataframes(dataframe)
    client_objects = {} #dictionary of client objects

    for client in clients:
        client_objects[client] = Client(client_dataframes[client], spark, client)
        print(client)
        ClientReport(client_objects[client])

