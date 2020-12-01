from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext,SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import *
import sys
import csv
import json
#reading csv files
players=open('players.csv','r')
teams=open('teams.csv','r')
fieldnames_players = ("name","birthArea","birthDate","foot","role","height","passportArea","weight","Id")
fieldnames_teams=("name","Id")
reader1=csv.reader(players,fieldnames_players)#initializing players
reader2=csv.reader(teams,fieldnames_teams)#initializing teams

sc = SparkContext.getOrCreate()#initializing spark context
spark = SparkSession(sc)
ssc = StreamingContext(sc, 5)
dataStream=ssc.socketTextStream("localhost",6100)#connecting to port "6100"
dataStream.foreachRDD(lambda rdd:readMyStream(rdd))#reading datastream from the port

d={}#dictionary containing passaccuracy
d2={}#Duel effectiveness
d3={}#Free kick effectiveness
d4={}#shots
d5={}#foul loss
d6={}#owngoals
#d7={}
t1=0
foul=0
free=0

def readMyStream(rdd):
	if not rdd.isEmpty():
		df = spark.read.json(rdd)#creating dataframe from json objects
		dt = map(lambda x: x.asDict(),df.collect())#converting dataframe to dictionary
		events={}#dictionary for event json 
		match={}#dictionary for match json
		
		for dict1 in dt:
			print(dict1)
			if dict1["eventId"]!=None: 
				events.update(dict1)
				#Pass accuracy
				if events["eventId"]==8:
					l=[]
					acc_norm=0
					non_acc=0
					key_p=0
					norm=0
					acc_key=0
					pass_acc=0
					#print(len(events["tags"]))
					for i in range(len(events["tags"])):
						l.append(events["tags"][i]["id"])
					
					#print("pass",l)
					if(302 not in l):
						norm+=1
						if(1801 in l):
							acc_norm+=1
					if(302 in l):
						key_p+=1
						if(1801 in l):
							acc_key+=1
					#calculating PassAccuracy for each player
					try:
						pass_accuracy=((acc_norm)+(acc_key*2))/(norm+(key_p*2))
					except:
						pass_accuracy=0
					#storing values in the dictionary
					try:	
						d[events['playerId']]=d[events['playerId']]+pass_accuracy
					except:
						d[events['playerId']]=pass_accuracy
							

				#Duel effectiveness
				if events["eventId"]==1:
					l_duel=[]
					w=0
					n=0
					los=0
					for i in range(len(events["tags"])):
						l_duel.append(events["tags"][i]["id"])
					#print(l_duel)
					#if(len(l_duel)!=0):
					if(701 in l_duel):
						los+=1	 
					if(702 in l_duel):
						n+=1
					if(703 in l_duel):
						w+=1
					#to avoid zero division error
					try:
						duel_effective=(w+(n*0.5))/(w+n+los)
					except:
						duel_effective=0
					#storing values in the dictionary
					try:
						d2[events['playerId']]=d2[events['playerId']]+duel_effective
					except:							
						d2[events['playerId']]=duel_effective
				#Free kick effectiveness
				if events["eventId"]==3:
					l_free=[]
					global free
					eff=0
					pen=0
					non_pen1=0
					ineff=0
					non_pen2=0
					free+=1
					for i in range(len(events["tags"])):
						l_free.append(events["tags"][i]["id"])
					#print(l_free)
					#print(l_free)
					if 1802 in l_free:
						if (events["subEventId"]==35):
							ineff+=1
						if (events["subEventId"]!=35):
							non_pen2+=1
					if 1801 in l_free:
						if ((events["subEventId"]==35) and (101 in l_free)):
							pen+=1
						if(events["subEventId"]==35)and(101 not in l_free):
							eff+=1
						if(events["subEventId"]!=35):
							non_pen1+=1
					#calculating Free kick effectiveness for each player
					try:
						free_effective=((eff+pen+non_pen1)+pen)/(ineff+non_pen2+(eff+pen+non_pen1))
					except:
						free_effective=0
					#storing values in the dictionary
					try:
						d3[events['playerId']]=d3[events['playerId']]+free_effective
					except:
						d3[events['playerId']]=free_effective	

				#shots
				if events["eventId"]==10:
					l_shot=[]
					tar_goal=0
					tar_not=0
					global t1
					t1+=1
					t2=0
					t3=0
					for i in range(len(events["tags"])):
						l_shot.append(events["tags"][i]["id"])
					if(1801 in l_shot):
						t1+=1
					if(1802 in l_shot):
						t2+=1
					if(101 in l_shot):
						t3+=1
					if(1801 in l_shot) and(101 in l_shot):
						tar_goal+=1
					if(1801 in l_shot)and (101 not in l_shot):
						tar_not+=1
					#calculating shots for each player
					try:
						shots=((tar_goal+(tar_not*0.5)))/(t1+t2+t3)
					except:
						shots=0
					#storing values in the dictionary
					try:								
						d4[events['playerId']]=d4[events['playerId']]+shots
					except:
						d4[events['playerId']]=shots

				#foul loss
				if events["eventId"]==2:
					l_foul=[]
					global foul
					for i in range(len(events["tags"])):
						l_foul.append(events["tags"][i]["id"])
					#print(l_foul)
					foul +=1
					#storing values in the dictionary
					try:
						d5[events['playerId']]=d5[events['playerId']]+foul
					except:
						d5[events['playerId']]=foul							



				#owngoals
				if events["eventId"]:
					l_goal=[]
					own=0
					for i in range(len(events["tags"])):
						l_goal.append(events["tags"][i]["id"])
					goal=len(l_goal)
					if 102 in l_goal:
						own+=1
					#storing values in the dictionary
					try:
						d6[events['playerId']]=d6[events['playerId']]+own
					except:
						d6[events['playerId']]=own					
				
			else:
				match.update(dict1)#creating a seperate dictionary for match json objectfor match json 	
	
					
ssc.start()#to start computation
ssc.awaitTermination()#wait for computation to terminate

