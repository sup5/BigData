from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext,SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import *
import sys
import csv
import json
players=open('players.csv','r')
teams=open('teams.csv','r')
fieldnames_players = ("name","birthArea","birthDate","foot","role","height","passportArea","weight","Id")
fieldnames_teams=("name","Id")
reader1=csv.reader(players,fieldnames_players)#initializing players
next(reader1)
reader2=csv.reader(teams,fieldnames_teams)#initializing teams
next(reader2)
sc = SparkContext.getOrCreate()#initializing spark context
spark = SparkSession(sc)
ssc = StreamingContext(sc, 5)
dS=ssc.socketTextStream("localhost",6100)#connecting to port "6100"
data=dS.foreachRDD(lambda rdd:readMyStream(rdd))#reading datastream from the port
#d={}
final={}
#reading streaming data
def readMyStream(rdd):
	if not rdd.isEmpty():
		teams_data=[]
		#l=[]
		match={}
		events={}
		for i in rdd.collect():
			dt=json.loads(i)
			if "eventId" in dt:
				events.update(dt)
				pass_events(dt,match)
			else:
				match.update(dt)
				player_profile(match)
				#print(teams_data)
				continue
#[[{'playerIn': 217078, 'playerOut': 14763, 'minute': 72}, {'playerIn': 285508, 'playerOut': 192748, 'minute': 82}, {'playerIn': 283142, 'playerOut': 8013, 'minute': 88}], [{'playerIn': 26010, 'playerOut': 370224, 'minute': 67}, {'playerIn': 7870, 'playerOut': 120339, 'minute': 67}, {'playerIn': 7879, 'playerOut': 7945, 'minute': 75}]]
bench=[]
def player_profile(match):
	for row in reader1:
		for row2 in reader2:
			if row2[1] in match["teamsData"]:
				for i in match["teamsData"][row2[1]]["formation"]["bench"]:
					if i["playerId"]==row[8]:
						bench.append(i["playerId"])

			 


"""
def player_cont(d1,d2,d3,d4,dt,match):
	teams_data=[]
	for row in reader1:
		pl_id=int(pl_id)
		
	player_Cont==(d1[dt["playerId"]]+d2[dt["playerId"]]+d3[dt["playerId"]]+d4[dt["playerId"]])/4
	try:
		cont[dt["playerId"]]=cont[dt["playerId"]]+player_Cont
	except:
		cont[dt["playerId"]]=player_Cont
	for id1 in reader2:
		if id1[1] in match["teamsData"]:
		#print(match["teamsData"][id1[1]])
			up=match["teamsData"][id1[1]]["formation"]['substitutions']
			teams_data.append(up)	
	for i in teams_data:
		for j in i:
			if j["playerIn"] in
"""
#function implementation
def pass_events(dt,match):
	if(dt["eventId"]==8):
		d1=pass_accuracy(dt)#pass_accuracy
	if(dt["eventId"]==1):
		d2=duel_effectiveness(dt)#duel effectiveness
	if(dt["eventId"]==3):
		d3=free_kick(dt)#free kick
	if(dt["eventId"]==10):
		d4=shots(dt)#shots
	if(dt["eventId"]):
		d5=own_goals(dt)#own goals
	

d_pass={}
def pass_accuracy(dt):
	acc_norm=0
	non_acc=0
	key_p=0
	norm=0
	acc_key=0
	pass_acc=0
	#if(dt["eventId"]==8):
	for i in dt["tags"]:
		if i["id"]==302:
			key_p+=1
			for j in dt["tags"]:
				if(j["id"]==1801):
					acc_key+=1
		if i["id"]==1801:
			acc_norm+=1
			for j in dt["tags"]:
				if(j["id"]!=302):
					norm+=1
	
	pass_accuracy=((acc_norm)+(acc_key*2))/(1+norm+(key_p*2))
	try:	
		d_pass[dt['playerId']]=d_pass[dt['playerId']]+pass_accuracy
	except:
		d_pass[dt['playerId']]=pass_accuracy

	#print(d_pass)
	return d_pass

d_duel={}
duel=0
def duel_effectiveness(dt):				
	w=0				
	n=0
	los=0
	global duel
	duel+=1
	#if dt["eventId"]==1:
	for i in dt["tags"]:				
		if(i["id"]==701):
			los+=1	 
		if(i["id"]==702):
			n+=1
		if(i["id"]==703):
			w+=1
						#to avoid zero division error
	duel_effective=(w+(n*0.5))/(w+n+los)				#storing values in the dictionary
	try:
		d_duel[dt['playerId']]=d_duel[dt['playerId']]+duel_effective
	except:							
		d_duel[dt['playerId']]=duel_effective
	
	return d_duel

d_free={}
def free_kick(dt):
	#global free
	eff=0
	pen=0
	non_pen1=0
	ineff=0
	non_pen2=0
	#free+=1	
	#if dt["eventId"]==3:
	for i in dt["tags"]:				
		if i["id"]==1802:
			if (dt["subEventId"]==35):
				ineff+=1
			if (dt["subEventId"]!=35):
				non_pen2+=1
		if i["id"]==1801:
			if((dt["subEventId"]==35) and (i["id"]==101)):
				pen+=1
			if(dt["subEventId"]==35)and(i["id"]!=101):
				eff+=1
			if(dt["subEventId"]!=35):
				non_pen1+=1
					
	free_effective=((eff+pen+non_pen1)+pen)/(ineff+non_pen2+1+(eff+pen+non_pen1))
					
	try:
		d_free[dt['playerId']]=d_free[dt['playerId']]+free_effective
	except:
		d_free[dt['playerId']]=free_effective	

	return d_free

d_shots={}
def shots(dt):
	tar_goal=0
	tar_not=0
	#global t1
	t1=0
	t2=0
	t3=0
	#if dt["eventId"]==10:
	for i in dt["tags"]:
		if(i["id"]==1801):
			t1+=1
		if(i["id"]==1802):
			t2+=1
		if(i["id"]==101):
			t3+=1
		if(i["id"]==1801) and(i["id"]==101):
			tar_goal+=1
		if(i["id"]==1801)and (i["id"]!=101):
			tar_not+=1
					
	shots=((tar_goal+(tar_not*0.5)))/(t1+t2+t3+1)
					
	try:								
		d_shots[dt['playerId']]=d_shots[dt['playerId']]+shots
	except:
		d_shots[dt['playerId']]=shots

d_goals={}
def own_goals(dt):
	own=0
	#if dt["eventId"]:
	for i in dt["tags"]:
		if(i["id"]==102):
			own+=1	
					
	try:
		d_goals[dt['playerId']]=d_goals[dt['playerId']]+own
	except:
		d_goals[dt['playerId']]=own	

	return d_goals



ssc.start()#to start computation
ssc.awaitTermination()#wait for computation to terminate

	
