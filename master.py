from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext,SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import *
import sys
import csv
import json
#reading csv files and initializing
players=open('players.csv','r')
teams=open('teams.csv','r')
fieldnames_players = ("name","birthArea","birthDate","foot","role","height","passportArea","weight","Id")
fieldnames_teams=("name","Id")
reader1=csv.reader(players,fieldnames_players)#initializing players
next(reader1)#to remove header
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
		l=[]
		m=[]
		match={}
		events={}
		for i in rdd.collect():
			dt=json.loads(i)
			if "eventId" in dt:
				events.update(dt)
				#pass_events(dt,match)
				l.append(dt)
			else:
				match.update(dt)
				m.append(dt)
				#player_profile(match)
				#print("1::",match)
				continue
		pass_events(l,m)
		#print(match)
#[[{'playerIn': 217078, 'playerOut': 14763, 'minute': 72}, {'playerIn': 285508, 'playerOut': 192748, 'minute': 82}, {'playerIn': 283142, 'playerOut': 8013, 'minute': 88}], [{'playerIn': 26010, 'playerOut': 370224, 'minute': 67}, {'playerIn': 7870, 'playerOut': 120339, 'minute': 67}, {'playerIn': 7879, 'playerOut': 7945, 'minute': 75}]]

cont={}
perform={}
player_rating={}
bench=[]
def player_cont(d1,d2,d3,d4,d5,d6,match):
	teams_data=[]
	global bench
	always=[]
	global cont
	player_in=[]
	player_out=[]
	global perform
	for row in reader1:
		pl_id=int(row[8])
		player_rating[pl_id]=0.5#initializing player rating to 0.5
		#contribution of players is set to 0 who not played match
		if pl_id not in d1:#d1=>pass accuracy
			d1[pl_id]=0 
		if pl_id not in d2:#d2 ==>duel
			d2[pl_id]=0
		if pl_id not in d3:#d3==>freekick
			d3[pl_id]=0
		if pl_id not in d4:#d4==>shots on target
			d4[pl_id]=0

		player_Cont=(d1[pl_id]+d2[pl_id]+d3[pl_id]+d4[pl_id])/4#calculating player contribution
		try:
			cont[pl_id]=cont[pl_id]+player_Cont #updtaing player contribution
		except:
			cont[pl_id]=player_Cont
		
		if pl_id in d6:#d6==>fouls
			perform[pl_id]=cont[pl_id]*(0.5/100)#performance calculation by reducing contibution by 0.5%  0.5% for every foul co#mmitted by the player 
		elif( pl_id not in d6):
			perform[pl_id]=cont[pl_id]
		if pl_id in d5:#d5==>owngoals
			perform[pl_id]=cont[pl_id]*(5/100)#Player performance is calculated by reducing the contribution by 5% for every own#goal.
		elif(pl_id not in d5):
			perform[pl_id]=cont[pl_id]
					
			
		
	for id1 in reader2:
		if id1[1] in match["teamsData"]:
		#print(match["teamsData"][id1[1]])
			up=match["teamsData"][id1[1]]["formation"]['substitutions']#taking values from substitutions for normalizing player #contribution
			teams_data.append(up)
			down=match["teamsData"][id1[1]]["formation"]['bench']#taking values from the bench
			bench.append(down)
	
	#normalizing player contibution
	for i in teams_data:
		for j in i:
			player_in.append(j["playerIn"])
			player_out.append(j["playerOut"])
			player_in_out=player_in+player_out
			if j["playerIn"] in cont:#other players who were substituted in and out are multiplied by minute/90
				cont[j["playerIn"]]=(cont[j["playerIn"]])*(j["minute"]/90)
				cont[j["playerOut"]]=(cont[j["playerOut"]])*(j["minute"]/90)

	for pl in reader1:#for each player 
		pl1=int(pl[8])
		player_rating[pl1]=(perform[pl1]+player_rating[pl1])/2
		if pl1 not in player_in_out:
			cont[pl1] = cont[pl1]*(1.05)#players who were never substituted in or out contribution multiplied by 1.05
	
		else:
			continue
	
	for pl2 in bench:
		for p in pl2:#For players who were on the bench for the entire match and had 0 fieldtime,the contribution is 0
			cont[p["playerId"]]=0
	
	print("Player Performance",perform)
	print("player_contribution",cont)

#bench2=[]
profile={}
"""
def player_profile(bench):
	for row in reader1:
		ply=int(row[8])
		for i in bench:
			for j in i:
				if j["playerId"]==ply:
					return {"error":"Invalid"}
		
		profile[ply]={}
		profile[ply][d1]=
"""

def pass_events(l,match):
	d1={}
	d2={}
	d3={}
	d4={}
	d5={}
	d6={}
	#global d1,d2,d3,d4,d5
	for i in l:
		#print(i)
	
		if(i["eventId"]==8):
			d1=pass_accuracy(i)#pass_accuracy
		if(i["eventId"]==1):
			d2=duel_effectiveness(i)#duel effectiveness
		if(i["eventId"]==3):
			d3=free_kick(i)#free kick
		if(i["eventId"]==10):
			d4=shots(i)#shots
		if(i["eventId"]):
			d5=own_goals(i)#own goals
		if(i["eventId"]):
			d6=fouls(i)
	
	print("Pass_Accuracy:",d1)
	print("duel_effectiveness:",d2)
	print("Free Kick effectiveness:",d3)
	print("shots:",d4)
	print("\n")
	#print("Owngoals:",d5)
	#print("Fouls:",d6)

	for i in match:
		player_cont(d1,d2,d3,d4,d5,d6,i) 

d_pass={}
def pass_accuracy(dt):
	#d_pass={}
	acc_norm=0
	non_acc=0
	key_p=0
	norm=0
	acc_key=0
	pass_acc=0
	tags=[]
	#d_pass={}
	#if(dt["eventId"]==8):
	for j in dt["tags"]:
		tags.append(j["id"])
	#for i in tags:
	if 1801 in tags:
		acc_norm+=1
		if(302 not in tags) and (302 not in tags):
			norm+=1
	if 302 in tags:
		key_p+=1
		if 1801 in tags:
			acc_key+=1
		
	pass_accuracy=((acc_norm)+(acc_key*2))/(1+norm+(key_p*2))
	try:	
		d_pass[dt['playerId']]=d_pass[dt['playerId']]+pass_accuracy
	except:
		d_pass[dt['playerId']]=pass_accuracy

	#print("Pass_Accuracy:",d_pass)
	return d_pass

d_duel={}
duel=0
def duel_effectiveness(dt):				
	w=0				
	n=0
	los=0
	tags=[]
	#d_duel={}
	global duel
	duel+=1
	for j in dt["tags"]:
		tags.append(j["id"])
	#if dt["eventId"]==1:
	#for i in dt["tags"]:				
	if(701 in tags):
		los+=1	 
	if(702 in tags):
		n+=1
	if(703 in tags):
		w+=1
						#to avoid zero division error
	duel_effective=(w+(n*0.5))/(w+1+n+los)				#storing values in the dictionary
	try:
		d_duel[dt['playerId']]=d_duel[dt['playerId']]+duel_effective
	except:							
		d_duel[dt['playerId']]=duel_effective
	
	#print("duel_effectiveness:",d_duel)
	return d_duel

d_free={}
def free_kick(dt):
	#global free
	eff=0
	pen=0
	non_pen1=0
	ineff=0
	non_pen2=0
	tags=[]
	#d_free={}
	#free+=1	
	#if dt["eventId"]==3:
	for j in dt["tags"]:
		tags.append(j["id"])
	
	#for i in dt["tags"]:				
	if 1802 in tags:
		if (dt["subEventId"]==35):
			ineff+=1
		if (dt["subEventId"]!=35):
			non_pen2+=1
	if 1801 in tags:
		if((dt["subEventId"]==35) and(101 in tags)):
			pen+=1
		if(dt["subEventId"]==35)and(101 not in tags):
			eff+=1
		if(dt["subEventId"]!=35):
			non_pen1+=1
					
	free_effective=((eff+pen+non_pen1)+pen)/(ineff+non_pen2+1+(eff+pen+non_pen1))
					
	try:
		d_free[dt['playerId']]=d_free[dt['playerId']]+free_effective
	except:
		d_free[dt['playerId']]=free_effective	

	#print("Free Kick effectiveness:",d_free)
	return d_free

d_shots={}
def shots(dt):
	tar_goal=0
	tar_not=0
	#global t1
	t1=0
	t2=0
	t3=0
	tags=[]
	#d_shots={}
	#if dt["eventId"]==10:
	for j in dt["tags"]:
		tags.append(j["id"])
	#for i in dt["tags"]:
	if(1801 in tags):
		t1+=1
	if(1802 in tags):
		t2+=1
	if(101 in tags):
		t3+=1
	if(1801 in tags) and(101 in tags):
		tar_goal+=1
	if(1801 in tags)and (101 not in tags):
		tar_not+=1
					
	shots=((tar_goal+(tar_not*0.5)))/(t1+t2+t3+1)
					
	try:								
		d_shots[dt['playerId']]=d_shots[dt['playerId']]+shots
	except:
		d_shots[dt['playerId']]=shots

	#print("shots:",d_shots)
	return d_shots

d_goals={}
def own_goals(dt):
	own=0
	#d_goals={}
	#if dt["eventId"]:
	for i in dt["tags"]:
		if(i["id"]==102):
			own+=1	
					
	try:
		d_goals[dt['playerId']]=d_goals[dt['playerId']]+own
	except:
		d_goals[dt['playerId']]=own	
	
	#print("Owngoals:",d_goals)
	return d_goals

#foul=0
d_foul={}
def fouls(dt):
	#d_foul={}
	foul=0
	foul+=1
	try:
		d_foul[dt["playerId"]]=d_foul[dt["playerId"]]+foul
	except:
		d_foul[dt["playerId"]]=foul
	
	#print("Fouls:",d_foul)
	return d_foul
	
ssc.start()#to start computation
ssc.awaitTermination()#wait for computation to terminate

	
