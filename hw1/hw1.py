#!/bin/usr/env python 
import os
import subprocess
import apachetime
import time
import tempfile
import csv
DATA_DIR=os.environ['HOME']+'/learning/database/hw1/'

def readData():
	with open(DATA_DIR+'web_server.log') as f:
		log_line=f.readline()
		print log_line


def apache_ts_to_unixtime(ts):
	dt=apachetime.apachetime(ts)
	unixtime=time.mktime(dt.timetuple())
	return int(unixtime)

def sortFile(filePath):
	tmp_sessions,path=tempfile.mkstemp()
	output = subprocess.Popen(["tail", "-n","+2",filePath], stdout=subprocess.PIPE).communicate()[0]
	os.write(tmp_sessions,output)
	output=subprocess.Popen(["sort",path],stdout=subprocess.PIPE).communicate()[0]
	return tmp_sessions
	
def process_logs(dataset_iter):

	#create hist.csv
	with open('hits.csv','w+') as hits_file:
		hitswriter=csv.writer(hits_file,delimiter=',',lineterminator='\n')
		hitswriter.writerow(["ip","timestamp"])
		for i,line in enumerate(dataset_iter):
			split_line=line.split("\t")
			ip=split_line[0]
			timestamp=split_line[2]
			hitswriter.writerow([ip,timestamp])
	hits_file.close()

	#create session.csv
	tmp_sessions=sortFile("hits.csv")
	with open('session.csv','w+') as session_file:
		sessionwriter = csv.writer(session_file,delimiter=',',lineterminator='\n')
		sessionwriter.write(["ip","session_length","num_hits"])
		#start calculating
		flag=False # no data yet
		wrote=False
		pre_ip,pre_time,curr_len,curr_hits=0,-1,0,1
		for line in tmp_sessions:
			split_line=line.split(",")
			curr_ip=split_line[0]
			curr_time=int(split_line[1])
			wrote=False
			#same session
			if curr_ip==pre_ip and pre_time>=0 and abs(curr_time-pre_time)<=1800:
				curr_len=curr_len+abs(curr_time-pre_time)
				curr_hits=curr_hits+1
			#different session
			else: 
				if flag:
					sessionwriter.writerow([pre_ip,curr_len,curr_hits])
					wrote=True
				curr_len=0
				curr_hits=1
			pre_ip=curr_ip
			pre_time=curr_time
			flag=True
		
		# check the last one
		if not wrote:
			sessionwriter.writerow([pre_ip,curr_len,curr_hits])
	session_file.close()

	#session_length_plot.csv
	session_len_plot,path=tempfile.mkstemp()
	output = subprocess.Popen(["tail", "-n","+2",path], stdout=subprocess.PIPE).communicate()[0]
	os.write(tmp_sessions,output)
	output=subprocess.Popen(["sort","-s","-n","-t",",","-k2",path],stdout=subprocess.PIPE).communicate()[0]
	with open("session_len_plot.csv","w+") as session_len_file:
		lenwriter=csv.writer(session_len_file,delimiter=',',lineterminator='\n')
		lenwriter.writerow(["left","right","count"])
		curr_left,curr_right,curr_count=0,2,0
		legit=False
		for line in session_len_plot:
			curr_len=line.split(",")[1]
			if curr_len>=curr_left and curr_len<curr_right:
				curr_count+=curr_count
			else:
				#write old one
				if legit and curr_count!=0:
					lenwriter.writerow([curr_left,curr_right,curr_count])
				#generate proper bin
				while not (curr_len>=curr_left and curr_len<curr_right):
					curr_left=curr_right
					curr_right*=2
				curr_count=1
			legit=True
			
		lenwriter.writerow([curr_left,curr_right,curr_count])
	session_len_file.close()


if __name__=="__main__":
	readData()
	fd,path=tempfile.mkstemp()
	output=subprocess.Popen(["tail","-n","+2","new.txt"], stdout=subprocess.PIPE).communicate()[0]
	os.write(fd,output)
	output=subprocess.Popen(["sort","-s", "-n","-t",",","-k2",path],stdout=subprocess.PIPE).communicate()[0]
	print output
	
	
