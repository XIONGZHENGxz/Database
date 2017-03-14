import os,sqlite3

import csv

def createDB(name="example.db"):
	conn=sqlite3.connect(name) # connect to database
	cur=conn.cursor() # get a cursor object
	fd=open("part1.sql")
	sqlFile=fd.read()
	sql_commands=sqlFile.split(";")
	for command in sql_commands:
		try:
			cur.execute(command)
		except:
			continue
	conn.commit()

	conn.close()

def readData():
	with open("companies.csv","w+") as companies:
		comwriter=csv.writer(companies,delimiter=',',lineterminator='\n')
		with open("organizations.csv","r") as corps:
			for i,line in enumerate(corps):
				if i==0:
					continue
				line=line.split(",")
				com_name,market,funding_total,status,country,state,city,funding_rounds,founded_at,first_funding_at,last_funding_at=line[0],line[13],line[16],line[11],line[5],line[6],line[8],line[15],line[-2],line[18],line[19]
				comwriter.writerow([com_name,market,funding_total,status,country,state,city,funding_rounds,founded_at,first_funding_at,last_funding_at])
			
def buildTables(db="example.db",table="example_table"):
	conn=sqlite3.connect(db)
	cur=conn.cursor()
	with open(table+".csv","r") as contents:
		statement="insert into "+table+" values ("
		for line in contents:
			line=line.split(",")
			new_st=statement
			for i in range(len(line)-1):
				new_st+=line[i]+","
			new_st+=line[-1]+")"
			print new_st
#			cur.execute(new_str)
			
			
if __name__=="__main__":
	createDB(name="part1.db")
	buildTables(db="part1.db",table="companies")




