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
#connect to a given db return a cursor object
def connectDB(db="test.db"):
	conn=sqlite3.connect(db)
	cursor=conn.cursor()
	return cursor

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



# Q1
# Question: for all companies whose status column contains 'acquired', show
# the associated acquisition price_amount (if there is no acquisition record, price_amount
# should be returned as NULL)
# Output: company_name, price_amount

def q1():
	cur=connectDB(db="part1.db")
	cur.execute("select C.company_name,A.price_amount from companies as C LEFT OUTER JOIN  acquisitions as A on C.company_name=A.company_name where C.status='acquired'")
	print cur.fetchall()

# Q2
# Question: How many startups, according this data, were founded (founded_at column) between 
# 2012 and 2014, inclusive (on or after 2012-01-01, and before or on 2014-12-31)?
# Output: a number (column name doesn't matter)
# Hint: time comparison will look like <= '1990-01-12'

def q2():
	cur=connectDB(db="part1.db") 
	cur.execute("SELECT COUNT(*) FROM companies AS C WHERE C.founded_at>='2012-01-01' and C.founded_at<='2014-12-31'")
	print cur.fetchall()

# Q3
# Question: What is/are the state(s) that has/have the largest number(s) of startups  in the  
#    "Security" market (i.e. market column contains the word "Security")? (Plural due to ties)
# Output: the state(s) and the corresponding number(s), column named "state" and "total" respectively.
#         Note that the test script relies on the naming of output columns.

# Notes:
# - If the output is tied, then display all (i.e. if both CA and NY have, say 1000 startups, 
#   then display both states)
# - You might want to look into the keyword "LIKE"
# - The state must be valid (i.e. not "")

def q3():
	cur=connectDB(db="part1.db")
	cur.execute('''DROP VIEW IF EXISTS grouped_table;
				CREATE VIEW grouped_table(state,num_startups) AS 
				SELECT state,COUNT(*) FROM companies as C WHERE market like '%Security%' AND state like '_%' GROUP BY state;
				SELECT state,num_startups as total FROM grouped_tables WHERE num_startups=(SELECT MAX(num_startups) 
				FROM grouped_table);''') 
	print cur.fetchall()

# Q4
# Question: which cities have a larger number of acquirers than startups?

# Note 
# - a startup could be counted as an acquirer, 
#   but it should only be counted at most once in each category
#   and the data is not very clean (i.e. duplicates in company_name entry)

def q4():
	cur.connectDB(db="part1.db")
	cur.execute('''DROP VIEW IF EXISTS acquires;
				DROP VIEW IF EXISTS startups;
				CREATE VIEW acquires(city,num) AS:
				SELECT acquirer_city,COUNT(DISTINCT acquirer_name)
				FROM acquisitions GROUPED BY acquirer_city;
				CREATE VIEW startups(city,num) AS:
				SELECT city, COUNT(DISTINCT company_name) 
				FROM companies GROUPED BY city;
				SELECT city as ac_city FROM acquires
				EXCEPT 
				SELECT acquires.city as ac_city from acquires,starups WHERE acquires.city = startups.city 
				AND acquires.num<=startups.num;''')
	print cur.fetchall()

if __name__=="__main__":
	createDB(name="part1.db")
	buildTables(db="part1.db",table="companies")
	q1(),q2(),q3(),q4()



