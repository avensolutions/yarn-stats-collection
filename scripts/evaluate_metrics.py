#	
# Evaluate Hourly Job Metrics	
#

import sys, json, MySQLdb

show_progress = str(sys.argv[1])

db = MySQLdb.connect(host="localhost",
	user="jobstats",
	passwd="jobstats",
	db="jobstats")
cur = db.cursor()

data_file = open('metrics.json')    
metricsobj = json.load(data_file)

for i in metricsobj["metrics"]:
	metric_name = i["metric_name"]
	if show_progress == 'true':
		print("Evaluating: " + metric_name)
	metric_sql = i["metric_sql"]
	cur.execute(metric_sql)

cur.close()
db.close()