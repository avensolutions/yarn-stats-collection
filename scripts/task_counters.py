import os, time, sys, json, urllib2, MySQLdb, warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

try:
#	db = MySQLdb.connect(host="localhost",
#						 user="jobstats",
#						  passwd="jobstats",
#						  db="jobstats")
#	cur = db.cursor()

	jobhist_uri = str(sys.argv[1])
	job_id = str(sys.argv[2])
	task_id = str(sys.argv[3])

	task_req = "http://" + jobhist_uri + "/ws/v1/history/mapreduce/jobs/" + job_id + "/tasks/" + task_id + '/counters'
	task_resp = urllib2.urlopen(task_req)
	task_json_obj = json.load(task_resp)

	# create counter file
	task_counters_filename = "tmp_task_counters_" + str(int(time.time()))
	task_counters_file = open(task_counters_filename,'w', 1)
	job_task_str = job_id + "," + task_id
	for i in task_json_obj['jobTaskCounters']['taskCounterGroup']:
		counterGroupName = i['counterGroupName']
		for ii in i['counter']:
			counter = ii['name']
			value = ii['value']
			# update counter file
			#job_id,task_id,counterGroupName,counter,value
			task_counters_file.write(job_task_str + "," + counterGroupName + "," + counter + "," + str(value) + "\n")
	# bulk insert counter file into task_counters table
	task_counters_file.close()
#	sql = "LOAD DATA LOCAL INFILE '" + task_counters_filename + "' INTO TABLE jobstats.task_counters FIELDS TERMINATED BY ','"
#	cur.execute(sql)
#	cur.close()
#	db.close()
#	os.remove(task_counters_filename)
	# to avoid 'Too many connections' error
#	time.sleep(0.01)
except Exception as e:
	print e.message