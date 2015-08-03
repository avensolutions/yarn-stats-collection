import sys, os, json, time, urllib2, pika, subprocess
#import MySQLdb
#db = MySQLdb.connect(host="localhost",
#                     user="jobstats",
#                      passwd="jobstats",
#                      db="jobstats")
#cur = db.cursor()
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='yarn-stats')
jobhist_uri = str(sys.argv[1])
jobs_req = "http://" + jobhist_uri + "/ws/v1/history/mapreduce/jobs"
jobs_resp = urllib2.urlopen(jobs_req)
jobs_json_obj = json.load(jobs_resp)
no_jobs = len(jobs_json_obj['jobs']['job'])
counter = 1
for i in jobs_json_obj['jobs']['job']:
	job_state = i['state']
	valid_states = ["SUCCEEDED", "FAILED", "KILL_WAIT", "KILLED"]
	if job_state in valid_states:
		job_submitTime = i['submitTime']
		job_sumbitTime_HR = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(job_submitTime/1000))
		job_startTime = i['startTime']
		job_finishTime = i['finishTime']
		job_id = i['id']
		job_name = i['name'].replace("'", "")
		queue = i['queue']
		user = i['user']
		mapsTotal = i['mapsTotal']
		mapsCompleted = i['mapsCompleted']
		reducesTotal = i['reducesTotal']
		reducesCompleted = i['reducesCompleted']
		queueTime = (job_startTime - job_submitTime)/1000
		runTime = (job_finishTime - job_startTime)/1000
		# insert results into jobs table	
		sql = "INSERT IGNORE INTO jobs SELECT '" + job_id + "','" + job_sumbitTime_HR + "','" + job_name + "','" + queue + "','" + user + "','" + job_state + "'," + str(queueTime) + "," + str(runTime) + "," + str(mapsTotal) + "," + str(mapsCompleted) + "," + str(reducesTotal) + "," + str(reducesCompleted)
		channel.basic_publish(exchange='',
                      routing_key='yarn-stats',
                      body=sql)
		#cur.execute(sql)
		# get job_info
		subprocess.Popen(["python", "scripts/job_info.py", jobhist_uri, job_id])
		# get tasks
		subprocess.Popen(["python", "scripts/tasks.py", jobhist_uri, job_id])
		# to avoid 'Too many connections' error
		time.sleep(0.01)
		sys.stdout.write("\r" + str(counter) + "/" + str(no_jobs))
		sys.stdout.flush()
		counter = counter + 1
#cur.close()
#db.close()
connection.close()