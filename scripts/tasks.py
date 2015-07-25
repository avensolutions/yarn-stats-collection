import sys, json, time, urllib2, MySQLdb, subprocess
db = MySQLdb.connect(host="localhost",
                     user="jobstats",
                      passwd="jobstats",
                      db="jobstats")
cur = db.cursor()

jobhist_uri = str(sys.argv[1])
job_id = str(sys.argv[2])

tasks_req = "http://" + jobhist_uri + "/ws/v1/history/mapreduce/jobs/" + job_id + "/tasks"
tasks_resp = urllib2.urlopen(tasks_req)
tasks_json_obj = json.load(tasks_resp)
for i in tasks_json_obj['tasks']['task']:
	task_id = i['id']
	task_startTime = i['startTime']
	task_startTime_HR = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(task_startTime/1000))
	elapsedTime = i['elapsedTime']
	runTime = elapsedTime/1000
	progress = i['progress']
	state = i['state']
	type = i['type']
	successfulAttempt = i['successfulAttempt']
	numberOfAttempts = successfulAttempt.split('_')[5]
	# insert results into tasks table
	sql = "INSERT IGNORE INTO tasks SELECT '" + task_id + "','" + task_startTime_HR + "'," + str(runTime) + "," + str(progress) + ",'" + state + "','" + type + "'," + str(numberOfAttempts)
	cur.execute(sql)
	# get task_counters
	subprocess.Popen(["python", "task_counters.py", jobhist_uri, job_id, task_id])