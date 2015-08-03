import sys, json, time, urllib2, pika, subprocess
#, warnings
#import MySQLdb
#warnings.filterwarnings("ignore", category=DeprecationWarning)
#try:
	#db = MySQLdb.connect(host="localhost",
	#					 user="jobstats",
	#					  passwd="jobstats",
	#					  db="jobstats")
	#cur = db.cursor()

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', retry_delay=1, socket_timeout=100))
channel = connection.channel()
channel.queue_declare(queue='yarn-stats')

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
	numberOfAttemptsList = successfulAttempt.split('_')
	if len(numberOfAttemptsList) > 4:
		numberOfAttempts = numberOfAttemptsList[5]
	else:
		numberOfAttempts = -1
	# insert results into tasks table
	sql = "INSERT IGNORE INTO tasks SELECT '" + task_id + "','" + task_startTime_HR + "'," + str(runTime) + "," + str(progress) + ",'" + state + "','" + type + "'," + str(numberOfAttempts)
	channel.basic_publish(exchange='',
		routing_key='yarn-stats',
		body=sql)
	#cur.execute(sql)
	# get task_counters
	subprocess.Popen(["python", "scripts/task_counters.py", jobhist_uri, job_id, task_id])
#cur.close()
#db.close()
# to avoid 'Too many connections' error
connection.close()	
time.sleep(0.01)

#except Exception as e:
#	print e.message