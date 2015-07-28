import sys, json, time, urllib2, MySQLdb
warnings.filterwarnings("ignore", category=DeprecationWarning)
try:
	db = MySQLdb.connect(host="localhost",
						 user="jobstats",
						  passwd="jobstats",
						  db="jobstats")
	cur = db.cursor()

	jobhist_uri = str(sys.argv[1])
	job_id = str(sys.argv[2])

	job_info_req = "http://" + jobhist_uri + "/ws/v1/history/mapreduce/jobs/" + job_id
	job_info_resp = urllib2.urlopen(job_info_req)
	job_info_json_obj = json.load(job_info_resp)
	avgMapTime = job_info_json_obj['job']['avgMapTime']
	avgReduceTime = job_info_json_obj['job']['avgReduceTime']
	avgShuffleTime = job_info_json_obj['job']['avgShuffleTime']
	avgMergeTime = job_info_json_obj['job']['avgMergeTime']
	failedReduceAttempts = job_info_json_obj['job']['failedReduceAttempts']
	killedReduceAttempts = job_info_json_obj['job']['killedReduceAttempts']
	successfulReduceAttempts = job_info_json_obj['job']['successfulReduceAttempts']
	failedMapAttempts = job_info_json_obj['job']['failedMapAttempts']
	killedMapAttempts = job_info_json_obj['job']['killedMapAttempts']
	successfulMapAttempts = job_info_json_obj['job']['successfulMapAttempts']
	# insert results into job_info table
	sql = "INSERT IGNORE INTO job_info SELECT '" + job_id + "'," + str(avgMapTime) + "," + str(avgReduceTime) + "," + str(avgShuffleTime) + "," + str(avgMergeTime) + "," + str(failedReduceAttempts) + "," + str(killedReduceAttempts) + "," + str(successfulReduceAttempts) + "," + str(failedMapAttempts) + "," + str(killedMapAttempts) + "," + str(successfulMapAttempts)
	cur.execute(sql)
	cur.close()
	db.close()
	# to avoid 'Too many connections' error
	time.sleep(0.01)
except Exception as e:
	print e.message