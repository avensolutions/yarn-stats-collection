jobhist_uri=$1
datadog_api_key=$2
show_progress=true
python scripts/jobs.py $jobhist_uri $show_progress
python scripts/evaluate_metrics.py $show_progress
python scripts/post_metrics.py $datadog_api_key