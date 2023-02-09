#!/bin/bash

# Check if the script file and number of processes arguments were provided
if [ $# -lt 2 ]; then
  echo "Usage: $0 script_file.py num_processes"
  exit 1
fi

# Get the script file name and number of processes from the arguments
script_file="$1"
num_processes="$2"

# Check if the script file exists
if [ ! -f "$script_file" ]; then
  echo "Error: $script_file not found"
  exit 1
fi

# Function to run the script and log the start and exit information
run_script() {
  echo "Process $1 started at $(date +"%Y-%m-%d %T")" >> job.log
  python "$script_file"
  ret=$?
  if [ $ret -ne 0 ]; then
    echo "Process $1 exited with code $ret at $(date +"%Y-%m-%d %T")" >> job.log
  else
    echo "Process $1 exited successfully at $(date +"%Y-%m-%d %T")" >> job.log
  fi
}

# Loop to run the script asynchronously according to the number of processes
while true; do
  for i in $(seq 1 "$num_processes"); do
    run_script "$i" &
    running_jobs="$running_jobs $!"
  done
  for job in $running_jobs; do
    wait "$job" || {
      running_jobs=''
      break
    }
  done
done
