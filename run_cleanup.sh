#!/bin/bash

# Get the current time in seconds since the epoch
current_time=$(date +%s)

# Loop through all files in the /downloads directory
for file in ./downloads/*; do
  # Skip the .gitkeep file
  if [ "$(basename "$file")" = ".gitkeep" ]; then
    continue
  fi

  # Get the creation time of the file in seconds since the epoch
  file_time=$(stat -c %Y "$file")

  # Calculate the difference in minutes between the current time and file time
  time_diff=$(( (current_time - file_time) / 60 ))

  # If the difference is greater than 15 minutes, delete the file
  if [ $time_diff -gt 15 ]; then
    rm "$file"
  fi
done
