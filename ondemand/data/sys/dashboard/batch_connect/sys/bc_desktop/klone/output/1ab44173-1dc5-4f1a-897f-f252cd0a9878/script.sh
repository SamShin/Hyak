#!/usr/bin/env bash

# Change working directory to user's home directory
cd "${HOME}"

# Reset module environment (may require login shell for some HPC clusters)
module purge && module restore

# Ensure that the user's configured login shell is used
export SHELL="$(getent passwd $USER | cut -d: -f7)"

# Start up desktop
echo "Launching desktop 'xfce'..."
source "/mmfs1/home/seunguk/ondemand/data/sys/dashboard/batch_connect/sys/bc_desktop/klone/output/1ab44173-1dc5-4f1a-897f-f252cd0a9878/desktops/xfce.sh"
echo "Desktop 'xfce' ended with $? status..."
