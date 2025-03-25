
cd /mmfs1/home/seunguk/ondemand/data/sys/dashboard/batch_connect/sys/bc_desktop/klone/output/1ab44173-1dc5-4f1a-897f-f252cd0a9878

# Export useful connection variables
export host
export port

# Generate a connection yaml file with given parameters
create_yml () {
  echo "Generating connection YAML file..."
  (
    umask 077
    echo -e "host: $host\nport: $port\npassword: $password\ndisplay: $display\nwebsocket: $websocket\nspassword: $spassword\ninstance_name: $instance_name" > "/mmfs1/home/seunguk/ondemand/data/sys/dashboard/batch_connect/sys/bc_desktop/klone/output/1ab44173-1dc5-4f1a-897f-f252cd0a9878/connection.yml"
  )
}

# Cleanliness is next to Godliness
clean_up () {
  echo "Cleaning up..."
  [[ -e "/mmfs1/home/seunguk/ondemand/data/sys/dashboard/batch_connect/sys/bc_desktop/klone/output/1ab44173-1dc5-4f1a-897f-f252cd0a9878/clean.sh" ]] && source "/mmfs1/home/seunguk/ondemand/data/sys/dashboard/batch_connect/sys/bc_desktop/klone/output/1ab44173-1dc5-4f1a-897f-f252cd0a9878/clean.sh"
  module load 

  apptainer exec instance://ed7deb0d-1bd3-4fda-a930-c1ab7d11968f vncserver -list | awk '/^:/{system("kill -0 "$2" 2>/dev/null || apptainer exec instance://ed7deb0d-1bd3-4fda-a930-c1ab7d11968f vncserver -kill "$1)}'
  [[ -n ${display} ]] && vncserver -kill :${display}
  apptainer instance stop ed7deb0d-1bd3-4fda-a930-c1ab7d11968f

  [[ ${SCRIPT_PID} ]] && pkill -P ${SCRIPT_PID} || :
  pkill -P $$
  exit ${1:-0}
}

# Source in all the helper functions
source_helpers () {
  # Generate random integer in range [$1..$2]
  random_number () {
    shuf -i ${1}-${2} -n 1
  }
  export -f random_number

  port_used_python() {
    python -c "import socket; socket.socket().connect(('$1',$2))" >/dev/null 2>&1
  }

  port_used_python3() {
    python3 -c "import socket; socket.socket().connect(('$1',$2))" >/dev/null 2>&1
  }

  port_used_nc(){
    nc -w 2 "$1" "$2" < /dev/null > /dev/null 2>&1
  }

  port_used_lsof(){
    lsof -i :"$2" >/dev/null 2>&1
  }

  port_used_bash(){
    local bash_supported=$(strings /bin/bash 2>/dev/null | grep tcp)
    if [ "$bash_supported" == "/dev/tcp/*/*" ]; then
      (: < /dev/tcp/$1/$2) >/dev/null 2>&1
    else
      return 127
    fi
  }

  # Check if port $1 is in use
  port_used () {
    local port="${1#*:}"
    local host=$((expr "${1}" : '\(.*\):' || echo "localhost") | awk 'END{print $NF}')
    local port_strategies=(port_used_nc port_used_lsof port_used_bash port_used_python port_used_python3)

    for strategy in ${port_strategies[@]};
    do
      $strategy $host $port
      status=$?
      if [[ "$status" == "0" ]] || [[ "$status" == "1" ]]; then
        return $status
      fi
    done

    return 127
  }
  export -f port_used

  # Find available port in range [$2..$3] for host $1
  # Default host: localhost
  # Default port range: [2000..65535]
  # returns error code (0: success, 1: failed)
  # On success, the chosen port is echoed on stdout.
  find_port () {
    local host="${1:-localhost}"
    local min_port=${2:-2000}
    local max_port=${3:-65535}
    local port_range=($(shuf -i ${min_port}-${max_port}))
    local retries=1 # number of retries over the port range if first attempt fails
    for ((attempt=0; attempt<=$retries; attempt++)); do
      for port in "${port_range[@]}"; do
        if port_used "${host}:${port}"; then
continue
        fi
        echo "${port}"
        return 0 # success
      done
    done

    echo "error: failed to find available port in range ${min_port}..${max_port}" >&2
    return 1 # failure
  }
  export -f find_port

  # Wait $2 seconds until port $1 is in use
  # Default: wait 30 seconds
  wait_until_port_used () {
    local port="${1}"
    local time="${2:-30}"
    for ((i=1; i<=time*2; i++)); do
      port_used "${port}"
      port_status=$?
      if [ "$port_status" == "0" ]; then
        return 0
      elif [ "$port_status" == "127" ]; then
         echo "commands to find port were either not found or inaccessible."
         echo "command options are lsof, nc, bash's /dev/tcp, or python (or python3) with socket lib."
         return 127
      fi
      sleep 0.5
    done
    return 1
  }
  export -f wait_until_port_used

  # Generate random alphanumeric password with $1 (default: 8) characters
  create_passwd () {
    tr -cd 'a-zA-Z0-9' < /dev/urandom 2> /dev/null | head -c${1:-8}
  }
  export -f create_passwd
}
export -f source_helpers

source_helpers

# Set host of current machine
host=$(hostname)


# Load 
echo "Loading ..."
module load 
export APPTAINER_BINDPATH=""
export INSTANCE_NAME="ed7deb0d-1bd3-4fda-a930-c1ab7d11968f"
export instance_name="ed7deb0d-1bd3-4fda-a930-c1ab7d11968f"
echo "Starting instance..."
apptainer instance start  /mmfs1/sw/ondemand/containers/vnc/latest ed7deb0d-1bd3-4fda-a930-c1ab7d11968f

# Setup one-time use passwords and initialize the VNC password
function change_passwd () {
  echo "Setting VNC password..."
  password=$(create_passwd "8")
  spassword=${spassword:-$(create_passwd "8")}
  (
    umask 077
    echo -ne "${password}\n${spassword}" | apptainer exec instance://ed7deb0d-1bd3-4fda-a930-c1ab7d11968f vncpasswd -f > "vnc.passwd"
  )
}
change_passwd


# Start up vnc server (if at first you don't succeed, try, try again)
echo "Starting VNC server..."
for i in $(seq 1 10); do
  # Clean up any old VNC sessions that weren't cleaned before
  apptainer exec instance://ed7deb0d-1bd3-4fda-a930-c1ab7d11968f vncserver -list | awk '/^:/{system("kill -0 "$2" 2>/dev/null || apptainer exec instance://ed7deb0d-1bd3-4fda-a930-c1ab7d11968f vncserver -kill "$1)}'

  # for turbovnc 3.0 compatability.
  if timeout 2 apptainer exec instance://ed7deb0d-1bd3-4fda-a930-c1ab7d11968f vncserver --help 2>&1 | grep 'nohttpd' >/dev/null 2>&1; then
    HTTPD_OPT='-nohttpd'
  fi

  # Attempt to start VNC server
  VNC_OUT=$(apptainer exec instance://ed7deb0d-1bd3-4fda-a930-c1ab7d11968f vncserver -log "vnc.log" -rfbauth "vnc.passwd" $HTTPD_OPT -noxstartup  2>&1)
  VNC_PID=$(pgrep -s 0 Xvnc) # the script above will daemonize the Xvnc process
  echo "${VNC_PID}"
  echo "${VNC_OUT}"

  # Sometimes Xvnc hangs if it fails to find working disaply, we
  # should kill it and try again
  kill -0 ${VNC_PID} 2>/dev/null && [[ "${VNC_OUT}" =~ "Fatal server error" ]] && kill -TERM ${VNC_PID}

  # Check that Xvnc process is running, if not assume it died and
  # wait some random period of time before restarting
  kill -0 ${VNC_PID} 2>/dev/null || sleep 0.$(random_number 1 9)s

  # If running, then all is well and break out of loop
  kill -0 ${VNC_PID} 2>/dev/null && break
done

# If we fail to start it after so many tries, then just give up
kill -0 ${VNC_PID} 2>/dev/null || clean_up 1

# Parse output for ports used
display=$(echo "${VNC_OUT}" | awk -F':' '/^Desktop/{print $NF}')
port=$((5900+display))

echo "Successfully started VNC server on ${host}:${port}..."

# Export the module function if it exists
[[ $(type -t module) == "function" ]] && export -f module

# Create Named Pipe on node for launching Xfce4 Terminal
export node_fifo="$(pwd -P)/node.fifo"
mkfifo ${node_fifo}
while :; do eval "$(<${node_fifo}) &"; done &
export APPTAINERENV_NODE_FIFO=${node_fifo}

# Run Xfce4 Terminal as login shell with UTF-8 encoding (sets proper TERM/LANG)
TERM_CONFIG="${HOME}/.config/xfce4/terminal/terminalrc"
if [[ ! -e "${TERM_CONFIG}" ]]; then
  mkdir -p "$(dirname "${TERM_CONFIG}")"
  echo "[Configuration]" >> ${TERM_CONFIG}
  echo "CommandLoginShell=TRUE" >> ${TERM_CONFIG}
  echo "Encoding=UTF-8" >> ${TERM_CONFIG}
else
  sed -i \
    '/^CommandLoginShell=/{h;s/=.*/=TRUE/};${x;/^$/{s//CommandLoginShell=TRUE/;H};x}' \
    "${TERM_CONFIG}"
  sed -i \
    '/^Encoding=/{h;s/=.*/=UTF-8/};${x;/^$/{s//Encoding=UTF-8/;H};x}' \
    "${TERM_CONFIG}"
fi

# Add Bookmarks for file-browser
THUNAR_BOOKMARKS="${HOME}/.config/gtk-3.0/bookmarks"
if [[ ! -e "${THUNAR_BOOKMARKS}" ]]; then
  find_flags=(/mmfs1/gscratch -maxdepth 1)
  for groupname in $(id --name --groups $USER); do
    find_flags+=(-group $groupname -name $groupname)
    find_flags+=(-o)
  done
  unset find_flags[-1]
  mkdir -p "$(dirname "${THUNAR_BOOKMARKS}")"
  for gscratch_dir in $(find "${find_flags[@]}"); do
    echo "file://${gscratch_dir} GScratch (${gscratch_dir##*/})" >> ${THUNAR_BOOKMARKS}
  done
  USER_SCRUBBED_DIR="/mmfs1/gscratch/scrubbed/${USER}"
  if [[ -d $USER_SCRUBBED_DIR ]]; then
    echo "file://${USER_SCRUBBED_DIR} Scrubbed (${USER})" >> ${THUNAR_BOOKMARKS}
  fi
fi



echo "Script starting..."
DISPLAY=:${display} apptainer exec instance://${instance_name} ./script.sh &
SCRIPT_PID=$!

[[ -e "/mmfs1/home/seunguk/ondemand/data/sys/dashboard/batch_connect/sys/bc_desktop/klone/output/1ab44173-1dc5-4f1a-897f-f252cd0a9878/after.sh" ]] && source "/mmfs1/home/seunguk/ondemand/data/sys/dashboard/batch_connect/sys/bc_desktop/klone/output/1ab44173-1dc5-4f1a-897f-f252cd0a9878/after.sh"

# Launch websockify websocket server
module load 
echo "Starting websocket server..."
websocket=$(find_port)
[ $? -eq 0 ] || clean_up 1 # give up if port not found
apptainer exec instance://ed7deb0d-1bd3-4fda-a930-c1ab7d11968f /usr/bin/websockify -D ${websocket} localhost:${port}

# Set up background process that scans the log file for successful
# connections by users, and change the password after every
# connection
echo "Scanning VNC log file for user authentications..."
while read -r line; do
  if [[ ${line} =~ "Full-control authentication enabled for" ]]; then
    change_passwd
    create_yml
  fi
done < <(tail -f --pid=${SCRIPT_PID} "vnc.log") &


# Create the connection yaml file
create_yml

# Wait for script process to finish
wait ${SCRIPT_PID} || clean_up 1

# Exit cleanly
clean_up


