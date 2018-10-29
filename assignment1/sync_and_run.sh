#!/bin/bash
from_dir="/mnt/d/os_project/assignment1/" # the last slash should not be forgot
remote="liz0f@10.73.106.4"
to_dir_basename="~/os_project/assignment1/"
run_command="
export GOPATH=\"/home/liz0f/os_project/assignment1/\";
cd \"\${GOPATH}src/main\";
go run wc.go master sequential pg-*.txt;
"
to_dir="${remote}:${to_dir_basename}"
sync(){
    sync_command="rsync -avrzhe ssh --progress --delete ${1} ${2}"
    dry_run_output="$($sync_command --dry-run)" || { echo "rsync exited with status $?" 1>&2; return 1; }
    grep_output="$(echo "$dry_run_output" | grep '^deleting')"
    if ! [ -z "$grep_output" ]; then
        echo "$dry_run_output"
        read -p "confirm? [y]" choice
        if [ "$choice" = "y" ]; then
            $sync_command || { echo "rsync exited with status $?" 1>&2; return 1; }
        else
            echo "user cancelled syncing" 1>&2 ; return 1;
        fi
    else
        $sync_command || { echo "rsync exited with status $?" 1>&2; return 1; }
    fi
}
<<<<<<< HEAD
sync "$from_dir" "$to_dir" && ! [ -z "$run_command" ] &&
{ printf "\n################### execute run command ####################\n";
  ssh -t "$remote" "cd $to_dir_basename; $run_command"  || echo "ssh exited with status $?" 1>&2; 
=======
sync "$from_dir" "$to_dir" &&
{ printf "\n################### execute run command ####################\n";
  ssh -t "$remote" "cd $to_dir_basename; $run_command"  || echo "ssh exited with status $?" 1>&2;
>>>>>>> 2cfe4f238e51919ce2f9ae4b832e4741e327d9ca
  exit 1; }
