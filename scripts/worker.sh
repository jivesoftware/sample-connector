#!/bin/bash

function usage() {
    echo "usage: $0 [id]"
    echo "    [id]   The workerID to start, a number between 1 and 5"
    exit 1
}

if [ ${#@} != 1 ]
then
    usage;
fi

case "$1" in
1)  export __WORKER_ID=1
    ;;
2)  export __WORKER_ID=2
    ;;
3)  export __WORKER_ID=3
    ;;
4)  export __WORKER_ID=4
    ;;
5)  export __WORKER_ID=5
    ;;
*)  usage
    ;;
esac

export __ROLE=worker

echo "Starting worker #$__WORKER_ID"
sleep 1
node app.js