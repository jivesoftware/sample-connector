#!/bin/bash
while true
do
    clear

    psql -U postgres -h localhost "sample-connector" -c "select * from workowners order by workownerid";

    echo "Total number of workitems/processed items:"
    psql -U postgres -h localhost "sample-connector" -c "select (select count(*) from workitems) as workitems, (select count(*) from worklog) as processed, (select count(*) from workitems) - (select count(*) from worklog) as backlog";

    #echo "Total number of processed items:"
    #psql -U postgres -h localhost "sample-connector" -c "select count(*) from worklog";

    #echo "Worker/workowner breakdown:"
    #psql -U postgres -h localhost "sample-connector" -c "select workerid, workownerid,count(modtime) from worklog group by workerid,workownerid order by workerid,workownerid";

    echo "Total number of processed items by worker:"
    psql -U postgres -h localhost "sample-connector" -c "select workerid, count(*) from worklog group by workerid order by workerid";

    echo "Number of work items handled concurrently by the same worker (we want this to be 0):"
    psql -U postgres -h localhost "sample-connector" -c "select count(*) from (select modtime, count(workerid) from worklog group by workerid, workownerid, modtime having count(workerid) > 1) as duplicate;";

    sleep 1s
done
