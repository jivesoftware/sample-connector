clear

echo "Lock table:"
psql -U postgres -h localhost "sample-connector" -c "select * from owners order by ownerid";

echo "Total number of workqueue/processed items:"
psql -U postgres -h localhost "sample-connector" -c "select (select count(*) from workqueue) as workqueue, (select count(*) from activitylog) as processed";

#echo "Total number of processed items:"
#psql -U postgres -h localhost "sample-connector" -c "select count(*) from activitylog";

#echo "Worker/owner breakdown:"
#psql -U postgres -h localhost "sample-connector" -c "select workerid,ownerid,count(modtime) from activitylog group by workerid,ownerid order by workerid,ownerid";

echo "Total number of processed items by worker:"
psql -U postgres -h localhost "sample-connector" -c "select workerid,count(*) from activitylog group by workerid order by workerid";

echo "Number of work items handled concurrently by the same worker (we want this to be 0):"
psql -U postgres -h localhost "sample-connector" -c "select count(*) from activitylog group by workerid,ownerid,modtime having count(*) > 1";

sleep 1s
./watch.sh
