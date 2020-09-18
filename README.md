# Database Replication Into BigQuery Using Change Data Capture Demo

## Overview
This project is a demo of how Change Data Capture can be used to replicate data to Google's BigQuery. 

A simple data generator simulates a typical web site session life cycle. A session has several attributes - 
id, start time, end time, customer id, and status (new, logged in, logged out, abandoned). 
All attributes except the start time are mutable and each session can go through several status transitions within several minutes. 

Demo code uses BigTable as the data source and uses BigQuery’s streaming inserts to populate the destination table. 
The demo mimics SAP Data Services replication to BigQuery and uses column naming conventions 
specific to that process.

The source code provides several scripts that help monitor the data replication process and data availability in various tables.

## Setup
**Note:** the demo uses BigTable and BigQuery and you may incur charges. To avoid or minimize the charges make sure to shut down the data generation script and clean up the environment at the end.

Decide which GCP project you would like to run this demo in (the snippet below assumes your current project).
You need to have sufficient privileges in that project to create BigQuery dataset and tables and a BigTable cluster and a table.
```
export TF_VAR_project_id=$(gcloud config get-value project)
```

Set up a variable to be used in several shell commands:
```.env
export DEMO_HOME=<root directory of the checked-out repository>
```

To create the environment for the demo:
```
find $DEMO_HOME -name '*.sh' -exec chmod +x {} \;
cd $DEMO_HOME/setup
./create-all.sh
```
At the end of the successful setup you will have created:
- BigTable instance `bq-sync-instance` with `bq-sync-instance-cluster` cluster
    - Table `session` in that cluster. This table is the source data for the demo.
- BigQuery dataset `cdc_demo`
    - Table `session_main`. Snapshot of the source of the data at a point in time.
    - Table `session_delta`. CDC events of the source data.
    - View `session_source_v`. View into the BigTable's `session` table (added as a federated data source).
    - View `session_latest_v`. View that selects the latest session record from `session_main` and `session_delta` tables.


## Generating data
In a separate Cloud Shell window or in a separate terminal window:
1. Build the executable:
```
cd $DEMO_HOME/data-generator/
mvn package
```

1. Run:
```.env
./start.sh --project_id $(gcloud config get-value project)
```

You should see output similar to this one:
```
Oct 23, 2019 1:25:17 PM com.google.demo.BigQueryService runInsertAll
INFO: Inserted next batch of 1096 rows.
```

The start script accepts several different parameters which can be used to tune the rate of data generation. The only required parameter is project_id.
```
    --delete_percent <number>           Percentage of deletes relative to
                                        inserts
    --inserts_per_batch <number>        Number of inserts per batch
    --original_session_size <number>    Original number of records in the
                                        session table
    --pause_between_batches <seconds>   Pause between batches
    --project_id <GCP project ID>       Project
    --update_percent <number>           Percentage of updates relative to
                                        inserts
```

Let this process run in this terminal window until you are ready to stop it at the end of the demo.

### Stopping data generation
The start.sh script runs the data generation process in the background. To stop it, run:
	./stop.sh

## Immediate data consistency
As part of the Terraform setup you created a view called “session_latest_v”, and a script that checks for the differences between the source database and the data in this view. 
Let’s see how it performs. 

Set up your session project id if it is not yet set:
```.env
gcloud config set project [PROJECT_ID]
```

Switch to the directory with various BigQuery-related scripts:
```
cd $DEMO_HOME/setup/bigquery/
```

Run this script to monitor the discrepancies between the data source and the view (it executes a query periodically):
```
./show-session-latest-discrepancies.sh
```

You will see output similar to this:
```
+----------------------------+-------+---------------------+
|        description         | count |     measured_ts     |
+----------------------------+-------+---------------------+
| Records not in destination |     0 | 2019-11-17 23:27:59 |
| Records not in source      |     0 | 2019-11-17 23:27:59 |
| Records with data mismatch |     0 | 2019-11-17 23:27:59 |
| Total record in the source | 19975 | 2019-11-17 23:27:59 |
+----------------------------+-------+---------------------+
Waiting on bqjob_r2c0423ff625f63af_0000016e7bb21771_1 ... (2s) Current status: DONE
+----------------------------+-------+---------------------+
|        description         | count |     measured_ts     |
+----------------------------+-------+---------------------+
| Records not in destination |     0 | 2019-11-17 23:28:20 |
| Records not in source      |     0 | 2019-11-17 23:28:20 |
| Records with data mismatch |     0 | 2019-11-17 23:28:20 |
| Total record in the source | 20450 | 2019-11-17 23:28:20 |
+----------------------------+-------+---------------------+
Waiting on bqjob_r5327a5c14a431ef8_0000016e7bb268d5_1 ... (3s) Current status: DONE
+----------------------------+-------+---------------------+
|        description         | count |     measured_ts     |
+----------------------------+-------+---------------------+
| Records not in destination |   500 | 2019-11-17 23:28:41 |
| Records not in source      |    25 | 2019-11-17 23:28:41 |
| Records with data mismatch |   555 | 2019-11-17 23:28:41 |
| Total record in the source | 20925 | 2019-11-17 23:28:41 |
+----------------------------+-------+---------------------+
```

The data is most of the time in sync with the source, but there are cases where the data in the source hasn’t been replicated yet to BigQuery, or records in BigQuery’s streaming buffer are not yet available for queries. As soon as you stop data generation the discrepancies disappear. 

Stop the script by using Ctrl+C.

## Merging delta table to main table
###Monitoring the progress of merging
First, let’s set up a process to monitor the status of merging. Switch to the directory with BigQuery scripts:
```
cd $DEMO_HOME/setup/bigquery/
```

Run the script that queries the main table and shows the discrepancies:
```
./show-merge-status.sh
```

You will see output similar to this:
```
+----------------------------+-------+---------------------+
|        description         | count |     measured_ts     |
+----------------------------+-------+---------------------+
| Records not in destination | 30900 | 2019-11-17 23:35:46 |
| Records not in source      |     0 | 2019-11-17 23:35:46 |
| Records with data mismatch |     0 | 2019-11-17 23:35:46 |
| Total record in the source | 30900 | 2019-11-17 23:35:46 |
+----------------------------+-------+---------------------+
```

That means that there are no merged records in the main table yet. Keep this process running.

### Run merge process on a periodic basis
Demo code contains a simple script which runs the merge DML statement every two minutes. Open another Cloud Shell tab or Terminal window and run:
```
cd $DEMO_HOME/setup/bigquery
./merge-periodically.sh
```

You should see output similar to this one:
```
Waiting on bqjob_r1b66ecea6ea65e3_0000016e7bbbe519_1 ... (5s) Current status: DONE   
Number of affected rows: 35650

Waiting on bqjob_r305f488fd3ee0f25_0000016e7bbdde2e_1 ... (3s) Current status: DONE   
Number of affected rows: 6343
```

If you switch to window where you monitor the progress of merging you will be able to see that the merge is in progress:
```
+----------------------------+-------+---------------------+
|        description         | count |     measured_ts     |
+----------------------------+-------+---------------------+
| Records not in destination |  2482 | 2019-11-17 23:40:56 |
| Records not in source      |   107 | 2019-11-17 23:40:56 |
| Records with data mismatch |  2715 | 2019-11-17 23:40:56 |
| Total record in the source | 38025 | 2019-11-17 23:40:56 |
+----------------------------+-------+---------------------+
Waiting on bqjob_r6e6fc8c6242c904b_0000016e7bbdf5f1_1 ... (2s) Current status: DONE   
+----------------------------+-------+---------------------+
|        description         | count |     measured_ts     |
+----------------------------+-------+---------------------+
| Records not in destination |     0 | 2019-11-17 23:41:18 |
| Records not in source      |     0 | 2019-11-17 23:41:18 |
| Records with data mismatch |     0 | 2019-11-17 23:41:18 |
| Total record in the source | 38500 | 2019-11-17 23:41:18 |
+----------------------------+-------+---------------------+
Waiting on bqjob_r41629d8746e64399_0000016e7bbe4bfc_1 ... (2s) Current status: DONE   
+----------------------------+-------+---------------------+
|        description         | count |     measured_ts     |
+----------------------------+-------+---------------------+
| Records not in destination |   802 | 2019-11-17 23:41:40 |
| Records not in source      |    31 | 2019-11-17 23:41:40 |
| Records with data mismatch |  1117 | 2019-11-17 23:41:40 |
| Total record in the source | 38975 | 2019-11-17 23:41:40 |
+----------------------------+-------+---------------------+
```

You can see how the merge process keeps updating the records, but with constant data generation there are some data consistency delays.

After several minutes stop the data generation process. Within 2 minutes you should see an output similar to this one:
```
+----------------------------+-------+---------------------+
|        description         | count |     measured_ts     |
+----------------------------+-------+---------------------+
| Records not in destination |     0 | 2019-11-17 23:47:44 |
| Records not in source      |     0 | 2019-11-17 23:47:44 |
| Records with data mismatch |     0 | 2019-11-17 23:47:44 |
| Total record in the source | 45150 | 2019-11-17 23:47:44 |
+----------------------------+-------+---------------------+
```

This confirms that the merge process works correctly and data eventually becomes consistent.

## Purging processed data 
Note: in order to demo partition deletion you would need to run the merge process for a couple of days. Merge process doesn't have to run every 2 minutes, several times a day is all that's needed. 

To see which partitions of the delta table can be removed run these commands:
```
cd $DEMO_HOME/setup/bigquery/
cat get-session-delta-partitions-safe-to-delete.sql | bq query --use_legacy_sql=false
```

You will see an output similar to this:
```
+--------------+
| partition_id |
+--------------+
| 20191022     |
| 20191023     |
+--------------+
```

A simple script can be run to remove the partitions:
```
python remove-processed-session-delta-partitions.py
```

It will produce output similar to this one:
```
Partition to be deleted: TableReference(DatasetReference(u'bigquery-data-sync-demo', 'data'), 'session_delta$20191022')
Partition to be deleted: TableReference(DatasetReference(u'bigquery-data-sync-demo', 'data'), 'session_delta$20191023')
```


## Cleanup
```
cd $DEMO_HOME/setup
./remove-all.sh
```