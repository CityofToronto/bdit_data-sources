- [Airflow](#airflow)
  - [DAG Naming](#dag-naming)
  - [Contents of DAGs Folder](#contents-of-dags-folder)
    - [Weather](#weather)
    - [Misc](#misc)
    - [Here](#here)
    - [Replication](#replication)
    - [Ecocounter](#ecocounter)
    - [GIS](#gis)
    - [**sql**](#sql)
    - [**custom\_operators.py**](#custom_operatorspy)
    - [**common\_tasks.py**](#common_taskspy)
    - [**dag\_functions.py**](#dag_functionspy)
    - [Miovision](#miovision)
    - [VDS (Formerly RESCU)](#vds-formerly-rescu)
    - [WYS](#wys)
  - [Common Tasks](#common-tasks)
    - [Deploying an Airflow Script on Our EC2](#deploying-an-airflow-script-on-our-ec2)
    - [Creating a new Airflow DAG](#creating-a-new-airflow-dag)
    - [Advice for updating an existing airflow DAG](#advice-for-updating-an-existing-airflow-dag)

# Airflow

We use [Airflow](airflow.apache.org/) to schedule our pipelines for data we can get from external data providers, and for scheduling internal aggregations. This folder contains ["DAGs"](https://airflow.apache.org/docs/stable/tutorial.html#it-s-a-dag-definition-file) which are Python files that specify the data pipeline scripts to run, the schedule for them to run on, the order of tasks, and what to do if something fails for each pipeline. 

Consult our internal documentation for more tips and resources for writing DAGs.

## DAG Naming
Our preferred naming convention for DAGs is **`dataset`_`verb`**. The .py file should also ideally match the DAG name.  

You may notice that many older DAGs have not been renamed to this standard: it is usually done as part of a major refactor since renaming the DAG makes the history inaccessible from the UI. 

## Contents of DAGs Folder
**Only put DAGs for data intake in this folder,** DAGs for data processing related to projects should be in their respective project repositories.

### Weather
- **[weather_pull.py](weather_pull.py)**: [readme](../weather/README.md#data-pipeline---weather_pull-dag).  
- Deprecated: [pull_weather.py](pull_weather.py)

### Misc
- [**log_cleanup.py**](log_cleanup.py): A maintenance workflow to periodically clean out the Airflow task logs to avoid those getting too big.
- [eoy_create_tables.py](eoy_create_tables.py): An end of year maintenance DAG which creates new partitions and maintains calendar tables.  

### Here
- **[pull_here.py](pull_here.py)**: [readme](../here/traffic/README.md#probe_path).  
- **[pull_here_path.py](pull_here_path.py)**: [readme](../here/traffic/README.md#path).  
- HERE Aggregations: [citywide_tti_aggregate.py](citywide_tti_aggregate.py).

### Replication
- [**replicators.py**](replicators.py): creates collisions and counts replicator DAGs as part of the MOVE -> bigdata replication process.   
- [**replicator_table_check.py**](replicator_table_check.py): Monitors the results of the collisions and counts replicator DAGs.  
- Deprecated: [collisions_replicator_transfer.py](collisions_replicator_transfer.py), [traffic_transfer.py](traffic_transfer.py).

### Ecocounter
- [**ecocounter_pull.py**](ecocounter_pull.py): [readme](../volumes/ecocounter/readme.md#ecocounter_pull-dag).  
- [**ecocounter_check.py**](ecocounter_check.py): [readme](../volumes/ecocounter/readme.md#ecocounter_check-dag).  

### GIS
- [**assets_pull.py**](assets_pull.py): [readme](../gis/assets/README.md#assets).  
- [**gcc_layers_pull.py**](gcc_layers_pull.py): [readme](../gis/gccview/README.md#gccview-pipeline).  
- [**vz_google_sheets.py**](vz_google_sheets.py): [readme](../gis/school_safety_zones/README.md#2-the-automated-data-pipeline).  
- Deprecated: [**pull_interventions_dag.py**](pull_interventions_dag.py).

### [**sql**](./sql/)
This folder contains generic sql scripts which are used by various volumes data checks via Airflow PostgresOperator + jinja templating. 

### [**custom_operators.py**](custom_operators.py)  
Contains custom Airflow Operators.  
- `SQLCheckOperatorWithReturnValue`: A custom Airflow SQLCheckOperator that extends the original operator to return the SQL check's return value.  

### [**common_tasks.py**](common_tasks.py)  
Contains common Airflow Task definitions which can be used in multiple DAGs.
- `wait_for_external_trigger`: Reusable sensor to wait for external DAG trigger.  
- `get_variable`: Task to access an airflow variable and return value as XCOM.  
- `copy_table`: A task to copy a postgres table contents (and comment) from one location to another within the same database. 
- `check_jan_1st`, `check_1st_of_month`: `short_circuit` operators which can be used to have downstream tasks occur only on Jan 1 / 1st of each month.  
- `check_if_dow`: A `short_circuit` operator which checks a date against a day of week and short circuits (skips downstream tasks) if not.  
- `wait_for_weather_timesensor`: returns a `TimeSensor` Airflow operator which can be used to delay data checks until the time of day when historical weather is avaialble.  

### [**dag_functions.py**](dag_functions.py)
Contains helper functions to be used in multiple DAGs.  
- `is_prod_mode`: helper function to determine of slack messages should be sent to dev or prod slack channels.  
- `task_fail_slack_alert`: Sends Slack task-failure notifications.  
- `send_slack_msg`: Send Slack notifications (not just for failures).  
- `get_readme_docmd`: helper function to extract text from a readme file for use as an Airflow doc_md, which is displayed in the Airflow UI.  
- `check_not_empty`: checks a SQL table has non-zero row count. 

### Miovision
- **[miovision_pull.py](miovision_pull.py)**: [readme](../volumes/miovision/api/readme.md#miovision_pull).  
- **[miovision_check.py](miovision_check.py)**: [readme](../volumes/miovision/api/readme.md#miovision_check).  
- Deprecated: [pull_miovision.py](pull_miovision.py), [check_miovision.py](check_miovision.py).  

### VDS (Formerly RESCU)
- **[vds_check.py](vds_check.py)**: [readme](../volumes/vds/readme.md#vds_check-dag).  
- **[vds_pull_vdsdata.py](vds_pull_vdsdata.py)**: [readme](../volumes/vds/readme.md#vds_pull_vdsdata-dag).  
- **[vds_pull_vdsvehicledata.py](vds_pull_vdsvehicledata.py)**: [readme](../volumes/vds/readme.md#vds_pull_vdsvehicledata-dag).  
- Deprecated: [check_rescu.py](check_rescu.py). 

### WYS
- [**refresh_wys_monthly.py**](refresh_wys_monthly.py): [readme](../wys/api/README.md#wys_monthly_summary).
- [**pull_wys.py**](pull_wys.py): [readme](../wys/api/README.md#pull_wys).
- [**wys_check.py**](wys_check.py): Contains additional data quality checks for `pull_wys`.  

## Common Tasks

### Deploying an Airflow Script on Our EC2

Our instance of Airflow runs as its own user `airflow` which resides in `/data/airflow`. In order to ensure the code running on Airflow to be in sync with the code in GitHub, we have developed a unique folder structure. The important folders are:

- `/data/airflow/data_scripts`: a copy of this repository at the `master` branch
- `/data/airflow/dags`: Airflow looks for DAGs in this folder. In order for them to be synced to their files in `git`, symbolically link this folder to the respective `dags` folder in the above `data_scripts` folder. For example: while in `/data/airflow/dags` run `ln -sf /data/airflow/data_scripts/dags/pull_here.py.` Examine the symbolic links using `ls -lha`. 
- `/data/airflow/dev_scripts`: a location which can be used to link DAGs to branches other than the `master` branch for testing. 

### Creating a new Airflow DAG

1) Branch off `master` and do your work in your normal git folder, like you do.

2) Create your dag in the dag folder and if applicable, your related script in another appropriate folder.

3) Commit and push to GitHub.

4) Give `airflow` user access to your home folder by entering this command in the EC2 terminal:

```
setfacl -m u:airflow:rwx <REPLACE-WITH-YOUR-HOME-FOLDER>
```

5) Give `airflow` access to the repo and all of its contents (including the DAG folder and any other helper files) by entering this command in the EC2 terminal:

```
setfacl -R -m u:airflow:rwx <REPLACE-WITH-THE-REPO-FOLDER>
``` 

6) Create a new symbolic link using:

```
ln -sf <PATH-TO-DAG-ON-YOUR-HOME-FOLDER> airflow/dags/<DAG-FILE>
```
If you get a "permission denied" error, ask a sys admin to do this step for you. The sys admin may transform into everyone's favourite super hero "Super User" using:
```
sudo su - airflow
ln -sf <PATH-TO-DAG-ON-YOUR-HOME-FOLDER> airflow/dags/<DAG-FILE>
```

7) In a new browser tab, go to https://`EC2 IP address`/airflow/ and enter your Airflow credentials.

**Note that the IP address is the address for the EC2 server (so if the EC2's IP address changes, the link will need to change too).**

8) Go to the DAGs tab and find your dag by searching or scrolling though the list. Click on the name. You should see a page devoted to your DAG with menu items like "Graph View" and "Tree View" (among other items) near the top. Click on the Trigger DAG option.

**Note that the name of the dag in Airflow is the same as the name in the `dag_id` variable within the .py script. It's a reallllly good idea to keep these the same, but if they are different, you may experience an unexpected scavenger hunt.**

9) Trigger your dag by clicking the "Trigger" button and watch what happens. Correct any errors shown in the log. Ensure any alerts are working as expected. Check the output where appropriate. Stay on this step until there are no more errors and everything is functioning splendidly.

10) Head back to GitHub and make a pull request for your pipeline branch to master. Once your pull request is approved, a sys admin will handle the rest so please be sure to specify anything that needs to happen to get the DAG running. It may include things such as:
- Changing references to personal schemas in any supporting sql scripts
- Moving the DAG from your personal home folder to `/data/airflow/data_scripts`
- Mentioning any DAGs that this DAG depends on, or other DAGs that depend on this DAG

11) **For sys admins only:** Once the PR is approved and merged, complete all of the sys admin tasks listed in the PR. Then, navigate to `/data/airflow/data_scripts` and git pull from master using:

```
git pull origin master
```
You might get an error that says `fatal: detected dubious owner in repository at '/data/airflow/data_scripts'`.
If you get this error, don't panic! Git is keeping you safe by preventing you from navigating to a directory that could have a maliciously crafted `/scratch/.git/`.
We know that's not the case here, so if you get the error, just enter:
```
git config --global --add safe.directory '/data/airflow/data_scripts'
```
...and git will know that everyone is safe! Yay!!!

12) **For sys admins only:** Navigate to `/data/airflow/dag` and change the symbolic link to the DAG in `data_scripts` using: 

```
ln -s /data/airflow/data_scripts/dags/your_dag.py your_dag.py
```

### Advice for updating an existing airflow DAG

- Consider copying the DAG and renaming it. If there are two DAGs with the same name, you may experience unpredictable behaviour. 
- Consider that if you rename tasks, the old ones will disappear from the UI: this will make things harder to debug and may not be worth the trouble. This also occurs if you move a task into a task group. 
- Make sure the temp DAG and the existing DAG do not conflict (inserting into the same table, for example). Use personal schema or temp tables for testing where possible. 
