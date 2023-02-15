# Airflow

We use [Airflow](airflow.apache.org/) to schedule our pipelines for data we can get from external data providers. This folder contains ["DAGs"](https://airflow.apache.org/docs/stable/tutorial.html#it-s-a-dag-definition-file) which are Python files that are configurations for each pipeline specify the data pipeline scripts to run, the schedule for them to run on, the order of tasks, and what to do if something fails. 

**Only put DAGs for data intake in this folder,** DAGs for data processing related to projects should be in their respective project repositories.

Consult our internal documentation for more tips and resources for writing DAGs.

## Deploying an Airflow Script on Our EC2

Our instance of Airflow runs as its own user `airflow` which resides in `/etc/airflow`. In order to ensure the code running on Airflow to be in sync with the code in GitHub, we have developed a unique folder structure. The important folders are:

- `/etc/airflow/dags`: for DAGs. In order for them to be synced to their files in `git`, it is preferable to symbolically link from this folder to the respective `dags` folder in the following two folders. For ex: while in `/etc/airflow/dags` run `ln -sf /etc/airflow/data_scripts/dags/pull_here.py .`
- `/etc/airflow/data_scripts`: a copy of this repository at the `master` branch
- `/etc/airflow/dev_scripts`: a copy of this repository on its own branch, into which users `git pull` their development branches. **Never ever ever ever `git push` from this folder**

## Common Tasks

### Creating a new Airflow DAG

1) Branch off `master` and do your work in your normal git folder, like you do.

2) Create your dag in the dag folder and if applicable, your related script in another appropriate folder.

3) Commit and push to GitHub.

4) Navigate to `/etc/airflow/dev_scripts` and pull your testing branch into `dev_scripts` using: 

```
git pull origin your_branch_name
```

**DO NOT PUSH ANYTHING IN THIS BRANCH**.

5) Navigate to `/etc/airflow/dag` 

6) Create a new symbolic link using:

```
ln -s /etc/airflow/dev_scripts/dags/your_dag.py your_dag.py
```

7) In a new browser tab, go to https://10.160.2.198/airflow/admin/ and enter your Airflow credentials.

**Note that the IP address is the address for the EC2 server (so if the EC2's IP address changes, the link will need to change too).**

8) Go to the DAGs tab and find your dag by searching or scrolling though the list. Click on the name. You should see a page devoted to your DAG with menu items like "Graph View" and "Tree View" (among other items) near the top. Click on the Trigger DAG option.

**Note that the name of the dag in Airflow is the same as the name in the dag variable within the .py script. It's a reallllly good idea to keep these the same, but if they are different, you may experience an unexpected scavenger hunt.**

9) Trigger your dag by clicking the "Trigger" button and watch what happens. Correct any errors shown in the log. Ensure any alerts are working as expected. Check the output where appropriate. Stay on this step until there are no more errors and everything is functioning splendidly.

10) Head back to GitHub and make a pull request for your pipeline branch to master.

11) Once your PR is approved and merged, navigate to `/etc/airflow/data_scripts` and git pull from master using:

```
git pull origin master
```
You might get an error that says `fatal: detected dubious owner in repository at '/etc/airflow/data_scripts'`.
If you get this error, don't panic! Git is keeping you safe by preventing you from navigating to a directory that could have a maliciously crafted `/scratch/.git/`.
We know that's not the case here, so if you get the error, just enter:
```
git config --global --add safe.directory '/etc/airflow/data_scripts'
```
...and git will know that everyone is safe! Yay!!!

12) Navigate to `/etc/airflow/dag` and change the symbolic link to your dag to use the one in `data_scripts` using: 

```
ln -s /etc/airflow/data_scripts/dags/your_dag.py your_dag.py
```

### Updating an existing airflow DAG

1) Branch off an appropriate branch and modify your dag in a testing branch, remember to change your dag name to something else.

2) Pull your testing branch into `dev_scripts`, **DO NOT PUSH ANYTHING IN THIS BRANCH**.

3) Test your dag on Airflow after adding a new symbolic link for testing using:
```
ln -s /etc/airflow/dev_scripts/dags/your_dag_test.py your_dag_test.py
```

4) When your test is successful, make a pull request to master.

5) Once your PR is approved and merged, git pull from master in `data_scripts`.

6) Change the symbolic link to your dag to use the one in `data_scripts`.

**Confused??? There are detailed instructions in the section above: Creating a new Airflow dag**

### Clearing a task

If you get the following error message when trying to clear a task in the UI

>*Oooops.
```python

                          ____/ (  (    )   )  \___
                         /( (  (  )   _    ))  )   )\
                       ((     (   )(    )  )   (   )  )
                     ((/  ( _(   )   (   _) ) (  () )  )
                    ( (  ( (_)   ((    (   )  .((_ ) .  )_
                   ( (  )    (      (  )    )   ) . ) (   )
                  (  (   (  (   ) (  _  ( _) ).  ) . ) ) ( )
                  ( (  (   ) (  )   (  ))     ) _)(   )  )  )
                 ( (  ( \ ) (    (_  ( ) ( )  )   ) )  )) ( )
                  (  (   (  (   (_ ( ) ( _    )  ) (  )  )   )
                 ( (  ( (  (  )     (_  )  ) )  _)   ) _( ( )
                  ((  (   )(    (     _    )   _) _(_ (  (_ )
                   (_((__(_(__(( ( ( |  ) ) ) )_))__))_)___)
                   ((__)        \\||lll|l||///          \_))
                            (   /(/ (  )  ) )\   )
                          (    ( ( ( | | ) ) )\   )
                           (   /(| / ( )) ) ) )) )
                         (     ( ((((_(|)_)))))     )
                          (      ||\(|(|)|/||     )
                        (        |(||(||)||||        )
                          (     //|/l|||)|\\ \     )
                        (/ / //  /|//||||\\  \ \  \ _)
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
#Stacktrace
TypeError: can't pickle _cffi_backend.CDataGCP objects
```

A [workaround](https://groups.google.com/g/cloud-composer-discuss/c/qWdaXZx-cuw/m/iMIdQClaCAAJ) is to 
going to 'Browse' -> 'Task Instances' -> check the instances that you want to clear -> With selected -> clear. 
