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
