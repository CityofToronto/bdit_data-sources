# How to Contribute

Help us document all the data that we use in our work! Fork this repo and then add a new dataset or improve on the code/documentation of an existing data set.

## What should go in this repo  

 - documentation documentation documentation, everything about the data
   - where it comes from,
   - what its good for,
   - how to import it,
   - where to find it,
   - how to use it,
   - what to watch out for when using it.
 - code for the extracting, transforming, validating, and loading of the data into our PostgreSQL data warehouse

## What should not go in this repo

* Code that is specific to an analysis project: this should go in that project's repo
* Information that might be hard to maintain: e.g. numbers of sensors, where they are located

### A reminder this repository is public

* Don't commit API keys or passwords
* Don't use specific names of other City staff to contact about issues, try instead to refer to units
* Don't include individual email addresses that could get scraped
* We want to maintain good working relationships with our vendors so try to avoid airing dirty laundary
  * Details of validation work should go in the validation repository
  * Yes our code and documentation will reflect data quality issues, but try to write about these in a neutral manner

## Folder structure

Each data source should have its own folder with a structure similar to below. In the root of that folder should be a `README` file explaining all the intricacies of the data, as well as the purpose of the most important code files. Code should be broken up by language (I think? Open to suggestions, by purpose?) into sub-folders.

```
drone-registrations/
├── README.md
├── sample_data.csv
├── sql
│   ├── create-table.sql
│   ├── create-index.sql
├── python
│   ├── copy-data.py
```

## Issues  

Feel free to open issues for project management or to request help. Each Issue title should probably be of the format `Data-Name: Issue Title`

## Dependency Management

If a new Python package is needed, follow these instructions to install it (ask an admin if you do not have enough permissions to do so):

1. Add the new Python package or update its version in the `requirements.in` file.
2. Run `pip-compile --resolver=backtracking requirements.in` to update the `requirements.txt` file.
3. Run `pip-sync requirements.txt` (as the `airflow` user) to install/update the new/updated packages.

## Improve this Contribution Guidelines!
