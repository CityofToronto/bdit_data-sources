# How to Contribute

Help us document all the data that we use in our work! Fork this repo and then add a new dataset or improve on the code/documentation of an existing data set.

## What should go in this repo  

 - sample data
 - documentation documentation documentation, everything about the data, where it comes from, what its good for, how to import it
 - basic code for importing into the PostgreSQL database or basic processing.
 
**Code that is specific to a project should go in that project's repo**

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

## Improve this Contribution Guidelines!