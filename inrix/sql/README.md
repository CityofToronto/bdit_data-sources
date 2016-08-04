## SQL scripts to manage Inrix data in PostgreSQL

**DO NOT USE bulk_copy sql scripts**: These loop over all the data files on the server, but cannot commit after each `COPY`, so this eventually leads to a lot of blocking overhead. I will be working on a python script to loop over all files in a given folder and `CREATE` a table for each and then `COPY` the file using some performance tweaks.