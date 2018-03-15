# HERE GIS Data

Here GIS data is provided through the Enterprise Data Download portal. Each Revision contains ~41 layers provided for the province of Ontario. Consult the encyclopedic `NAVSTREETS Street Data Reference Manual.pdf` in the K drive for further info on a layer.

## Loading New Data 

After downloading to the EC2, uncompress and un-tar the file with
```shell
mkdir HERE_GIS_DATAR1/
tar -xvf HERE_GIS_DATA.tar.gz -C HERE_GIS_DATAR1/
```

The second version of this download included all provinces. Running `du -sh *` will show the size of the different province folders. Ontario was the largest folder, and appears to have `"Region Code: 2E"`. 

The [`batch_upload.sh`](batch_upload.sh) shell script loops over each shape file and pipes the output of [`shp2pgsql`](http://postgis.net/docs/manual-2.1/using_postgis_dbmanagement.html#shp2pgsql_usage) to psql ([inspiration](http://gis.stackexchange.com/a/7806/36886)) to upload into the `here_gis` schema. Not that this shortened use of `psql` assumes the current Ubuntu username and that the password for that user is stored in a [`.pgpass`](https://www.postgresql.org/docs/current/static/libpq-pgpass.html) file in your home directory. Edit the `rev` variable at the start to specify the revision of the geographies. This will be appended to the uploaded tables' names. 

Prior to running `shp2pgsql` the script performs some manipulation of the `$f` filename variable in order to lowercase it and remove the `.shp` string to turn it into a compatible tablename for PostgreSQL. Tables are versioned by appending `YY_R` to their names where YY is the year and R is the revision number.

After the data is loaded, `psql` is called again to alter each table's owner to the here_admins group and then run [`here_gis.clip_to(tablename, revision)`](clip_to.sql) to clip the layer to within the City's boundary. 

Subsequently [`split_streets_att.sql`](split_streets_att.sql) can be run to split the streets layer into a GIS layer and an attributes table (there are a lot of columns in the attributes table) in order to reduce the size of the streets layer when loading it in QGIS.

Run the shell script in the background with nohup with the following command. The `tail -f` piece will continually display the tail of the log in the terminal. To cancel the command do `CTRL-C` to stop `tail` and then `fg` to return the job to the foreground and then `CTRL-C` again.
```shell
nohup ./batch_upload.sh > batch_upload.log& tail -f batch_upload.log
```

## Reference Node

Links are defined by a reference node, and a non-reference node. The reference node is *always* the node with the lowest latitude, in the case of two nodes with equal latitude, the node with the lowest longitude is the reference node. This impacts things like traffic direction, which can be towards or from a reference node, or addressing whether addresses are on the right or left side of a link (facing the non-reference node). 