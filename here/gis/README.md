# HERE GIS Data

Here GIS data is provided through the Enterprise Data Download portal. Each Revision contains ~41 layers provided for the province of Ontario. Consult the encyclopedic `NAVSTREETS Street Data Reference Manual.pdf` in the K drive for further info on a layer.

## Loading New Data

After downloading to the EC2, uncompress and un-tar the file with the below command. Some versions of this download included all provinces. Each province folder within the tar contains another compressed tar for that folder. Ontario's folder starts with `2E` (from `"Region Code: 2E"`).

```shell
# List the contents of the tar to identify the Ontario folder starting with 2E
tar --exclude="*/*/*" -tf HERE_GIS_DATA.tar
mkdir HERE_GIS_DATA_R/
#the 2E* wildcard should probably work
tar --wildcards "2E*/*" -xf HERE_GIS_DATA.tar -C HERE_GIS_DATA_R/
```

## Importing New GIS layers

There are three shapefiles in the unzipped folder that we need to import:
1) `Streets.shp`` - Shapefile of all new here links
2) `Zlevels.shp`` - Zlevels of all new nodes
3) `Adminbndy3.shp`` - Admin boundary of municipalities

### Current way of importing 

[`batch_upload.sh`](batch_upload.sh) locates in `/data/here/gis`, is a shell script that loops over the three shapefiles, and directly import the layers into the `here_gis` schema using `ogr2ogr`. `ogr2ogr` allows direct import from zipped or compressed files using `vsitar` or `vsizip`(You might have to update the bash script base on the type of file received, `vsitar` for `tar` and `vsizip` for `zip`). Note that this shortened use of `psql` assumes the prompted username and that the password for that user is stored in a [`.pgpass`](https://www.postgresql.org/docs/current/static/libpq-pgpass.html) file in your home directory. 

Run the bash script with:

```shell
nohup bash batch_upload.sh > batch_upload.log& tail -f batch_upload.log
```

It prompts for:
- your bigdata postgres username
- your bigdata posrgres password
- revision number (e.g. 25_1)
- directory of the shapefiles
  
Example entry:
```
What is your bigdata username?username
What is your bigdata password?password
Which map version are you trying to import?25_1
What directory are the shapefiles in?ON_2025_Q4_HERE_SHAPEFILES.zip/2EAM251G0N2E000AACU8
```

Prior to running `ogr2ogr` the script remove the `.shp` string to turn it into a compatible tablename for PostgreSQL. Tables are versioned by appending `YY_R` to their names where YY is the year and R is the revision number.

After the data is loaded, `psql` is called again to alter each table's owner to the here_admins group and then run [`here_gis.clip_to(tablename, revision)`](clip_to.sql) to clip the layer to within the City's boundary. `Adminbndy3` layer will be imported first as we use the Toronto municipalites boundary defined in this layer for clipping purposes.

Subsequently [`split_streets_att.sql`](split_streets_att.sql) can be run to split the streets layer into a GIS layer and an attributes table (there are a lot of columns in the attributes table) in order to reduce the size of the streets layer when loading it in QGIS.


**Note:** Please add a [`COMMENT`](https://devdocs.io/postgresql~9.6/sql-comment) to the `streets_YY_R` layer explaining which years of traffic data should use that layer.


### Derivative gis layers to update

There are a few additional things to create based on the new map layer:

- [`function_create_routing_tables.sql`](sql/function_create_routing_tables.sql): creates routing nodes, traffic streets, and routing streets layers. Run `SELECT here.function_create_routing_tables(ref_yr)` to create layers.
- `here_gis.traffic_streets`

## Reference Node

Links are defined by a reference node, and a non-reference node. The reference
node is *always* the node with the lowest latitude, in the case of two nodes
with equal latitude, the node with the lowest longitude is the reference node.
This impacts things like traffic direction, which can be towards or from a
reference node, or addressing whether addresses are on the right or left side
of a link (facing the non-reference node).
