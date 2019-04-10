# Storing historical centreline versions

Since the toronto centreline layer is ever changing and there are a bunch of datasets mapped to the centreline layer, we need to keep track of what changes are made to the layer over time and update our datesets with new attributes accordingly.

The `centreline_wh` layer avaliable in GCCVIEW stores all historical centreline versions and record them in these four attributes:

|Attribute name|Description|
|-----------------|-------------------------------------------------------------------------|
| date_effective  | Effective date from   trans_id_create                                 |  
| date_expiry     | Expiry date from trans_id_expiry                                     | 
| trans_id_create | Transaction created the   centreline segment                            |
| trans_id_expire | Transaction expired the centreline segment; -1 for current   centreline |  


Relationship between old and new versions of centreline:

![image](https://user-images.githubusercontent.com/46324452/55912374-8a0d2600-5bb0-11e9-8262-570460a9c326.png)

Now we know which features replaced which features by looking at the relationships of transaction id.

How do we link it and what type of reference table should we create:
(to be filled in)



A thought on possible use of SharedStreets for a reference table between old and new version:


SharedStreets might not be the best choice for this purpose as the `planet` that we are using might not capture all the old versions of centreline streets (not to mention geostatical lines and boundaries..). However, if we have a better understanding of how to link `reference_id` between two planet systems then we might be able to use SharedStreets, given the osm planet system captures all centerline changes.



