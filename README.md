This tutorial shows how you can use this script to generate neccessary files which are used in installing our web map applications.

**Overview**

This is a python scripts uses ESRI ArcGIS geodatabase schema `.xml` file to generate following files which are required in installing Naghsara Co. web map applications:

```
layer_part.py               --> ./root/fill_db.py       
add_shapefile_part.py       --> ./root/fill_db.py
db_interface.py             --> ./root/db_interface_init.py
dictionary.js               --> ./root/static/config.js
```

**Usage**
* Create requirements:

1. Make sure that all python packages has already been installed.
2. Open ArcMap
3. From Catalog window, find interesting gedatabase
4. Do right click > export > XML Workspace Document ...
5. Check 'Schema Only'
6. Check 'Export Metadata'
7. Next
8. Finish

* Run Script
1. Put schema file `<schema_name>.xml` in `./root/schema`
2. got to `convert.py` > line 636 > replace `'file_name'` by `<schema_name>`
3. Run `convertor.py`

**Results**
1. Get back to `./root`
2. New directory has been created named same as `<schema_name> ("%Y.%m.%d %H.%M.%S")`
3. The 4 mentioned files can be found inside this directory
