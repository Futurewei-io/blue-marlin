### 0.1.1
1. Project structure has been changed slightly. Folders tests and datagen are moved outside of main package.
2. Factdata Health-Check test is added to the tests.
3. A step to sanitize Factdata is added to the pipeline. This step is to:
    a. Remove invalid si
    b. Remove region
    c. Remaps ip based on ip-mapping table
    d. Recalculate bucket-id
4. Add TAG to config file. The whole set of tmp tables are named by product_tag and pipeline_tag. The user does not need to review the name of those tables anymore. 
5. Remove residency from UCKey. The value of residency is repleace by an empty string. The number of commas are still the same.
6. Remove region mapping for IPL.
7. Remove normalization of residency and IPL in main_norm.

### 0.1.2
1. Add docs folder

Note: From here, the version matches setup.py
### 2.0.0
1. New module added to process request-log table
2. Cluster module has been modifed to process uckeys with lot's of zeros
3. Outlier2 has been added to perform hampel on individual uckeys
4. Date format has been changed
