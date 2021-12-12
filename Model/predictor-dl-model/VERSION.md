### 1.5
1. Project structure has been changed slightly. Folders tests and datagen are moved outside of main package.
2. Factdata Health-Check test is added to the tests.
3. A step to sanitize Factdata is added to the pipeline. This step is to:
    a. Remove invalid si
    b. Remove region
    c. Remaps ip based on ip-mapping table
    d. Recalculate bucket-id

### 1.6
1. Add region and IPL features
2. Add TAG to config file. The whole set of tmp tables are named by product_tag and pipeline_tag. The user does not need to review the name of those tables anymore. 

### 1.7
1. Remove residency from UCKey. The value of residency is repleace by an empty string. The number of commas are still the same.
2. Remove region mapping for IPL.
3. Remove normalization of residency and IPL in main_norm.