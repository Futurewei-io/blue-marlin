### 1.5
1. Project structure has been changed slightly. Folders tests and datagen are moved outside of main package.
2. Factdata Health-Check test is added to the tests.
3. A step to sanitize Factdata is added to the pipeline. This step is to:
    a. Remove invalid si
    b. Remove region
    c. Remaps ip based on ip-mapping table
    d. Recalculate bucket-id

### 1.6
1. Add residency and IPL features
2. Add pipeline and product tags to config file. The whole set of tmp tables are named by product_tag and pipeline_tag. The user does not need to review the name of those tables anymore. 