################################################################################
#                               DO NOT MODIFY                                  #
################################################################################
 
# LIBRARIES
pip install pyyaml

## JARS
cp /dbfs/FileStore/libraries/nbb-default/*.jar /databricks/jars
 
## WHLS
pip install /dbfs/FileStore/libraries/nbb-default/preview/*.whl

# DEFAULT INIT SCRIPTS
bash /dbfs/FileStore/init_scripts/nbb-default/*.sh
################################################################################
#                          DEVELOPER'S INIT SCRIPT                             #
################################################################################
