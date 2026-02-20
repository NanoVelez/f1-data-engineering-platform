# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

# 1. AUTOMATIC INSTALLATION (The missing magic line)
# This ensures the library exists before trying to import it
%pip install semantic-link-labs

# 2. IMPORT AND LOGIC
import sempy_labs as sl
import time

# --- AUTOMATIC CONFIGURATION ---
# By leaving it as None, it will apply to the notebook's current workspace and lakehouse
MODEL_NAME = "F1_Gold_Model"  # Make sure this is the exact name

print(f"Starting automatic model remapping: {MODEL_NAME}")

try:
    # 1. Remap the connection (Wire to the current Lakehouse)
    print("   Searching for attached Lakehouse to reconnect...")
    sl.directlake.update_direct_lake_model_connection(
        dataset = MODEL_NAME,
        source_type = "Lakehouse",
        use_sql_endpoint = True
    )
    print("   Connection successfully remapped.")

    # 2. Synchronize the schema (Read the new tables)
    print("   Synchronizing schema...")
    sl.directlake.direct_lake_schema_sync(
        dataset = MODEL_NAME
    )
    print("   Schema synchronized.")

    print("\nDONE! The model now points to your local data.")

except Exception as e:
    print(f"\nError: {e}")
    print("HINT: Have you added a Lakehouse to the left panel ('Lakehouses') of this notebook?")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
