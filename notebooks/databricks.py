# Define the storage account and container details
storage_account_name = "abc"
input_container_name = "def"
output_container_name = "abc"
input_file_path = "data.csv"
output_file_path = "curated_data.csv"

# Set up the configuration for accessing the blob storage
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", "place-key-here")

# Read data from the input blob container
input_df = spark.read.format("csv").option("header", "true").load(f"wasbs://az@az.blob.core.windows.net/data.csv")

# Remove duplicates
deduplicated_df = input_df.dropDuplicates(["_c12"])

# Write the deduplicated data to the output blob container
deduplicated_df.write.format("csv").option("header", "true").mode("overwrite").save(f"wasbs://abca@def.blob.core.windows.net/curated_data.csv")

# Display the deduplicated DataFrame
display(deduplicated_df)
