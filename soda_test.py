from soda.scan import Scan # importing the Soda library



scan = Scan() # loading the function
scan.set_data_source_name("test") # initialising the datasource name

# Loading the configuration file
scan.add_configuration_yaml_file(
    file_path="soda_configuration.yml"
)


# Loading the check yaml file
scan.add_sodacl_yaml_file("checks.yml")

# Executing the scan
scan.execute()

# Setting logs to verbose mode
scan.set_verbose(True)

# Inspect the scan result
print(scan.get_scan_results())

