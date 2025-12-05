# script_combine_frozen_indices_commands_creation
This is a python code to simply create the commands to combine small frozen indices (partial-) in to a larger one.

The env file should be changed to .env, as this need to be a hidden file.

For every new data_stream you want to work, modify on the main.py the line 299:
data_stream_name_to_process = "your_datastream_name" # <<< IMPORTANT: Change this to your actual data stream name 

Example:
data_stream_name_to_process = "logs-filestream.log_generator-default" 
