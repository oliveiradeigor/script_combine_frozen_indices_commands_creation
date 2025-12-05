import datetime
import re
import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.exceptions import ConnectionError, AuthenticationException

# Load environment variables from .env file
load_dotenv()

# Get values from .env
ELASTICSEARCH_CLOUD_ID = os.getenv("ELASTICSEARCH_CLOUD_ID")
ELASTICSEARCH_API_KEY = os.getenv("ELASTICSEARCH_API_KEY")

def connect_to_elasticsearch():
    """
    Establishes and tests a connection to Elasticsearch using environment variables.
    """
    try:
        client = Elasticsearch(
            cloud_id=ELASTICSEARCH_CLOUD_ID,
            api_key=ELASTICSEARCH_API_KEY
        )

        # Test the connection
        if client.ping():
            print("Connected to Elasticsearch successfully!")
        else:
            print("Failed to connect to Elasticsearch.")

        return client

    except AuthenticationException:
        print("Authentication failed. Check your API key in the .env file.")
    except ConnectionError:
        print("Connection error. Check your internet connection or Cloud ID in the .env file.")
    except Exception as e:
        print(f"An unexpected error occurred during connection: {e}")

    return None

def get_partial_indices_for_data_stream(data_stream_name: str, es_client: Elasticsearch) -> list[str]:
    """
    Fetches all backing indices prefixed with 'partial-' for a given data stream,
    excluding those that are from previous reindexing operations (partial-reindex-).
    """
    try:
        # Query for all backing indices of the data stream by providing the data stream name directly.
        # Then, filter the results in Python to only include indices that start with 'partial-'
        # AND do not start with 'partial-reindex-'.
        response = es_client.cat.indices(index=data_stream_name, format='json', h='index')
        partial_indices = [
            item['index'] for item in response
            if item.get('index') and
               item['index'].startswith('partial-') and
               not item['index'].startswith('partial-reindex-') # Exclude previously reindexed partials
        ]
        return partial_indices
    except Exception as e:
        print(f"Error fetching partial indices: {e}")
        return []

def get_lifecycle_date_millis(index_name: str, es_client: Elasticsearch) -> int | None:
    """
    Retrieves the lifecycle_date_millis for a given index.
    """
    try:
        # Corrected: Use perform_request for direct API call to {index_name}/_ilm/explain
        response = es_client.perform_request('GET', f'/{index_name}/_ilm/explain')

        if "indices" in response and response["indices"]:
            index_info = response["indices"].get(index_name)
            if index_info:
                return index_info.get("lifecycle_date_millis")
            else:
                # Fallback in case the exact key doesn't match but there's only one entry
                if len(response["indices"]) == 1:
                    return list(response["indices"].values())[0].get("lifecycle_date_millis")
        return None
    except NotFoundError:
        print(f"Index '{index_name}' not found for ILM explanation (NotFound error).")
        return None
    except Exception as e:
        print(f"Error getting lifecycle_date_millis for {index_name}: {e}")
        return None

def generate_elasticsearch_reindex_commands(data_stream_name: str, es_client: Elasticsearch) -> str:
    """
    Generates a series of Elasticsearch commands to reindex and manage
    backing indices within a data stream.
    """
    output = []

    if not es_client or not es_client.ping():
        output.append("❌ Elasticsearch client is not connected. Please ensure your .env file is configured correctly and the connection is successful.")
        return "\n".join(output)

    output.append(f"# Commands for Data Stream: `{data_stream_name}`")
    output.append("\n---\n")

    # --- Step 3: Reindex individual partial indices ---
    output.append("## Step 3: Reindex Individual Partial Indices (Run these in sequence)")
    partial_indices = get_partial_indices_for_data_stream(data_stream_name, es_client)
    individual_reindex_commands = []
    reindex_temp_indices = [] # Stores names of the new reindex- prefixed temporary indices

    if not partial_indices:
        output.append("⚠️ No partial indices found for the specified data stream. Please ensure the data stream name is correct and partial indices exist.")
    else:
        output.append("Here are the commands to reindex each `partial-` index into a temporary `reindex-` prefixed index:")
        # Sort indices to ensure consistent processing, especially for finding the 'most recent'
        partial_indices.sort()

        for index in partial_indices:
            # Extract the unique part of the index name (e.g., .ds-my-data-stream-2024.12.17-000001)
            # This regex assumes the format partial-.ds-<data-stream-name>-YYYY.MM.DD-00000X
            match = re.match(r"partial-(\.ds-.+-\d{4}\.\d{2}\.\d{2}-\d{6})", index)
            if match:
                base_name = match.group(1)
                dest_index = f"reindex-{base_name}"
                reindex_temp_indices.append(dest_index)
                command = f"""```json
POST /_reindex?wait_for_completion=false
{{
  "source": {{
    "index": "{index}"
  }},
  "dest": {{
    "index": "{dest_index}",
    "op_type": "create"
  }}
}}
```"""
                individual_reindex_commands.append(command)
            else:
                output.append(f"⚠️ Could not parse index name format for: {index}. Skipping reindex command for this index.")
        output.extend(individual_reindex_commands)

    # --- Step 4: Check document counts ---
    output.append("\n---\n")
    output.append("## Step 4: Compare Document Counts")
    if partial_indices and reindex_temp_indices:
        output.append("Use these commands to compare document counts between the original and newly reindexed temporary indices. Look for matching `docs.count`:")
        check_commands = []
        for i in range(len(partial_indices)):
            original = partial_indices[i]
            reindexed = reindex_temp_indices[i]
            command = f"""```bash
GET _cat/indices/{original},{reindexed}?v&expand_wildcards=all
```"""
            check_commands.append(command)
        output.extend(check_commands)
    else:
        output.append("⚠️ No indices to compare. Please ensure Step 3 generated temporary indices successfully.")

    # --- Step 5: Obtain lifecycle_date_millis ---
    output.append("\n---\n")
    output.append("## Step 5: Obtain `lifecycle_date_millis` for the Most Recent Index")
    lifecycle_date_millis = None
    if partial_indices:
        # Find the most recent index based on the suffix number (e.g., -000003 is more recent than -000001)
        def get_suffix_number(index_name):
            match = re.search(r'-(\d{6})$', index_name)
            return int(match.group(1)) if match else -1 # Use -1 to put unmatchable at the start

        most_recent_partial_index = max(partial_indices, key=get_suffix_number)
        output.append(f"We'll fetch the `lifecycle_date_millis` for the most recent partial index: `{most_recent_partial_index}`.")
        lifecycle_date_millis = get_lifecycle_date_millis(most_recent_partial_index, es_client)

        if lifecycle_date_millis:
            output.append(f"The `lifecycle_date_millis` obtained is: `{lifecycle_date_millis}`. This value is crucial for Step 7.")
        else:
            output.append("⚠️ Could not retrieve `lifecycle_date_millis`. This value is required for subsequent steps.")

        ilm_explain_command = f"""```bash
GET {most_recent_partial_index}/_ilm/explain
```"""
        output.append("Use the command below to manually verify the `lifecycle_date_millis` value:")
        output.append(ilm_explain_command)
    else:
        output.append("⚠️ Cannot obtain `lifecycle_date_millis` as no partial indices were found.")

    # --- Step 6: Consolidate into a large index ---
    output.append("\n---\n")
    output.append("## Step 6: Consolidate Temporary Indices into a New Large Index")
    current_date_str = datetime.date.today().strftime('%Y.%m.%d')
    large_index_name = f"reindex-.ds-{data_stream_name}-large-{current_date_str}"
    if reindex_temp_indices:
        temp_indices_list_str = ", ".join([f'"{idx}"' for idx in reindex_temp_indices])
        consolidate_command = f"""```json
POST _reindex?wait_for_completion=false
{{
  "source": {{
    "index": [{temp_indices_list_str}]
  }},
  "dest": {{
    "index": "{large_index_name}"
  }}
}}
```"""
        output.append(f"The temporary indices will be reindexed into a new, consolidated index named `{large_index_name}`:")
        output.append(consolidate_command)
    else:
        output.append("⚠️ No temporary reindex indices to consolidate. Please ensure Step 3 was successful.")

    # --- Step 7: Adjust Rollover Age and Manually Resolve ILM Error ---
    output.append("\n---\n")
    output.append("## Step 7: Adjust Rollover Age and Manually Resolve ILM Error")
    if lifecycle_date_millis and large_index_name:
        settings_command = f"""```json
PUT {large_index_name}/_settings
{{
  "index.lifecycle.indexing_complete": true,
  "index.lifecycle.origination_date": {lifecycle_date_millis}
}}
```"""
        output.append(f"First, apply the `lifecycle_date_millis` to the new large index (`{large_index_name}`) and mark it as complete for indexing. This is crucial for ILM to correctly manage its age:")
        output.append(settings_command)

        ilm_move_command = f"""```json
POST _ilm/move/{large_index_name}
{{
  "current_step": {{
    "phase": "hot",
    "action": "rollover",
    "name" : "ERROR"
  }},
  "next_step": {{
      "phase": "hot",
      "action": "complete"
  }}
}}
```"""
        output.append("If, after the settings update, the index enters an ILM ERROR state (which is expected as it doesn't have a traditional write alias), use this command to manually move it past the error phase and allow ILM to continue its lifecycle:")
        output.append(ilm_move_command)
    else:
        output.append("⚠️ Cannot generate Step 7 commands without a valid `lifecycle_date_millis` or the large index name.")

    # --- Step 8: Modify Data Stream ---
    output.append("\n---\n")
    output.append("## Step 8: Modify Data Stream (Add/Remove Backing Indices) and Confirm")
    if partial_indices and large_index_name:
        remove_actions = []
        for index in partial_indices:
            remove_actions.append(f"""    {{
      "remove_backing_index": {{
        "data_stream": "{data_stream_name}",
        "index": "{index}"
      }}
    }}""")
        remove_actions_str = ",\n".join(remove_actions)

        modify_data_stream_command = f"""```json
POST _data_stream/_modify
{{
  "actions": [
{remove_actions_str},
    {{
      "add_backing_index": {{
        "data_stream": "{data_stream_name}",
        "index": "{large_index_name}"
      }}
    }}
  ]
}}
```"""
        output.append(f"Now, replace the old `partial-` indices with the new large index (`{large_index_name}`) within data stream `{data_stream_name}`. This will make the new index part of your data stream:")
        output.append(modify_data_stream_command)

        confirm_command = f"""```bash
GET _data_stream/{data_stream_name}
```"""
        output.append("Confirm the swap was successful and that the new large index is listed under your data stream's indices using this command:")
        output.append(confirm_command)
    else:
        output.append("⚠️ Cannot generate Step 8 commands without partial indices or a large index name.")

    # --- Step 9: Delete Old Indices ---
    output.append("\n---\n")
    output.append("## Step 9: Delete Old and Temporary Indices")
    indices_to_delete = partial_indices + reindex_temp_indices
    if indices_to_delete:
        # Corrected: Join with ',' instead of ', ' for single-line format with no spaces
        delete_list_str = ",".join(indices_to_delete)
        output.append("Once you have confirmed that all steps are successful and your data stream is functioning correctly with the new large index, you can safely delete the old `partial-` indices and the temporary `reindex-` indices (but NOT the `reindex-.ds-<data-stream-name>-large` index!):")
        delete_command = f"""```bash
DELETE {delete_list_str}
```"""
        output.append(delete_command)
    else:
        output.append("✅ No old or temporary indices found to delete. If this is unexpected, verify previous steps.")

    return "\n".join(output)

# Run connection function
es_client = connect_to_elasticsearch()

# Example usage with your connected client:
data_stream_name_to_process = "logs-filestream.log_generator-default" # <<< IMPORTANT: Change this to your actual data stream name
if es_client:
    generated_commands = generate_elasticsearch_reindex_commands(data_stream_name_to_process, es_client)
    print(generated_commands)
else:
    print("Failed to connect to Elasticsearch. Cannot generate commands.")
