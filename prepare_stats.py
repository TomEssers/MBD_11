import os
import pandas as pd

# The parent directory containing the 'stats' folder
parent_dir = '.'

# The 'stats' directory
stats_dir = os.path.join(parent_dir, 'stats')

# Iterate over each subfolder in the 'stats' directory
for subfolder in os.listdir(stats_dir):
    subfolder_path = os.path.join(stats_dir, subfolder)
    
    # Check if it's a directory
    if os.path.isdir(subfolder_path):
        # Create an empty DataFrame to store the data from this subfolder
        df = pd.DataFrame()
        
        # Iterate over each file in the subfolder
        for filename in os.listdir(subfolder_path):
            file_path = os.path.join(subfolder_path, filename)
            
            # Check if it's a file, not named _SUCCESS, and has a .csv extension
            if os.path.isfile(file_path) and not filename.startswith('_SUCCESS') and filename.endswith('.csv'):
                # Read the file into a DataFrame
                print(file_path)
                df_file = pd.read_csv(file_path)
                
                # Concatenate the file's DataFrame to the subfolder's DataFrame
                df = pd.concat([df, df_file], ignore_index=True)
        
        # Write the subfolder's DataFrame to a CSV file
        df.to_csv(f"res_stats/{subfolder}.csv", index=False)
        
        # Remove the subfolder directory
        
        
