import os
import pandas as pd
import matplotlib.pyplot as plt

# The directory containing the data files
data_dir = './res_stats'

# Create a folder to store the plots
plots_dir = os.path.join(data_dir, 'plots')
os.makedirs(plots_dir, exist_ok=True)

# Get all files starting with "avg" in the directory
avg_files = [file for file in os.listdir(data_dir) if file.startswith('avg')]

# Iterate over each file in the directory
for filename in os.listdir(data_dir):
    # Check if the filename starts with 'avg' and contains 'EHAM'
    if filename.startswith('avg') and 'EHAM' in filename:
        file_path = os.path.join(data_dir, filename)
        
        # Check if it's a file
        if os.path.isfile(file_path):
            # Read the file into a DataFrame
            df = pd.read_csv(file_path)
            
            # Combine month and year into a date
            df['date'] = pd.to_datetime(df['month(firstseen)'].astype(str) + df['year(firstseen)'].astype(str), format='%m%Y')
            
            # Convert 'avg_time_difference' from seconds to minutes
            df['avg_time_difference'] = df['avg_time_difference'] / 60
            
            # Set 'date' as the index
            df.set_index('date', inplace=True)
            
            # Sort the DataFrame by the 'date' index
            df.sort_index(inplace=True)
            
            # Create a line plot
            plt.plot(df.index, df['avg_time_difference'])
            plt.xlabel('Date')
            plt.ylabel('Average Time Difference (minutes)')
            plt.title('Average Time Difference Over Time')
            
            # Rotate the x-axis labels vertically
            plt.xticks(rotation='vertical')
            
            # Save the plot as an image file
            plot_filename = os.path.splitext(filename)[0] + '.png'
            plot_filepath = os.path.join(plots_dir, plot_filename)
            plt.savefig(plot_filepath)
            
            # Clear the plot for the next iteration
            plt.clf()
