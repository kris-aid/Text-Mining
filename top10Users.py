import pandas as pd
import os

# Define the path to the directory with your CSV files
directory_path = 'Tweets_by_apellido'
output_directory = 'Output/Tweets'  # Output directory

# Create an empty DataFrame to store aggregated data
all_tweets_df = pd.DataFrame()

# Loop through each file in the directory
for filename in os.listdir(directory_path):
    if filename.endswith('.csv'):
        filepath = os.path.join(directory_path, filename)
        # Read the current CSV file into a DataFrame
        current_df = pd.read_csv(filepath)
        # Concatenate 'nombre' and 'apellido' into a new 'full_name' column
        current_df['full_name'] = current_df['nombre'] + ' ' + current_df['apellidos']
        # Concatenate the current DataFrame to the aggregated DataFrame
        all_tweets_df = pd.concat([all_tweets_df, current_df], ignore_index=True)

# Group the data by 'tweet_screen_name' and 'full_name', then count the number of tweets
tweet_counts = all_tweets_df.groupby(['tweet_screen_name', 'full_name']).size()

# Sort tweet counts in descending order and get the top 10
top_10_tweet_counts = tweet_counts.sort_values(ascending=False).head(10)

# Reset the index to get a DataFrame with screen name, full name, and count columns
top_10_df = top_10_tweet_counts.reset_index(name='tweet_count')

# Drop the 'tweet_screen_name' column
top_10_df = top_10_df.drop(columns=['tweet_screen_name'])

# Ensure the output directory exists
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Define the full output file path
output_file_path = os.path.join(output_directory, 'Top10Users.txt')

# Save the top 10 result to a text file in the specified directory
top_10_df.to_csv(output_file_path, index=False, sep='\t')

print(f'Top 10 tweet counts saved to {output_file_path}')
