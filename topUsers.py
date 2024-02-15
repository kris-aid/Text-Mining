import pandas as pd
import os
import matplotlib
matplotlib.use('Agg')  # Set backend to a non-interactive backend 'Agg'
import matplotlib.pyplot as plt


def calcTopUsers(max_users=10):
    print("Calculating top ",max_users," users...")
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
            #retrieve first "apellido" and store it in "apellido"
            current_df['apellido'] = current_df['apellidos'].str.split(' ').str[0]
            # Concatenate the current DataFrame to the aggregated DataFrame
            all_tweets_df = pd.concat([all_tweets_df, current_df], ignore_index=True)

    # Group the data by 'tweet_screen_name' and 'full_name', then count the number of tweets
    tweet_counts = all_tweets_df.groupby(['tweet_screen_name', 'full_name','apellido']).size()

    # Sort tweet counts in descending order and get the top 10
    top_10_tweet_counts = tweet_counts.sort_values(ascending=False).head(max_users)

    # Reset the index to get a DataFrame with screen name, full name, and count columns
    top_10_df = top_10_tweet_counts.reset_index(name='tweet_count')

    # Ensure the output directory exists
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)


    # Save the top 10 result to a text file in the specified directory, excluding 'tweet_screen_name'
    output_csv_path = os.path.join(output_directory, 'Top' + str(max_users) + 'Users.csv')
    top_10_df.to_csv(output_csv_path, index=False, header=True)


    # Generate a bar chart using the full name and tweet count
    plt.figure(figsize=(10, 8))
    plt.barh(top_10_df['full_name'], top_10_df['tweet_count'])
    plt.xlabel('Number of Tweets')
    plt.ylabel('Full Name')
    plt.title('Top 10 People with the Most Tweets')
    plt.gca().invert_yaxis()  # Invert y-axis to have the highest count on top
    plt.tight_layout()

    # Define the full output file path for the chart image
    output_chart_path = os.path.join(output_directory, 'Top'+ str(max_users) +'_Users_Chart.png')

    # Save the chart
    plt.savefig(output_chart_path)

    print(f"Chart saved to {output_chart_path}")

