import pandas as pd
import os
from collections import Counter
import nltk
from nltk.tokenize import word_tokenize
from MyUtils import create_folder

nltk.download('punkt')

create_folder('Output/Tweets/Top10Words')

# Step 1: Read the Top10Users.csv file
top_users_df = pd.read_csv('Output/Tweets/Top10Users.csv')
top_users = top_users_df['tweet_screen_name'].values.tolist()

# Set your Tweets folder and output folder
tweets_folder = 'Output/Tweets/lemmas'
output_folder = 'Output/Tweets/Top10Words'

# Ensure the output folder exists
create_folder(output_folder)

# Iterate over the top users and calculate term frequencies
for top_user in top_users:
    # Initialize Counter object for the user
    term_freq = Counter()

    # Iterate over all files in the tweets_folder
    for file_name in os.listdir(tweets_folder):
        if file_name.endswith('.csv'):
            # Load the CSV file
            df = pd.read_csv(os.path.join(tweets_folder, file_name))
            
            # Filter tweets for the current top_user
            user_tweets = df[df['tweet_screen_name'] == top_user]

            # Concatenate all tweet texts into a single string
            tweets_combined = ' '.join(user_tweets['tweet_text'].astype(str))

            # Tokenize the text
            tokens = word_tokenize(tweets_combined)

            # Update the term frequency Counter
            term_freq.update(tokens)
    
    # Convert term frequencies to a DataFrame
    freq_df = pd.DataFrame(term_freq.items(), columns=['term', 'frequency'])
    
    # Sort DataFrame by frequency in descending order
    freq_df.sort_values(by='frequency', ascending=False, inplace=True)
    
    # Write the frequency DataFrame of the current user to a CSV file
    freq_df.to_csv(os.path.join(output_folder, f'{top_user}.csv'), index=False)
