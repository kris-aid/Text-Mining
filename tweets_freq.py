import pandas as pd
import os
from collections import Counter
import nltk
from nltk.tokenize import word_tokenize
from MyUtils import delete_create_folder
from math import log
import json

nltk.download('punkt')

def tweets_frequency(max_users =10,max_rows=10):

    # Define JSON output folder
    json_output_folder = 'Output/Tweets/Top' + str(max_rows) + 'WordsJson'
    # Ensure the JSON output folder exists
    delete_create_folder(json_output_folder)

    delete_create_folder('Output/Tweets/Top'+ str(max_rows) +'Words')

    # Step 1: Read the Top10Users.csv file
    top_users_df = pd.read_csv('Output/Tweets/Top'+ str(max_users) +'Users.csv')
    top_users = top_users_df['tweet_screen_name'].values.tolist()

    # Set your Tweets folder and output folder
    tweets_folder = 'Output/Tweets/lemmas'
    output_folder = 'Output/Tweets/Top'+ str(max_rows) +'Words'

    # Ensure the output folder exists
    delete_create_folder(output_folder)

    # Iterate over the top users and calculate term frequencies
    for top_user in top_users:
        apellido = top_users_df[top_users_df['tweet_screen_name'] == top_user]['apellido'].values[0]
        apellido = apellido.lower()
        term_freq = Counter()

        # Load tweets for the current top_user
        files_tweets = []
        for file_name in os.listdir(tweets_folder):
            if file_name.endswith('.csv'):
                # Load the CSV file
                df = pd.read_csv(os.path.join(tweets_folder, file_name))
                user_tweets = df[df['tweet_screen_name'] == top_user]
                files_tweets.extend(user_tweets['tweet_text'].astype(str).tolist())

        # N is the total number of tweets for the user
        N = len(files_tweets)

        # Tokenize the text and update term frequencies
        for tweet in files_tweets:
            tokens = word_tokenize(tweet)
            term_freq.update(tokens)

        # Convert term frequencies to a DataFrame
        freq_df = pd.DataFrame(term_freq.items(), columns=['term', 'tf'])

        # Sort DataFrame by frequency in descending order
        freq_df.sort_values(by='tf', ascending=False, inplace=True)
        #Count the times that a term appears in the tweets
        
        term_documents = {term: set() for term in term_freq.keys()}
        for i, tweet in enumerate(files_tweets):
            for term in term_freq.keys():
                if term in tweet:
                    term_documents[term].add(i)

        freq_df['df'] = freq_df['term'].apply(lambda x: len(term_documents[x]))

        # Calculate inverse frequency for each term and add it as a new column
        freq_df['idf'] = freq_df['df'].apply(lambda x: log(N / x))

        # Calculate the product of frequency and inverse frequency
        freq_df['tf_idf'] = freq_df['tf'] * freq_df['idf']

        # Limit the DataFrame to the specified number of rows using the max_rows variable
        #limited_freq_df = freq_df.head(max_rows)

        # Write the DataFrame with inverse frequency to a CSV file
        #limited_freq_df.to_csv(os.path.join(output_folder, f'{top_user}.csv'), index=False)
        freq_df.to_csv(os.path.join(output_folder, f'{apellido}.csv'), index=False)

        limited_freq_df = freq_df.head(max_rows)
        limited_freq_df.to_csv(os.path.join(output_folder, f'{apellido}.csv'), index=False)

        # Convert the limited DataFrame to a list of [word, frequency] for JSON output
        json_data = limited_freq_df[['term', 'tf','df','idf','tf_idf']].values.tolist()
        json_file_path = os.path.join(json_output_folder, f'{apellido}.json')
        
        # Write the list to a JSON file
        with open(json_file_path, 'w') as outfile:
            json.dump(json_data, outfile)