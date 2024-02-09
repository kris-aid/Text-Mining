# install libraries 
# pip install pandas nltk

#pandas for CSV file handling
#nltk for stopwords and tokenization
#os for file handling
#re for regular expressions
#json for reading the result from mapreduce

import re
import os
import json
import nltk
from MyUtils import create_folder, save_to_file
nltk.download('stopwords')
nltk.download('punkt')

import pandas as pd
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

########################
#Functions for CSV files
########################
# Load Spanish stopwords.
spanish_stopwords = stopwords.words('spanish')

# Function to remove stopwords from a text
def remove_stopwords(text):
    # Remove URLs
    text = re.sub(r'https?://\S+|www\.\S+', ' ', text) 
    # Remove special characters
    text = re.sub(r'[\W_]+', ' ', text)  # This regex replaces any non-alphanumeric character with a space
    # Tokenize the text
    words = word_tokenize(text)
    # Remove stopwords
    filtered_words = [word for word in words if word.lower() not in spanish_stopwords]
    # Reconstruct the text
    return ' '.join(filtered_words)

def noStopWordsCSV(input_folder, output_folder):
    create_folder(output_folder)
    
    # Loop through all files in the input folder
    for filename in os.listdir(input_folder):
        file_path = os.path.join(input_folder, filename)
        
        # Check if it's a file and has a .csv extension before processing
        if os.path.isfile(file_path) and filename.endswith('.csv'):
            print(f"Processing {filename}...")
            
            # Load CSV file
            df = pd.read_csv(file_path)
            
            # Apply the function to remove stopwords from the 'tweet_text' column.
            df['tweet_text'] = df['tweet_text'].apply(remove_stopwords)
            
            # Save the processed data back to a CSV 
            output_path = os.path.join(output_folder, filename)
            df.to_csv(output_path, index=False)

    print("All files processed.")

########################
#Functions for mapreduce
########################

    
# Function to clean a word (remove URLs, special characters excluding underscore)
def clean_word(word):
    # Remove URLs
    word = re.sub(r'https?://\S+|www\.\S+', '', word)
    # Remove special characters but allow underscore
    word = re.sub(r'[^\w\s]', '', word)
    return word

# Function to process each word/count pair in the JSON input
def process_word_count_pairs(pairs):
    processed_pairs = []
    for word, count in pairs:
        cleaned_word = clean_word(word)
        # Filter out stopwords and non-empty strings
        if cleaned_word and cleaned_word.lower() not in spanish_stopwords:
            processed_pairs.append([cleaned_word, count])
    return processed_pairs

def noStopWordsMapReduce(input_folder, output_folder):
    create_folder(output_folder)
    
    # Loop through all files in the input folder
    for filename in os.listdir(input_folder):
        file_path = os.path.join(input_folder, filename)
        
        if os.path.isfile(file_path):
            print(f"Processing {filename}...")
            
            # Read the JSON-file structured result
            with open(file_path, 'r', encoding='utf-8') as file:
                word_count_pairs = json.load(file)
            
            # Process word-count pairs, removing stop words and cleaning special chars
            processed_word_count_pairs = process_word_count_pairs(word_count_pairs)
            
            # Save the processed result to file
            save_to_file(processed_word_count_pairs, filename, output_folder)

    print("All files processed.")

# Usage
# noStopWordsCSV('Tweets_by_apellido', 'Tweets_by_apellido_noStops')
# noStopWordsMapReduce('MapReduce_Manifests', 'MapReduce_Manifests_noStops')


