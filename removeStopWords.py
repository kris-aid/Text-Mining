# install libraries 
# pip install pandas nltk

#pandas for CSV file handling
#nltk for stopwords and tokenization
#os for file handling
#re for regular expressions

import re
import os
import nltk
nltk.download('stopwords')
nltk.download('punkt')

import pandas as pd
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize



def create_folder(folder_path):
    # Check if the folder exists
    if not os.path.exists(folder_path):
        # Create the folder
        os.makedirs(folder_path)
        print(f"Folder created: {folder_path}")
    else:
        print(f"Folder already exists: {folder_path}")

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

# Usage

noStopWordsCSV('Tweets_by_apellido', 'Tweets_by_apellido_noStops')