#pip install spacy
#python -m spacy download es_core_news_sm
import spacy
import json
from MyUtils import create_folder, save_to_file
import os
import pandas as pd
# Initialize Spacy Spanish language model
nlp = spacy.load("es_core_news_sm")

# Function to process files for lemmatization
def lemmatizationMapReduce(input_folder, output_folder):
    create_folder(output_folder)

    # Loop through all files in the input folder
    for filename in os.listdir(input_folder):
        file_path = os.path.join(input_folder, filename)
        
        if os.path.isfile(file_path):
            print(f"Processing {filename}...")
            
            # Read the JSON-file structured result
            with open(file_path, 'r', encoding='utf-8') as file:
                word_count_pairs = json.load(file)
            
            # Create a dictionary to hold lemma counts
            lemma_counts = {}
            for word, count in word_count_pairs:
                # Process the word to get its lemma
                doc = nlp(word)
                lemma = doc[0].lemma_
                # Sum the counts for each lemma
                lemma_counts[lemma] = lemma_counts.get(lemma, 0) + count
            
            # Convert the dictionary back to the required list of lists format
            processed_lemmas = [[lemma, count] for lemma, count in lemma_counts.items()]
            
            # Save the processed result to file in the output folder
            save_to_file(processed_lemmas, filename, output_folder)

    print(f"All files in '{input_folder}' processed and outputs saved in '{output_folder}'.")

def lemmatizationCSV(input_folder, output_folder):
    create_folder(output_folder)
    
    # Loop through all the CSV files in the input folder
    for filename in os.listdir(input_folder):
        input_file_path = os.path.join(input_folder, filename)
        
        # Check if it's a file and ends with .csv
        if os.path.isfile(input_file_path) and filename.endswith('.csv'):
            print(f"Lemmatizing {filename}...")
            
            # Load CSV file
            df = pd.read_csv(input_file_path)
            
            # Convert 'tweet_text' to lowercase, and then lemmatize it
            df['tweet_text'] = df['tweet_text'].apply(
                lambda text:
                    ' '.join([token.lemma_ for token in nlp(text.lower())])
                    if pd.notnull(text) else text
            )
            
            # Save the lemmatized data to a CSV file in the output folder
            output_file_path = os.path.join(output_folder, filename)
            df.to_csv(output_file_path, index=False)
            
    print(f"Lemmatization complete for all files in '{input_folder}'. Results are in '{output_folder}'.")

# Usage
# lemmatizationCSV('Tweets_by_apellido_noStops', 'Tweets_by_apellido_lemmas')
# lemmatizationMapReduce('MapReduce_Manifests_noStops', 'MapReduce_Manifests_lemmas')
