#pip install spacy
#python -m spacy download es_core_news_sm
import spacy
import json
from MyUtils import create_folder, save_to_file
import os
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

# Usage
# lemmatizationMapReduce('MapReduce_Manifests_noStops', 'MapReduce_Manifests_lemmas')
