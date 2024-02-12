import json
from MyUtils import create_folder
import os

def top10WordsMapReduce(input_folder, output_folder):
    create_folder(output_folder)
  
    for filename in os.listdir(input_folder):
        input_filepath = os.path.join(input_folder, filename)
        
        if os.path.isfile(input_filepath):
            print(f"Processing {filename}...")
            
            # Read JSON file which is expected to be a list of lists
            with open(input_filepath, 'r', encoding='utf-8') as file:
                word_count_list = json.load(file)

            # Sort the list of lists based on the count, which is the second element in the sub-lists
            word_count_list.sort(key=lambda x: x[1], reverse=True)

            # Get the top 10 words
            top10words = word_count_list[:10]
            
            # Prepare the output file path
            output_filename = f"{os.path.splitext(filename)[0]}_top10Words.json"
            output_filepath = os.path.join(output_folder, output_filename)

            # Save top 10 words to a new JSON file
            with open(output_filepath, 'w', encoding='utf-8') as outfile:
                json.dump(top10words, outfile, indent=4)

            print(f"Top 10 words saved to {output_filepath}")

# Example usage:
top10WordsMapReduce('Output/Manifest/lemmas', 'Output/Manifest/top10words')
