import math
import json
import pandas as pd
import os
import shutil
import re

def calculate_similarity(vector1, vector2):
    dot_product = sum(v1 * v2 for v1, v2 in zip(vector1, vector2))
    magnitude1 = math.sqrt(sum(v**2 for v in vector1))
    magnitude2 = math.sqrt(sum(v**2 for v in vector2))
    similarity = dot_product / (magnitude1 * magnitude2)
    return similarity
def inverse_frequency(words_freq,number_doc):
    inverse_frequency_list = [(key, math.log(number_doc/value)) for key, value in words_freq]
    return inverse_frequency_list


def create_dataset_from_folder(folder_path):
    # Get list of file names without extension and only until the first "_"
    file_names = [file_name.split("_")[0] for file_name in os.listdir(folder_path) if file_name.endswith('.json')]
    
    # Get list of all unique keys from JSON files
    keys = set()
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.json'):
            with open(os.path.join(folder_path, file_name), 'r') as file:
                json_data = json.load(file)
                for item in json_data:
                    if isinstance(item, list) and len(item) == 2:
                        key, _ = item
                        keys.add(key)
    
    # Initialize DataFrame with 0 values
    df = pd.DataFrame(0, index=list(keys), columns=file_names)
    
    # Iterate over each JSON file
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.json'):
            file_prefix = file_name.split("_")[0]  # Extract the part before the first "_"
            with open(os.path.join(folder_path, file_name), 'r') as file:
                json_data = json.load(file)
                for item in json_data:
                    if isinstance(item, list) and len(item) == 2:
                        key, value = item
                        if key in df.index:
                            df.loc[key, file_prefix] = value  # Use the file prefix as index instead of the full file name
    
    return df

def document_frequency(dataset):
    words_count = []
    
    # Iterate over each word in the dataset
    for word in dataset.index:
        # Check how many files have a non-zero value for the word
        count = sum(dataset.loc[word] != 0)
        words_count.append((word, count))
    
    return words_count

def inverse_document_frequency(dataset):
    words_count = []
    # Iterate over each word in the dataset
    for word in dataset.index:
        # Check how many files have a non-zero value for the word
        count = sum(dataset.loc[word] != 0)
        words_count.append((word, math.log(len(dataset.columns)/count)))
    
    return words_count
def inverse_document_term_frequency(dataset):
    words_count = inverse_document_frequency(dataset)
    
    # Iterate over each word and its count
    for word, count in words_count:
        dataset.loc[word] *= count
    
    return dataset
def similarity_matrix(df):
    file_names = df.columns
    dataset = pd.DataFrame(index=file_names, columns=file_names)
    
    for file1 in file_names:
        for file2 in file_names:
            similarity = calculate_similarity(df[file1], df[file2])
            dataset.loc[file1, file2] = similarity
    
    return dataset

def create_word_vector(text, dataset,idf):
    # Tokenize the text into words
    words = re.findall(r'\b\w+\b', text.lower())
    
    # Count the frequency of each word
    word_freq = {}
    for word in words:
        if word in word_freq:
            word_freq[word] += 1
        else:
            word_freq[word] = 1
    
    # Initialize a word vector DataFrame with 0 values
    word_vector = pd.DataFrame(0, index=[0], columns=dataset.index)
    
    # Update the word vector with frequencies of words in the text
    for word, freq in word_freq.items():
        if word in dataset.index:
            word_vector.loc[0, word] = freq
    
    return word_vector.multiply([count for _, count in idf])
def score_search(df, uservector):
    file_names = df.columns
    score = pd.DataFrame(0, index=[0], columns=df.columns)
    
    # Extract values from uservector if it's a DataFrame
    if isinstance(uservector, pd.DataFrame):
        uservector = uservector.values.flatten()
    
    for file1 in file_names:
        similarity = calculate_similarity(df[file1], uservector)
        score.loc[0, file1] = similarity
    
    return score

folder_path = 'Output/Manifest/top10words'  # Relative path to the folder
tf = create_dataset_from_folder(folder_path)
print(tf)
df = document_frequency(tf)
idf = inverse_document_frequency(tf)
idf_tf=inverse_document_term_frequency(tf)
similarity_M=similarity_matrix(idf_tf)
uservector=create_word_vector("adulto",tf,idf)

print(df)
print(idf)
print(idf_tf)
print(similarity_M)
print(score_search(idf_tf,uservector))
