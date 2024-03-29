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
                    if isinstance(item, list) and len(item) == 5:
                        key, _,_,_,_ = item
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
                    if isinstance(item, list) and len(item) == 5:
                        key, value,_,_,_ = item
                        if key in df.index:
                            df.loc[key, file_prefix] = value  # Use the file prefix as index instead of the full file name
    
    return df
# def create_dataset_from_folder(folder_path):
#     file_names = [file_name.split("_")[0] for file_name in os.listdir(folder_path) if file_name.endswith('.json')]
#     data = []  # List to store (term, value1) tuples

#     for file_name in os.listdir(folder_path):
#         if file_name.endswith('.json'):
#             with open(os.path.join(folder_path, file_name), 'r') as file:
#                 try:
#                     json_data = json.load(file)
#                     for item in json_data:
#                         if isinstance(item, list) and len(item) == 5:
#                             term, value1, _, _, _ = item
#                             data.append((term, value1))
#                 except json.JSONDecodeError:
#                     print(f"Error decoding JSON in file: {file_name}")

#     df = pd.DataFrame(data, columns=['Word', 'tf'])

#     return df

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
        #print(count)
        dataset.loc[word] *= count
    
    return dataset
def similarity_matrix(df):
    file_names = df.columns
    dataset = pd.DataFrame(index=file_names, columns=file_names)
    
    for file1 in file_names:
        for file2 in file_names:
            df1 = df[file1]
            #print(df1)
            df2 = df[file2]
            #print(df2)
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
def create_dataset_from_files(manifest_path, tweets_path):
    # Read data from the first JSON file
    with open(manifest_path, 'r') as file:
        data1 = json.load(file)
    
    # Read data from the second JSON file
    with open(tweets_path, 'r') as file:
        data2 = json.load(file)
    
    # Extract unique keys from both JSON files
    keys = set()
    for item in data1:
        keys.add(item[0])
    for item in data2:
        keys.add(item[0])
    
    # Convert keys to a list
    keys = list(keys)
    
    # Initialize DataFrame with 0 values
    df = pd.DataFrame(0, index=keys, columns=[manifest_path, tweets_path])
    
    # Update DataFrame with values from the first JSON file
    for key, value, in data1:
        df.loc[key, manifest_path] = value
    
    # Update DataFrame with values from the second JSON file
    for key, value,_,_,_ in data2:
        df.loc[key, tweets_path] = value
    
    return df
folder_path = 'Output/Tweets/Top10WordsJson'  # Relative path to the folder
tf = create_dataset_from_folder(folder_path)
#print(tf)
df = document_frequency(tf)
idf = inverse_document_frequency(tf)
idf_tf=inverse_document_term_frequency(tf)
similarity_M=similarity_matrix(idf_tf)
uservector=create_word_vector("sueno quito triunfo",tf,idf)

# print(df)
# print(idf)
# print(idf_tf)
# print(similarity_M)
#print(score_search(idf_tf,uservector))


#Similarity Manifest-Twitter

folder_path_manifest='Output/Simil/ManifiestTopQuito/yunda_top10Words.json'
folder_path_top_quito='Output/Simil/TopQuito/yunda.json'
sim_manif_tf=create_dataset_from_files(folder_path_manifest, folder_path_top_quito)
print(sim_manif_tf)
similarity_manifest_vector=similarity_matrix(inverse_document_term_frequency(sim_manif_tf))
print(similarity_manifest_vector)
