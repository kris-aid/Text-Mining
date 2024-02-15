import math
import json
import pandas as pd
import os
import shutil
import re
import  matplotlib.pyplot as plt
from MyUtils import delete_create_folder
def calculate_similarity(vector1, vector2):
    #print(vector1)
    #print(vector2)
    dot_product = sum(v1 * v2 for v1, v2 in zip(vector1, vector2))
    magnitude1 = math.sqrt(sum(v**2 for v in vector1))
    #print(magnitude1)
    magnitude2 = math.sqrt(sum(v**2 for v in vector2))
    #print(magnitude2)
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
    dataset.to_csv('Output/similarity_matrix.csv', index=True)
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

def create_dataset_from_files(file1_path, file2_path):
    # Read data from the first JSON file
    with open(file1_path, 'r') as file:
        data1 = json.load(file)
    
    # Read data from the second JSON file
    with open(file2_path, 'r') as file:
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
    df = pd.DataFrame(0, index=keys, columns=[file1_path, file2_path])
    
    # Update DataFrame with values from the first JSON file
    for key, value in data1:
        df.loc[key, file1_path] = value
    
    # Update DataFrame with values from the second JSON file
    for key, value in data2:
        df.loc[key, file2_path] = value
    
    return df
def create_word_vector_from_json(json_path, dataset,idf):
    # Read data from the JSON file
    with open(json_path, 'r') as file:
        data = json.load(file)
    
    # Initialize a dictionary to store word frequencies
    word_freq = {}
    
    # Extract word frequencies from the data
    for word, freq in data:
        word_freq[word] = freq
    
    # Initialize a word vector DataFrame with 0 values
    word_vector = pd.DataFrame(0, index=[0], columns=dataset.index)
    
    # Update the word vector with frequencies of words from the JSON data
    for word, freq in word_freq.items():
        if word in dataset.index:
            word_vector.loc[0, word] = freq
    
    return word_vector.multiply([count for _, count in idf])

def electoral_desviation(json_manifest_path, tf, idf, idf_tf):
    scores = score_search(idf_tf, create_word_vector_from_json(json_manifest_path, tf, idf))
    candidate = json_manifest_path.split("/")[-1].split(".")[0]
    
    # Clean each column name
    scores.columns = scores.columns.str.replace(".json", "")

    score = None  # Set a default value for the score variable

    if candidate in scores.columns:
        score = scores[candidate].values[0]
        print(score)

    if score is not None:
        return float(1 - score)
    else:
        # Handle the case where score is not defined
        print("Score not found for candidate:", candidate)
        return 0  # Or return an appropriate value or raise an exception

def desviation_candidates(folder_path,tf,idf,idf_tf):
    desviation={}
    for file in os.listdir(folder_path):
        if file.endswith('.txt'):
            result=electoral_desviation(folder_path+"/"+file,tf,idf,idf_tf)
            if result is not 0:
                desviation[file]=result
            else:
                print("No score for ",file)
    
    return desviation
def piechart(data):
    delete_create_folder('Output/Simil/Desviation')
    for key, value in data.items():
        data[key] = round(value, 2)
        labels = ["Dissimilar","Similar"] 
        values = [value, 1 - value]
        name=key=key.split(".")[0]
        plt.pie(values, labels=labels, autopct='%1.1f%%', startangle=140)
        plt.title('Pie Chart for Score of '+ name)
        plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        
        plt.savefig('Output/Simil/Desviation/desviation '+name+'.png')
        plt.show()
        
        
        
        
        
#folder_path = 'Output/Manifest/top10words'  # Relative path to the folder
folder_path = 'Output/Tweets/Top10WordsJson'  # Relative path to the folder
tf = create_dataset_from_folder(folder_path)
print(tf)
df = document_frequency(tf)
idf = inverse_document_frequency(tf)
idf_tf=inverse_document_term_frequency(tf)
similarity_M=similarity_matrix(idf_tf)


desviation=desviation_candidates('Output/Manifest/noStops',tf,idf,idf_tf)
# print(desviation)
piechart(desviation)
# print(similarity_M)
uservector=create_word_vector("quito triunfo empleo",tf,idf)
print(score_search(idf_tf, uservector))