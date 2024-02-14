import threading
from removeStopWords import noStopWordsCSV, noStopWordsMapReduce
from lemmatization import lemmatizationMapReduce, lemmatizationCSV
from top10Users import calcTop10Users
from top10WordsManifest import top10WordsMapReduce

def thread1_workflow():
    # Remove stopwords using MapReduce
    noStopWordsMapReduce('MapReduce_Manifests', 'Output/Manifest/noStops')
    # Lemmatization using MapReduce
    lemmatizationMapReduce('Output/Manifest/noStops', 'Output/Manifest/lemmas')
    # Get top 10 words using MapReduce
    top10WordsMapReduce('Output/Manifest/lemmas', 'Output/Manifest/top10Words')

def thread2_workflow():
    # Remove stopwords and output to CSV
    noStopWordsCSV('Tweets_by_apellido', 'Output/Tweets/noStops')
    # Lemmatization and output to CSV
    lemmatizationCSV('Output/Tweets/noStops', 'Output/Tweets/lemmas')
    # Calculate top 10 users
    calcTop10Users()

# Create threads
thread1 = threading.Thread(target=thread1_workflow)
thread2 = threading.Thread(target=thread2_workflow)

# Start threads
thread1.start()
thread2.start()

# Join threads to the main thread to wait for their completion
thread1.join()
thread2.join()
