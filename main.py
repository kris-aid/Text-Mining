import threading
from removeStopWords import noStopWordsCSV, noStopWordsMapReduce
from lemmatization import lemmatizationMapReduce, lemmatizationCSV
from topUsers import calcTopUsers
from topWordsManifest import topWordsMapReduce
from tweets_freq import tweets_frequency
from topTermsTimeline import make_timeline
def thread1_workflow(nostops=True, lemmatization=True, calcTopWords=10):
    if nostops:
        # Remove stopwords using MapReduce
        noStopWordsMapReduce('MapReduce_Manifests', 'Output/Manifest/noStops')
    if lemmatization:
        # Lemmatization using MapReduce
        lemmatizationMapReduce('Output/Manifest/noStops', 'Output/Manifest/lemmas')
    if calcTopWords > 0:
        # Get top words using MapReduce
        topWordsMapReduce('Output/Manifest/lemmas', 'Output/Manifest/top10Words', max_words=calcTopWords)

def thread2_workflow(nostops=True, lemmatization=True, TopUsers=10, tweetsFreq=True,max_terms=10, timeline = True):
    if nostops:
        # Remove stopwords and output to CSV
        noStopWordsCSV('Tweets_by_apellido', 'Output/Tweets/noStops')
    if lemmatization:
        # Lemmatization and output to CSV
        lemmatizationCSV('Output/Tweets/noStops', 'Output/Tweets/lemmas')
    if TopUsers > 0:
        # Calculate top users
        calcTopUsers(TopUsers)
    if tweetsFreq:
        # Calculate tweets frequency
        print("Calculating tweets frequency...")
        tweets_frequency(TopUsers,max_terms)
    if timeline:
        # Calculate timeline
        print("Calculating timeline...")
        make_timeline(max_terms)

def runTask(manifestThread=True, csvThread=True, nostops1=True, lemmatization1=True, calcTopWords=10,
            nostops2=True, lemmatization2=True, TopUsers=10, tweetsFreq=True, max_terms=10):
    threads = []
    
    if manifestThread:
        # create and start thread1
        t1 = threading.Thread(target=thread1_workflow, args=(nostops1, lemmatization1, calcTopWords))
        threads.append(t1)
        t1.start()
        
    if csvThread:
        # create and start thread2
        t2 = threading.Thread(target=thread2_workflow, args=(nostops2, lemmatization2, TopUsers,tweetsFreq,max_terms))
        threads.append(t2)
        t2.start()

    # Join all the threads that were started
    for thread in threads:
        thread.join()

runTask(manifestThread=False, nostops2=False, lemmatization2=False, TopUsers=15,max_terms=15)

