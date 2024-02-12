from removeStopWords import noStopWordsCSV, noStopWordsMapReduce
from lemmatization import lemmatizationMapReduce, lemmatizationCSV
from MyUtils import create_folder
from top10Users import calcTop10Users
from top10WordsManifest import top10WordsMapReduce

# create_folder('Output')
# create_folder('Output/Manifest')
# create_folder('Output/Manifest/lemmas')
# create_folder('Output/Tweets')

# Remove stopwords 
noStopWordsCSV('Tweets_by_apellido', 'Output/Tweets/noStops')
noStopWordsMapReduce('MapReduce_Manifests', 'Output/Manifest/noStops')

# Lemmatize the words 
lemmatizationCSV('Output/Tweets/noStops', 'Output/Tweets/lemmas')
lemmatizationMapReduce('Output/Manifest/noStops', 'Output/Manifest/lemmas')

# Calculate the top 10 users
calcTop10Users()

# Calculate the top words of the mannifests
top10WordsMapReduce('Output/Manifest/lemmas', 'Output/Manifest/top10words')