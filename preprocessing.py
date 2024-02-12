from removeStopWords import noStopWordsCSV, noStopWordsMapReduce
from lemmatization import lemmatizationMapReduce, lemmatizationCSV
from MyUtils import create_folder

create_folder('Output')
create_folder('Output/Manifest')
create_folder('Output/Tweets')

# Remove stopwords 
noStopWordsCSV('Tweets_by_apellido', 'Output/Tweets/noStops')
noStopWordsMapReduce('MapReduce_Manifests', 'Output/Manifest/noStops')

# Lemmatize the words 
lemmatizationCSV('Output/Tweets/noStops', 'Output/Tweets/lemmas')
lemmatizationMapReduce('Output/Manifest/noStops', 'Output/Manifest/lemmas')
