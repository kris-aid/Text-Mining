from removeStopWords import noStopWordsCSV, noStopWordsMapReduce
from lemmatization import lemmatizationMapReduce, lemmatizationCSV

# Remove stopwords 
noStopWordsCSV('Tweets_by_apellido', 'Tweets_by_apellido_noStops')
noStopWordsMapReduce('MapReduce_Manifests', 'MapReduce_Manifests_noStops')

# Lemmatize the words 
lemmatizationCSV('Tweets_by_apellido_noStops', 'Tweets_by_apellido_lemmas')
lemmatizationMapReduce('MapReduce_Manifests_noStops', 'MapReduce_Manifests_lemmas')
