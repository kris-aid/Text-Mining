#pip install pyarrow
import pandas as pd
import os
from matplotlib import pyplot as plt
from MyUtils import delete_create_folder

def make_timeline(max_terms=10):
    top_words_folder = 'Output/Tweets/Top'+str(max_terms)+'Words'
    lemmas_folder = 'Output/Tweets/lemmas'

    # Make sure the plotting folder exists
    plotting_folder = 'Output/Tweets/'+str(max_terms)+'TermTimelines'
    delete_create_folder(plotting_folder)

    for top_user_file in os.listdir(top_words_folder):
        if top_user_file.endswith('.csv'):
            # Read top 10 words for the user
            user_terms_df = pd.read_csv(os.path.join(top_words_folder, top_user_file))
            top_terms = user_terms_df['term'].tolist()

            # DataFrame to store the complete weekly timeline for all terms
            timeline_df = pd.DataFrame()

            for term in top_terms:
                term_timeline = {}

                # Collect tweets containing the term for each user and calculate their frequencies by week
                for lemmas_file in os.listdir(lemmas_folder):
                    if lemmas_file.endswith('.csv'):
                        df = pd.read_csv(os.path.join(lemmas_folder, lemmas_file))
                        user_term_tweets = df[(df['tweet_screen_name'] == top_user_file[:-4]) & 
                                            (df['tweet_text'].str.contains(term, na=False))]

                        # Assign a new 'tweet_week' column representing the week
                        user_term_tweets.loc[:, 'tweet_week'] = user_term_tweets['tweet_date'].apply(lambda x: pd.to_datetime(x).isocalendar()[1])

                        # Update the term_timeline dictionary with counts
                        term_counts = user_term_tweets['tweet_week'].value_counts().to_dict()
                        for week, count in term_counts.items():
                            term_timeline[week] = term_timeline.get(week, 0) + count

                # Convert the term_timeline into a DataFrame
                term_df = pd.DataFrame(list(term_timeline.items()), columns=['Week', term])
                term_df.sort_values(by='Week', inplace=True)

                # Merge into the overall timeline DataFrame
                if timeline_df.empty:
                    timeline_df = term_df
                else:
                    timeline_df = pd.merge(timeline_df, term_df, on='Week', how='outer')

            # Now, plot all terms for the user on the same graph, on a weekly basis
            plt.figure(figsize=(15, 8))
            for term in top_terms:
                if term in timeline_df.columns:
                    plt.plot(timeline_df['Week'], timeline_df[term].fillna(0), marker='o', label=term)

            plt.title(f"Weekly Frequency Timeline for {top_user_file[:-4]}")
            plt.xlabel('Week')
            plt.ylabel('Frequency')
            plt.xticks(rotation=90)
            plt.legend()
            plt.tight_layout()
            plt.savefig(os.path.join(plotting_folder, f"{top_user_file[:-4]}_timeline.png"))
            plt.close()
