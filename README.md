Bluesky Social Analysis
This repository contains the source code for collecting, processing, and analyzing data from the Bluesky social network (based on the AT Protocol).

1. Project Structure
The code is organized into three main folders representing the logical phases of the workflow:

data_collection/: Scripts for downloading data.

cleaning&processing/: Scripts for cleaning raw data, building the interaction graph, and calculating sentiment.

experiments/: Scripts for statistical analysis, topic extraction (BERTopic), and generating final visualizations.

2. Installation and Requirements
Ensure Python 3.9+ is installed.

Install the necessary dependencies using the requirements.txt file:

3. Execution Pipeline
Follow the chronological order of these phases to reproduce the experiment.

Phase 1: Data Collection
Location: data_collection/

```
Choose one of the data collection modes:

	Real-time Stream: python listen.py (Captures live events).

	Historical Timelines: python crawl_timelines.py 0 (Downloads past posts of users).

It is also possible to use the real-time stream to identify active users and subsequently download their history.

Feeds & Likes:

	python get_top_feeds.py (Finds popular feeds).

	python crawl_feed_post_likes.py (Downloads likes for those feeds).

```

Phase 2: Cleaning & Network Construction
Location: cleaning&processing/

```
Transforms raw data into clean formats and builds the social graph.

Clean Data & Map Users: python clean_data.py (Cleans JSON files and automatically builds the DID-to-Integer mapping).

Extract Interactions: python interactions.py (Extracts replies, reposts, and mentions).

Build Graph Files: python make_interaction_graphs.py python create_gephi_files.py (Generates files for network analysis and Gephi visualization).

```

Phase 3: NLP & Sentiment Analysis
Location: cleaning&processing/

```
Performs linguistic analysis on the cleaned posts.

Prepare Text: python textdata.py (Extracts raw English text for analysis).

Calculate Sentiment: python sentiment.py (Calculates Positive/Negative/Neutral scores using RoBERTa model).

Merge Results: python add_sentiment.py (Adds sentiment scores back to the original JSON dataset).

```

Phase 4: Experiments & Visualization
Location: experiments/

```
Generates statistics, analyzes topics, and produces the final plots for the thesis.

Basic Statistics:

	python posts_stats.py (Temporal activity).

	python langs_dist.py (Language distribution).

	python instance_dist.py (Instance/Server distribution).

	python inter_event_time.py (User retention/fidelity).

Network Analysis:

	python graph_stats.py (Degree distribution, reciprocity, connected components).

Topic Modeling (NLP):

	python to_topics.py (Filters English posts).

	python topic_extraction.py (Runs BERTopic to identify themes).

	python clean_feed_text.py (Preprocesses feed texts).

Custom Feed Ecosystem Analysis:

	python make_feed_census_hybrid.py (Executes the Hybrid Census strategy to discover active & silent feeds).

	python analyze_statistics.py (Generates statistical plots: Zipf Law, Lorenz Curve, Creator Correlation, etc.).

Final Plotting:

	python bsky_plots.py (Generates all the PNG figures ).

```
