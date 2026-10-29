# Last.fm Music Recommender (Hadoop MapReduce)

A content-based recommender that suggests artists to Last.fm users, built in Java on Hadoop MapReduce. Graduate course project for CIS 5570 at the University of Michigan–Dearborn (Fall 2018).

## What it does

Given each user's listening history and the tags users gave to artists, it recommends new artists whose tag profile is most similar to each user's taste, ranked by a similarity score.

Sample output (`output/recommendations/`):

    #1) Recommend Artist 2121 to User 10 >>> Recommendation Score: 0.907
    #2) Recommend Artist 969 to User 10 >>> Recommendation Score: 0.906

## How it works

The pipeline is a chain of MapReduce jobs:

1. **User–artist counts:** counts how many artists each user listens to.
2. **Item profiles:** builds a tag-weight vector for every artist from user tags (`user_taggedartists.dat`).
3. **Join:** joins each user's play counts (`user_artists.dat`) with the profiles of the artists they listen to.
4. **User profiles:** aggregates tag weights across a user's artists, weighted by the log of play count, then normalizes each profile to unit length.
5. **Inverted index:** indexes user and item profile vectors by tag, so only pairs that share a tag are compared.
6. **Cosine similarity:** scores each user against each candidate artist.
7. **Top-N ranking:** sorts each user's scores and writes the top recommendations.

Hadoop pieces written for this project:
- custom input formats and record readers
- custom Writables for profile vectors, join keys and index entries
- custom partitioners and grouping comparators for reduce-side joins and secondary sorting

## Data

The Last.fm dataset (HetRec 2011) in `input/`:
- `user_artists.dat`: user ID, artist ID, play count
- `user_taggedartists.dat`: user ID, artist ID, tag ID, date

## How to run

Requires Java and Maven. Hadoop runs in local mode, so no cluster is needed.

    cd cis-5570-recommender-system
    ./build.sh clean   # remove output from earlier runs
    ./build.sh build   # mvn clean compile assembly:single
    ./build.sh run     # runs the full job chain

Recommendations are written to `output/recommendations/`, one file per user.

## Tech

Java · Hadoop MapReduce · Maven
