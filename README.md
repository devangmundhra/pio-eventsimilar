# pio-eventsimilar
Similar Item recommendations using PredictionIO

This template uses two algorithms:
1. Picking random items
2. MLib ALS + Cosine similarity

The first algorithm is a "pseudo" algorithm that just takes random items from the list of given items. This is useful
when there is no user-item data at all. Once more and more data is available, the recommendations provided by
collaborative filtering (Algo #2) will also start influencing the results, at which point Algo #1 can be either removed
or its impact on recommendations reduced in case some element of randomness is still desired.

- python data/import_eventserver.py --access_key <>
- pio build
- pio train
- python data/send_query.py 
