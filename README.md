# NycTaxi_BigDataAnalysis
A anaylsis on Big data taken from New York Taxis
HTRC dataset
What is the HTRC dataset?

Stockpile of books where we only get “features” about each book: words (and part-of-speech) appearing on the page in terms of counts, not the original text
Lots of books: 17mil, multi-lingual, location: /data/htrc, filenames are like: ls /data/htrc/uiuo/a+30107/uiuo.ark+=13960=t16n0ps7f.json.bz2
What is our goal?

Determine similarity between every book and every other book
What are our assumptions?

Books that use the same words are more similar than books that do not use the same words
How do we do it?

Compute distance between each book according to the words used
When two books use mostly the same words, they will be closer in distance
Given a “query” book (either one we know about in the dataset or a new one), we can find the most similar books in the dataset
Why is this challenging?

Large dataset, so must make aggressive use of indexes and sampling of data to get insights
Procedure

Take a random subset (small amount, 1000 books) and determine the most common words from that subset (say, 100k most frequent).
These words (sorted alphabetically, just to maintain an order) will become the vector columns/dimensions.
Unknowns: which words are ‘good’ words to keep? Punctuation? Stop words (the/and/or/not/etc.)? Non-English words? Unicode/emoji characters? Numbers? Use stemming/lemmatization (drop plurals, -ing, etc.)?
Save this word ordering/list to a file for later querying.
Train a PCA model on the random subset vectors with n_components = 256; save this model
Initialize faiss index (blank).
What kind of faiss index?
IndexIVFPQ with 256-dim vectors, with 4096 clusters and 16 bit quantization
Now, for each book (in random order to get a good estimate of processing time per book):
Save book filename / id pair to a file
Open the book’s data file, get the tokens (words) from the file, and make a vector for this book that holds the ratio of each word’s frequency vs. total number of words in the book, transform the vector using the learned PCA model
Save the vector into faiss
Save faiss index to disk every 1000 iterations
Querying:

Receive a book’s filename as input, open the file, get the tokens
Make the vector using the pre-saved most common word list and ordering; transform with PCA
Query faiss with this vector, ask for K most similar matches
Faiss will typically give you the row id of the matching books
Look up the row id in a file you saved ahead of time of book filename/id pair
Show the resulting closest matching books/filenames
