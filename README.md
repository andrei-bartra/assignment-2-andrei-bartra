# MACS 30123: Large Scale Computing
## Assignment 2
## Andrei Bartra
***

## 1 Parallel Web Scrapingwith `PyWren`
### 1.a Execution Time Comparisson and Bottlenecks

Code: A2_Q1_AB.py

First, I did some modifications to the original sequential solutions:

- The writing into memory feature is now independent. That way it is easy to change the database system implementation (`database`, `sqlite3`, `psycopg2`, etc). For this excercise I used pandas with csv files. Just to keep it simple. 
- The writting into memory can now be made in batches. That way the  process is more efficient. For this excercise I chose a batch size bigger than the data to make just one writting operation. The size of the file was not big enough to compromise the RAM capacity.

The PyWren implementation parallelizes the individual book scraping. It is 6.5 times faster on average with respect to the sequential solution (~15% of the time).

![pywren](pywren.png)

The optimmum batch size was 4 (searching on powers of 2 of batch size). There is a trade-off between speed-up due to parallelization and the time it takes to set-up a parallel instance. 

On the other hand, the process has bottlenecks that limits the efficiency of parallelization via `PyWren`. The size of the data allowed me to handle everything with RAM memory. However with a higher scale the would be problem and the parallel solution via PyWren would have to be done in batches. 

Another bottleneck is the initial collection of books id. The problem is that not knowing the number of pages beforehand makes it more difficult to call `PyWren` map function. 

### 1.b AWS storage solutions

In the current state it is easy to manage the storage locally. The amount of data not overcome the capacity of a regular RAM memory. If the scale of the data stars to make querying unmanagebale, or there is a need for a more complex transactions infraestructure it would be advisable to upgrade the database system. 

- **S3**: This is the first option to consider for a cloud based storage system in AWS. One major advantage is that it does not require structured data. This is convenient for web scraping tasks and exploratory analysis. The html file of each book can be stored as a whole and then processed on demand. Furthermore, having the unestructured data allows more flexibility. You can approach the data from different angles and exploit features of the data that could get loss during the systematizaztion. The main disadvantage of S3 is that it forces you to work on each object at a time. Making very inneficiento to extract a feature for every object. Furthermore, S3 offers eventual consistency. If the project relies on accesing data from multiple terminals and it is critical that procesess are replicable in real time, S3 may not be the best option.
- **DynamoDB**: If the project requires to prioritize consistency, quick access to groups of data and the html files can be systematized in low size JSON files (< 4KB), then DynamoDB is a good option.  The main disadvantages are that data items must be low size and traffic bursts can can be difficult to handle. Moreover, an additional system would be required if the project needs to perform aggregation, joins and sorts with values different from the key, recurrently.
- **Redshift**: Finally, if data can be structured as a relational database to perform SQL operations, Redshift is the best option. Redshift supports complex querying that could be suitable for data-science reated projects. The tradeoff with redshif is that it does not ensures consistency like Dynamodb and it is required a highly structured data with well designed DIST KEYS and SORT KEYS. 


## 2. Most-Used Words with `mrjob`

![pywren](mrjob.png)

## 3. Streaming Stock Data with `Kinesis`


## 4. Final Project Proposal

