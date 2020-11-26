# Yelp dataset - fake users and reviews analysis with Spark

<img src="..\pics\spark\yelp2.jpg" width="200">

## 1. Introduction

### 1.1 Yelp dataset
The _Yelp dataset challenge_ is a competition that sees the famous American company release internal datasets for analysis and researches purposes. 

The datasets published are in JSON format and include:

- business.json containing data of 174K bussinesses in 11 metropolitan areas. Each business is characterized by features such as: id, name, neighborhood, city, state, longitude, latitude, rating on Yelp, number of reviews and categories.

- review.json containing data for 5.2M reviews to the businesses included in the business.json file. The reviews are characterized by: id, user, business, rating, date, text, scores.

- user.json containing data of the users that wrote reviews. Features are: id, name, number of reviews, date of subscription, id of friends on Yelp, average review score, votes sent and greetings received.

### 1.2 Stack
For the analysis it was used a virtual machine Ubuntu with processor A8 v2, 8 core, 16GB RAM and in a cloud environment Azure. Access was provided with SSH connetion through Putty.

<img src="..\pics\spark\stack.png" width="500">

#### Neo4j graph database
The database chosen for storing the datasets is Neo4j, a graph database (so NoSQL); its main features are:
- Schemaless, giving the possibility of defining data etherogeneous;
- ACID;
- Not supporting Sharding but supporting replication;
- Support for different programming languages;
- Providing a shell for querying with _Cypher_ language;
- Storing and processing based on graphs.

With Neo4j was created a graph with different types of nodes for Business, Review, User and Category and their relationships.

<img src="..\pics\spark\grafo.png" width="300">

Other important element is that, with the protocol Bolt, it is pretty straightforward to connect Spark and Neo4j, so that all Spark's components and libraries are available for the analysis.

#### Neo4j settings
To define the settings for NEo4j, installed on the Microsoft Azure VM, it is needed to modify the _.conf_ file with the following directives:

1. Enable the import of external files, such as the datasets JSON, into the database and enable the import with the apoc plugin to upload the data
```bash
dbms.directories.import = import
apoc.import.file.enabled = true
```

2. Modify security parameters to enable the acces to internal API for the apoc plugin
```bash
dbms.security.procedures.unrestricted = apoc .*
dbms.security.auth_enabled = false
```

3. Dedicate higher Java heap size to have more resources for the graph processing
```bash
dbms.memory.heap.initial_size =8G
dbms.memory.heap.max_size =12G
```

#### Connection Neo4j - Spark
Connection was enabled with the _neo4j-spark-connector_ based on the Bolt protocol, for client-server communication in database applications.
To use the connector, a parameter referring to the connector package must be added when starting the spark-shell:
```bash
SPARK_LOCAL_IP =127.0.0.1 .\ spark - shell -- packages
neo4j - contrib :neo4j -spark - connector :2.1.0 - M4
```

#### GraphFrame
To analyze data in Spark, the library used is GraphFrame, for graph processing.
A GraphFrame is characterized by a set of verices and a set of edges, defined with Cypher language and represented with Dataframe.
Once a GraphFrame has been created, it is possible to process it in many dufferent ways: it supports all the APIs from GraphX, 3 different languages (Java, Python and Scala). The programming language chosen was Scala.


## 2. Analysis

### 2.1 Goal
The goal is to find in the datasets the fake reviews and the users that could be defined as "spammers". The two are strictly related, as a review can be considered fake after checking the users who wrote it, while a user can be defined as spammer after analyzing his reviews.

Approaches for the analysis are:
- Analyzing the linguistic features: focus on the words used by the users in the reviews and their distribution;
- Analyzing the behavioral features: focus on generic behavior characteristics.

 ### 2.2 Statistical analysis
Starting point for the analysis is the research entitled _What Yelp fake review filter might be doing" from the University of Illinois in collaboration with Google. The research, based on a labeled Yelp dataset (meaning that the reviews were already known and flagged as fake or true), makes statistical analysis on data to then use it for machine learning; the result of the analysis shows that the linguistic feature apporach to find fake reviews is less effective as spammers use a language similar to real reviewers. <br>
On the opposite, the analysis on behavioral features is more effective to discover patterns in the spammers.

For example, analyzing the CDF of users (in blue real users and in red spammers):

- *Number of daily reviews*: users writing multiple reviews per day are suspect, as data shows that real users write maximum 3 reviews per day in 90% of cases, while spammers in 75% of cases are writing more than 6 reviews per day.<br>
<img src="..\pics\spark\cdf1.png" width="200"><br>

- *Percentage of positive reviews*: spammers have a percentage of good reviews much higher than real users; 85% of them have more than 80% of 4-5 stars reviews.<br>
<img src="..\pics\spark\cdf2.png" width="200"><br>

- *Lenght of reviews*: even if from a linguistic point of view reviews are close, typically fake reviews contain less words than real ones. From data we observe that 80% of spammers is not going over 135 words, while real users in 92% go over 200 words. <br>
<img src="..\pics\spark\cdf3.png" width="200"><br>

- *Deviation from average rating*: fake reviews usually have as goal to improve the rating of a business; this means that often they have high deviation value from average ratings. We can observe that on average real users deviation is 0.6 for 70% of cases, while for spammers only the 20% is staying under an average deviation of 2.5.
<img src="..\pics\spark\cdf4.png" width="200"><br>


## 3. Spark implementation
## 3.1 Upload of data

To upload the data in the graph database Neo4j, queries in Cypher language have been run in the shell. The queries rely on the APOC library, that defines Java function to support data integration in Neo4j.

```SQL
-- Upload business
CALL apoc.periodic.iterate("
CALL apoc.load.json('file:\\\home\vmadmin\dataset_yelp\business.json') 
YIELD value RETURN value
","
MERGE (b:Business{id:value.business_id})
SET b += apoc.map.clean(value, ['attributes','hours','business_id','categories','address','postal_code'],[])
WITH b,value.categories as categories
UNWIND categories as category
MERGE (c:Category{id:category})
MERGE (b)-[:IN_CATEGORY]->(c)
",{batchSize: 10000, iterateList: true});

-- Upload review
CALL apoc.periodic.iterate("
CALL apoc.load.json('file:\\\home\vmadmin\dataset_yelp\review.json')
YIELD value RETURN value
","
MERGE (b:Business{id:value.business_id})
MERGE (u:User{id:value.user_id})
MERGE (r:Review{id:value.review_id})
MERGE (u)-[:WROTE]->(r)
MERGE (r)-[:REVIEWS]->(b)
SET r += apoc.map.clean(value, ['business_id','user_id','review_id','text'],[0])
",{batchSize: 10000, iterateList: true});

-- Upload user
CALL apoc.periodic.iterate("
CALL apoc.load.json('file:\\\home\vmadmin\dataset_yelp\user.json')
YIELD value RETURN value
","
MERGE (u:User{id:value.user_id})
SET u += apoc.map.clean(value, ['friends','user_id'],[0])
WITH u,value.friends as friends
UNWIND friends as friend
MATCH (u1:User{id:friend})
MERGE (u)-[:FRIEND]-(u1)
",{batchSize: 100, iterateList: true});
```

The function `apoc.periodic.iterate` processes a number of _batchSize_ JSON records per iteration. With the _SET_ is possible to explicitally choose which attributes to take when creating a node.

<img src="..\pics\spark\datasetcompleto.png" width="400"><br>

## 3.2 Data manipulation
To reduce the dimension of dataset and focusing only on data of interest, some filtering has been applied on the data. In addition, some of the features have been transformed to better reflect some aspects the analysis is focusing on.

### Business filtering
Filter is based on the feature _is_open_ that flags if a business is open or not.

<img src="..\pics\spark\businesschiusi.png" width="500"><br>
<img src="..\pics\spark\businessfilter.png" width="400"><br>

### User filtering and aggregating
Two different filters have been applied for Users:

1. Users with Elite status for at least an year have been filtered out.

<img src="..\pics\spark\elite.png" width="400"><br>
<img src="..\pics\spark\elitedelete.png" width="500"><br>

2. Users with many reviews and without any review.

<img src="..\pics\spark\user0delete.png" width="600"><br>
<img src="..\pics\spark\user50delete.png" width="700"><br>

Also, for the Users some features have been aggregated: attribytes such as useful, funny, cool, greetings and more, have been aggregated into _sum_of_reactions_ and _sum_of_compliments_.

### Text Review feature manipulation
Given that the apporach chosen for the analysis is based on behavioral features, the content of the attribute _text_ for the reviews have been transformed: the interest is not in the actual words but on the length of the review. A new feature _text_length_ has been added to replace text.<br>

<img src="..\pics\spark\length.png" width="500"><br>


## 3.3 Spark analysis
### Analysis on subset
First analysis involved a subset of the dataset: goal is to find spammers and their reviews for hotels and restaurants businesses.
The features used for this purpose are: _text_length_ of the review, _yelping_since_ of the user writing the review (date since the user is active on Yelp), _date_ of the review and _review_count_ of the user.<br>

For this, we import into a GraphFrame data from the subset of interest.
We compute a new attributes _differenza_data_ (date difference) to keep the difference between subscription on Yelp and date of the review.
Finally, we filter the data selecting the attributes of interest and the records matching the clauses specified in the filter.

```scala
import org.neo4j.spark._
val neo = Neo4j(sc)

val nodesQuery = "MATCH (u:User)-[r3:WROTE]->
                (w:Review)-[r2:REVIEWS]->(n:Business) 
                RETURN w.id as id, u.id as id_user, u.name as nome_user, 
                u.review_count as num_recensioni_user, 
                u.yelping_since as data_iscrizione, 
                w.stars as stelle_review, w.date as data_review,
                w.text_length as lunghezza_testo, n.id as id_business, 
                n.name as nome_business, n.city as citta, 
                n.state as stato, n.latitude as latitudine,
                n.longitude as longitudine, n.stars as rating_business, 
                n.review_count as numero_review_business"

val relsQuery = "MATCH (u:User)-[r3:WROTE]-> (w:Review)-[r2:REVIEWS]->(n:Business) 
                 RETURN id(u) as src, id(n) as dst"

val graphFrame = Neo4j(sc).nodes(nodesQuery, Map.empty).rels(relsQuery, Map.empty).loadGraphFrame

val gf2 = graphFrame.vertices.withColumn("differenza_date", datediff(col("data_review"), col("data_iscrizione")))

gf2.select("nome_business", "numero_review_business",
    "rating_business", "id_user", "num_recensioniuser",
    "numeromedio_stelle, "lunghezza_testo", "differenza_date")
    .filter("num_recensioni_user = 1 AND lunghezza_testo < 15 
    AND differena_date < 3").orderBy("nome_business").show
```

The clauses specified are:

- text_length < 15 as average length i the subset is 115, while 90% of reviews have a value of over 27 words. This means that reviews with less than 15 words can be considered highly suspicious;

- date_difference < 3 AND review_user = 1 to focus on users with few reviews made in short interval since the subscription.


```scala
val nodesQuery = "MATCH (u:User)-[r3:WROTE]->
  (w:Review)-[r2:REVIEWS]->(n:Business) 
  WHERE u.review_count > 30 AND u.average_stars > 4.8 
  AND u.yelping_since > '2017-01-01'
  return distinct(u.id) as id, u.name as nome, 
  u.average_stars as numero_medio_stelle, 
  u.yelping_since as data_iscr "
val relsQuery = "MATCH (u:User)-[r3:WROTE]->
  (w:Review)-[r2:REVIEWS]->(n:Business) 
  RETURN id(u) as src, id(n) as dst"
val graphFrame = Neo4j(sc).nodes(nodesQuery, 
  Map.empty).rels(relsQuery, Map.empty).loadGraphFrame
```

<img src="..\pics\spark\diffdata.png" width="800"><br>

<img src="..\pics\spark\risultato_spark_1.png" width="800"><br>

Last step is to analyze the bussinesses whose the reviews are referring to; for example, two of the businesses belong to the same city Montreal. With a custom function the distance in kilometers between the two businesses was computed, showing us a distance of 9 kilometers and the possibility of a "rivalry". 

<img src="..\pics\spark\fdist.png" width="600"><br>

<img src="..\pics\spark\distanza.png" width="800"><br>


### First analysis on full dataset
The analysis set as goal to find fake reviews considering the values related to the users features:
- review_count
- average_stars
- yelping_since

Purpose is to focus on users with a significant number of reviews (> 30, based on previous indexes) and with average stars value extremely high, and that were fresh members on Yelp.


```scala
val nodesQuery = "MATCH (u:User)-[r3:WROTE]->
  (w:Review)-[r2:REVIEWS]->(n:Business) 
  WHERE u.review_count > 30 AND u.average_stars > 4.8 
  AND u.yelping_since > '2017-01-01'
  return distinct(u.id) as id, u.name as nome, 
  u.average_stars as numero_medio_stelle, 
  u.yelping_since as data_iscr "

val relsQuery = "MATCH (u:User)-[r3:WROTE]->
  (w:Review)-[r2:REVIEWS]->(n:Business) 
  RETURN id(u) as src, id(n) as dst"

val graphFrame = Neo4j(sc).nodes(nodesQuery, 
  Map.empty).rels(relsQuery, Map.empty).loadGraphFrame
```

We obtain a GraphFrame with only 7 users.

<img src="..\pics\spark\Analisi_2\Prima_Analisi.PNG" width="500"><br>

We focus the analysis on the ones with newer subscription date (highlighted in yellow). 

```scala
val nodesQuery = "MATCH (u:User)-[r3:WROTE]->
  (w:Review)-[r2:REVIEWS]->(n:Business) 
  WHERE u.review_count > 30 AND 
  u.average_stars > 4.8 AND u.yelping_since > '2017-01-01'
  return distinct(u.id) as id, u.name as nome, 
  w.id as id_review, w.text_length as lunghezza_testo, 
  w.date as data_review, n.name as nome_business, 
  n.id as id_business" 

val relsQuery = "MATCH (u:User)-[r3:WROTE]->
  (w:Review)-[r2:REVIEWS]->(n:Business) 
  RETURN id(u) as src, id(n) as dst"

val graphFrame = Neo4j(sc).nodes(nodesQuery, Map.empty)
  .rels(relsQuery, Map.empty).loadGraphFrame
```

We can notice that some users have more reviews in the same day (Peter and Chris). 

<img src="..\pics\spark\Analisi_2\Approfondimento_1.PNG" width="800"><br>

We compute the reviews per day value for all of them, obtaining the following GraphFrame.

```Scala
val count = graphFrame.vertices.orderBy("nome").
  groupBy($"nome", $"data_review").count
```

<img src="..\pics\spark\Analisi_2\count_Recensioni_giornaliere.PNG" width="600"><br>

Filtering on > 6, we obtain three users that have high number of reviews for a single day, with average vallue over 4.6 and more than 30 reviews.

<img src="..\pics\spark\Analisi_2\Probabili_spammer.PNG" width="400"><br>

We then move to the bussinesses these users reviewed: by joining the GraphFrame for the spammers and the reviews.

```Scala
val df1 = graphFrame.vertices.select("id",
   "nome", "id_review","lunghezza_testo", 
   "data_review", "nome_business", "id_business")
val joined = df1.join(spammers, 
  Seq("nome", "data_review"), "inner")
df1.show
```

<img src="..\pics\spark\Analisi_2\Considero_solo_spammer_campi_associati.PNG" width="700"><br>

We then create a new GraphFrame with the information on the bussiness.

```Scala
val nodesQuery = "MATCH (b:Business)-[r:IN_CATEGORY]->
  (c:Category)  return id(b) as id, b.id as id_business,  
  b.name as nome_business, b.review_count as numero_stelle, 
  b.city as citta, b.latitude as latitudine, 
  b.longitude as longitudine, b.stars as rating "
val relsQuery = "MATCH (b:Business)-[r:IN_CATEGORY]->
  (c:Category) RETURN id(b) as src, id(c) as dst"
val graphFrame = Neo4j(sc).nodes(nodesQuery, Map.empty)
  .rels(relsQuery, Map.empty).loadGraphFrame

val business_considered = business_consider.
  select("id_business","nome_business", "numero_stelle",
  "citta", "latitudine", "longitudine", "rating" ).distinct

business_considered.show
```

<img src="..\pics\spark\Analisi_2\Analisi_business.PNG" width="700"><br>

A partial feedback about the spammers analysis can be found on some of the reviews of the users that were flagged on Yelp.

<img src="..\pics\spark\Analisi_2\Peter.PNG" width="500"><br>
<img src="..\pics\spark\Analisi_2\chris.PNG" width="500"><br>


### Seconda analisi sull'intero dataset
Goal of the analysis is to find users with similar behavior to spammers found in the previous section, but with negative reviews.
Starting point is again a GraphFrame with users having average_stars value low.

```Scala
val nodesQuery = "MATCH (u:User)-[r3:WROTE]->
  (w:Review) WHERE u.review_count > 30 AND 
  u.average_stars < 1.2 return distinct(u.id) as id,
  u.name as nome, u.average_stars as numero_medio_stelle,
  u.yelping_since as data_iscr "
val relsQuery = "MATCH (u:User)-[r3:WROTE]->
  (w:Review) RETURN id(u) as src, id(w) as dst"
val graphFrame = Neo4j(sc).nodes(nodesQuery, Map.empty)
  .rels(relsQuery, Map.empty).loadGraphFrame
```

We can see that there are 13 users with more than 30 reviews and a rating average close to 1.

<img src="..\pics\spark\Analisi_3\1.PNG" width="800"><br>
	
We decided to get from the database data of the reviews of users from this GraphFrame.

```Scala
val nodesQuery = "MATCH (u:User)-[r3:WROTE]->
  (w:Review)-[r2:REVIEWS]->(n:Business) 
  WHERE u.review_count > 30 AND u.average_stars < 1.2 
  return distinct(u.id) as id, u.name as nome,
  w.id as id_review, w.text_length as lunghezza_testo,
  w.date as data_review, w.stars as stelle_recensione, 
  n.name as nome_business, n.id as id_business" 
val relsQuery = "MATCH (u:User)-[r3:WROTE]->
  (w:Review)-[r2:REVIEWS]->(n:Business) 
  RETURN id(u) as src, id(n) as dst"
val graphFrame = Neo4j(sc).nodes(nodesQuery, Map.empty)
  .rels(relsQuery, Map.empty).loadGraphFrame
```

In the pictures we can observe the creation of the GraphFrame and 


<img src="..\pics\spark\Analisi_3\2.PNG" width="800"><br>


<img src="..\pics\spark\Analisi_3\3.PNG" width="800"><br>
	
We again see as some users were writing multiple reviews in single days. 

```Scala
val count = graphFrame.vertices.orderBy("nome").groupBy($"nome", $"data_review").count
```

<img src="..\pics\spark\Analisi_3\4.PNG" width="800"><br>

From the output we can observe that some users were writing multiple negative reviews in a single day; for example, the user Sean wrote 5 negative reviews in a single day.

```Scala
val spammers = count.filter("count > 4")
```

<img src="..\pics\spark\Analisi_3\5.PNG" width="800"><br>

To understand which business were reviewd, we create a new DataFrame and join it to the ones holding data about the possible spammers.

```Scala
val df1 = graphFrame.vertices.select("id", "nome", 
  "id_review","lunghezza_testo", "data_review",
  "stelle_recensione", "nome_business", "id_business")
val joined = df1.join(spammers, Seq("nome",
  "data_review"), "inner")
val business_saved = joined
  .select("id_business")
```

<img src="..\pics\spark\Analisi_3\6.PNG" width="800"><br>

Last step of the analysis is creating a GraphFrame with data of the businesses found at the previous step.

```Scala
val nodesQuery = "MATCH (b:Business)-[r:IN_CATEGORY]
  ->(c:Category)  return id(b) as id,b.id as id_business,
  b.name as nome_business, b.review_count as numero_recensioni,
  b.city as citta, b.latitude as latitudine, 
  b.longitude as longitudine, b.stars as rating"
val relsQuery = "MATCH (b:Business)-[r:IN_CATEGORY]->
  (c:Category) RETURN id(b) as src, id(c) as dst"
val graphFrame = Neo4j(sc).nodes(nodesQuery, Map.empty)
  .rels(relsQuery, Map.empty).loadGraphFrame

val df_b = graphFrame.vertices.select("nome_business",
  "id_business", "numero_recensioni", "citta", 
  "latitudine", "longitudine", "rating")
val business_consider = df_b.join(business_saved, 
  Seq("id_business"), "inner")
val business_considered = business_consider
  .select("id_business","nome_business", 
  "numero_recensioni", "citta", "latitudine", "longitudine",
  "rating", "stelle_recensione" ).distinct
```

<img src="..\pics\spark\Analisi_3\7.PNG" width="800"><br>

From the picture we can notice that 4 out of 5 reviews made by the user belong to different offices of the same company located nearby to each other.
We compute the distance in kilometers between them using the geo coordinates, as done in the previous section.

<img src="..\pics\spark\Analisi_3\8.PNG" width="800"><br>

Similarly to the first full dataset analysis, a feedback was found on Yelp: the reviews by the user Sean were reported on the website.

<img src="..\pics\spark\Analisi_3\9.PNG" width="800"><br>
