#  BigDataING5_RAHMOUNE_TEBIB

Durant les élections présidentielles américaines il y a eu une très grande quantité de Tweets et nombreux tweets provenaient de comptes ‘’bot’’, beaucoup de données autour des élections sont alors à récupérer via des API Twitter. Nous avons décidé pour ce projet Big Data d’utiliser les technologies Spark pour détecter les suspicions de bots et dans un second temps construire des indicateurs autour de ces tweets à visualiser dans un tableau de bord. Il s’agit d’indicateurs sur la provenance des tweets, l’ancienneté des comptes mais aussi de voir s’il y a des corrélations possible ente l’ancienneté du compte et les avis favorables à la fraude par exemple ; le nombre d’indicateurs évoluera en fonction de notre avancée dans le projet.

# Structure du projet

 - **Sujet principal :** Elections USA, fraudes aux élections
 - **Objectif :** Détecter les suspicions de bots
 - **Objectif annexe :** Indicateurs tweet par provenance, tweet C/C, ancienneté des comptes, corrélation entre avis positif à la fraude et
   ancienneté du compte

## Environnement 

 - WSL Ubuntu 20.04
   		 -  Application
   		 - Kerberos Client
   		 - Kafka client
 - Adaltas hadoop cluster
		 -  Kafka Server
		 -  Spark server
		 -  Kerberos server

## Architecture cluster

Nous avons opté pour Kafka et Spark streaming pour lire les tweets en temps réel. ![architecture du projet ](https://i.imgur.com/OjObqXu_d.webp?maxwidth=760&fidelity=grand)

## Twitter API Kafka Producer

On utilise le module tweepy, qui nous sert de connecteur à la twitter API. Puis Kafka producer pour écrire sur le topic donné par adaltas.

### Kerberos configuration

     - sasl_mechanism='GSSAPI'
     - security_protocol = 'SASL_PLAINTEXT'
     - bootstrap_servers=['kfk-brk-1.au.adaltas.cloud:6667','kfk-brk-2.au.adaltas.cloud:6667','kfk-brk-3.au.adaltas.cloud:6667']

### Twitter Keywords
    ELEC = "#USElection2020|#2020Election|#USA2020|USAElection2020|#USElection"
    BOT = "fraud|big fraud|ballot|LARGE WIN"
    CANDIDATES = "Biden|Trump|#Trump|#Biden"
## Spark Streaming

On utilise spark streaming pour lire sur topic préalablement rempli et réécrit le topic de sortie. Spark streaming nous servira à faire différents agrégats et process sur la data

### Kerberos configuration
*Pyspark kerberos configuration :*

    spark.readStream \
             .format("kafka") \
             .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
             .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
             .option("startingOffsets", "earliest") \
             .option("kafka.security.protocol","SASL_PLAINTEXT")\
             .option("kafka.sasl.mechanism","GSSAPI")\
             .option("kafka.sasl.kerberos.service.name","kafka")\
             .load()

*Spark submit :*

    > spark-submit --master local --driver-memory 4g --num-executors 2
    > --executor-memory 4g --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 tweet_processing.py

### Problèmes rencontrés
Lors du spark-submit nous avons rencontré un problème que nous n'avons pas su débloquer. Apparemment, nous devons indiquer le chemin vers le *"kafka_jaas.conf"* mais ce fichier se trouve uniquement sur l'edge donc innacessible demuis ma machine distante.
![enter image description here](https://i.imgur.com/HKojm5b_d.webp?maxwidth=1520&fidelity=grand)

## Dashboard
