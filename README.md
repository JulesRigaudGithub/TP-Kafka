# TP Architectures applicatives
## Architecture à base de message : Kafka

#### Grégoire Desjonquères et Jules Rigaud

**Documentation des scripts :**
1. `init-server.sh` sert à créer le serveur Kafka avec le storage
2. `create-streams.sh` sert à créer les quatres topic ainsi que les trois composants Kafka
3. `input.sh` sert à envoyer les lignes du livre sur le topic
4. `output.sh` [OPTIONNEL] permet de consommer et d'afficher les messages (topic `tagged-words-stream`)
5. `input.sh` envoie le texte _Les Trois Mousquetaires_ sur le topic des lignes
6. `command.sh` permet de récupérer le mot clef **END** pour afficher les résultats (qui apparaissent dans le shell n°2)
