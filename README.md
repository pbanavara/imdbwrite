### This is a Java client for loading the IMDB movie review dataset into Aerospike

#### To run this client, from the parent directory
`mvn package`

`java -cp target/aerospike-1.0-jar-with-dependencies.jar main.java.ImdbWrite 0.0.0.0 3000`

#### Some data information
* The primary key for each row is BASE_KEY + a counter so BASE_KEY1 for instance
* The data is read from the csv file included in the resources directory
* To verify the data upload
    * From aql -> `select * from test.demo`
