package main.java;

import com.aerospike.client.AerospikeClient;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class ImdbWrite {
    private static final String NAMESPACE = "test";
    private static final String SET = "demo";
    private static final String SENTIMENT_BIN = "sentiment"; //based on CSV header
    private static final String REVIEW_BIN = "review"; //based on CSV header
    private static final String BASE_KEY = "movieKey"; //based on CSV header

    // Bin or column names for each primary key. These are the headers from the CSV file
    private String hostName;
    private int port;
    private AerospikeClient client;
    public ImdbWrite(String hostName, int port) {
        this.hostName = hostName;
        this.port = port;
        client = new AerospikeClient(this.hostName, this.port);
    }

    public void writeData(String fileName) {
        /**
         * Write data from the IMDB CSV file into Aerospike.
         * We create primary keys using a string and a counter concatenation
         */
        try {
            InputStream is = getClass().getResourceAsStream(fileName);
            System.out.println(Info.request(hostName, port));
            Reader reader = new InputStreamReader(is);
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withIgnoreHeaderCase()
                    .withTrim());
            int c = 0;
            for (CSVRecord rec: csvParser) {
                Key key = new Key(NAMESPACE, SET, BASE_KEY + String.valueOf(c));
                String sentiment = rec.get(SENTIMENT_BIN);
                String review = rec.get(REVIEW_BIN);
                Bin bin1 = new Bin(SENTIMENT_BIN, sentiment);
                Bin bin2 = new Bin(REVIEW_BIN, review);
                client.put(null, key, bin1, bin2);
                c++;
            }
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage : java ImdbWrite <hostname> <port>");
            System.exit(0);
        }
        ImdbWrite writer = new ImdbWrite(args[0], Integer.parseInt(args[1]));
        String fileName = "/imdb_dataset.csv";
        writer.writeData(fileName);

    }
}


