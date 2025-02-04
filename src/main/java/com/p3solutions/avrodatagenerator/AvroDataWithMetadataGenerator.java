package com.p3solutions.avrodatagenerator;

import com.p3solutions.avrodatagenerator.base.DataGenerator;
import org.springframework.boot.autoconfigure.SpringBootApplication;



@SpringBootApplication
public class AvroDataWithMetadataGenerator {

    public static void main(String[] args) {
        DataGenerator generator = new DataGenerator("jdbc:sqlserver://192.168.1.17:1433/CLAIMS_SYS",
                "adsuser",
                "AdS@3421",
                "LOB",
                "/home/p3-ubuntu/Downloads/Avro/metadata.json",
                "/home/p3-ubuntu/Downloads/Avro/data.avro",
                1000);

        generator.execute();}

}




