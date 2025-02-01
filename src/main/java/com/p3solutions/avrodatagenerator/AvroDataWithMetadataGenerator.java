package com.p3solutions.avrodatagenerator;

import com.p3solutions.avrodatagenerator.base.DataGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;



@SpringBootApplication
public class AvroDataWithMetadataGenerator {
    @Autowired
    private static DataGenerator dataGenerator;

    public static void main(String[] args) {
//        dataGenerator.initializeProcess();
    }

}




