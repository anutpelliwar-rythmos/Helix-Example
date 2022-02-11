  package com.example.demo;
  
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
  
     @EnableAutoConfiguration(exclude = BatchAutoConfiguration.class)
     @ComponentScan
     @SpringBootApplication
     public class DemoApplication {
       
         public static void main(String[] args) {
         SpringApplication.run(DemoApplication.class, args);
         }
         
     }        
            