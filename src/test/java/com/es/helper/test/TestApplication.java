package com.es.helper.test;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;


@SpringBootApplication(scanBasePackages = "com.es.helper")
public class TestApplication {
    public static void main(String[] args) {

        new SpringApplicationBuilder(WebApplicationType.class)
                .web(WebApplicationType.NONE)
                .run(args);
    }
}
