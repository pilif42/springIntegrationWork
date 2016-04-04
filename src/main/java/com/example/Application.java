package com.example;

import com.example.service.TempConverter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.IntegrationComponentScan;

@Configuration
@SpringBootApplication
@IntegrationComponentScan
public class Application {
  public static void main(String... args) throws Exception {
    ConfigurableApplicationContext ctx = SpringApplication.run(Application.class, args);
    TempConverter converter = ctx.getBean(TempConverter.class);
    System.out.println(converter.fahrenheitToCelcius(68.0f));
    ctx.close();
  }


}
