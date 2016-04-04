package com.example.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BeanDefiner {

//  @Bean
//  public IntegrationFlow convert() {
//    return f -> f
//        .transform(payload ->
//            "<FahrenheitToCelsius xmlns=\"http://www.w3schools.com/webservices/\">"
//                +     "<Fahrenheit>" + payload +"</Fahrenheit>"
//                + "</FahrenheitToCelsius>")
//        .enrichHeaders(h -> h
//            .header(WebServiceHeaders.SOAP_ACTION,
//                "http://www.w3schools.com/webservices/FahrenheitToCelsius"))
//        .handle(new SimpleWebServiceOutboundGateway(
//            "http://www.w3schools.com/webservices/tempconvert.asmx"))
//        .transform(Transformers.xpath("/*[local-name()=\"FahrenheitToCelsiusResponse\"]"
//            + "/*[local-name()=\"FahrenheitToCelsiusResult\"]"));
//  }
}
