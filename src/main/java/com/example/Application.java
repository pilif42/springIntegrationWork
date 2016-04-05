package com.example;

import com.example.domain.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.*;
import org.springframework.integration.dsl.AggregatorSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.RouterSpec;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.dsl.support.Consumer;
import org.springframework.integration.dsl.support.GenericHandler;
import org.springframework.integration.router.ExpressionEvaluatingRouter;
import org.springframework.integration.scheduling.PollerMetadata;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;
import org.springframework.integration.stream.CharacterStreamWritingMessageHandler;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.stereotype.Component;

@SpringBootApplication
@IntegrationComponentScan
public class Application {

  public static void main(String[] args) throws Exception {
    ConfigurableApplicationContext ctx = SpringApplication.run(Application.class, args);

    Cafe cafe = ctx.getBean(Cafe.class);
    for (int i = 1; i <= 100; i++) {
      Order order = new Order(i);
      order.addItem(DrinkType.LATTE, 2, false);
      order.addItem(DrinkType.MOCHA, 3, true);
      cafe.placeOrder(order);
    }

    System.out.println("Hit 'Enter' to terminate");
    System.in.read();
    ctx.close();
  }

  @MessagingGateway
  public interface Cafe {

    @Gateway(requestChannel = "orders.input")
    void placeOrder(Order order);

  }

  private final AtomicInteger hotDrinkCounter = new AtomicInteger();

  private final AtomicInteger coldDrinkCounter = new AtomicInteger();

  @Autowired
  private CafeAggregator cafeAggregator;

  @Bean(name = PollerMetadata.DEFAULT_POLLER)
  public PollerMetadata poller() {
    return Pollers.fixedDelay(1000).get();
  }

  @Bean
  @SuppressWarnings("unchecked")
  public IntegrationFlow orders() {
    return IntegrationFlows.from("orders.input")
        .split("payload.items", (Consumer) null)
        .channel(MessageChannels.executor(Executors.newCachedThreadPool()))
        .route("payload.iced",
            new Consumer<RouterSpec<ExpressionEvaluatingRouter>>() {

              @Override
              public void accept(RouterSpec<ExpressionEvaluatingRouter> spec) {
                spec.channelMapping("true", "iced")
                    .channelMapping("false", "hot");
              }

            })
        .get();
  }

  @Bean
  public IntegrationFlow icedFlow() {
    return IntegrationFlows.from(MessageChannels.queue("iced", 10))
        .handle(new GenericHandler<OrderItem>() {

          @Override
          public Object handle(OrderItem payload, Map<String, Object> headers) {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            System.out.println(Thread.currentThread().getName()
                + " prepared cold drink #" + coldDrinkCounter.incrementAndGet()
                + " for order #" + payload.getOrderNumber() + ": " + payload);
            return payload;
          }

        })
        .channel("output")
        .get();
  }

  @Bean
  public IntegrationFlow hotFlow() {
    return IntegrationFlows.from(MessageChannels.queue("hot", 10))
        .handle(new GenericHandler<OrderItem>() {

          @Override
          public Object handle(OrderItem payload, Map<String, Object> headers) {
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
            System.out.println(Thread.currentThread().getName()
                + " prepared hot drink #" + hotDrinkCounter.incrementAndGet()
                + " for order #" + payload.getOrderNumber() + ": " + payload);
            return payload;
          }

        })
        .channel("output")
        .get();
  }

  @Bean
  public IntegrationFlow resultFlow() {
    return IntegrationFlows.from("output")
        .transform(new GenericTransformer<OrderItem, Drink>() {

          @Override
          public Drink transform(OrderItem orderItem) {
            return new Drink(orderItem.getOrderNumber(),
                orderItem.getDrinkType(),
                orderItem.isIced(),
                orderItem.getShots());                                       // 31
          }

        })
        .aggregate(new Consumer<AggregatorSpec>() {                        // 32

          @Override
          public void accept(AggregatorSpec aggregatorSpec) {
            aggregatorSpec.processor(cafeAggregator, null);                // 33
          }

        }, null)
        .handle(CharacterStreamWritingMessageHandler.stdout())             // 34
        .get();
  }


  @Component
  public static class CafeAggregator {                                   // 35

    @Aggregator                                                          // 36
    public Delivery output(List<Drink> drinks) {
      return new Delivery(drinks);
    }

    @CorrelationStrategy                                                 // 37
    public Integer correlation(Drink drink) {
      return drink.getOrderNumber();
    }

  }

}