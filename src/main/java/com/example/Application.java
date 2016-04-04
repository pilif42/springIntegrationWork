package com.example;

import com.example.service.TempConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.scheduling.PollerMetadata;

import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
@IntegrationComponentScan
public class Application {
  public static void main(String... args) throws Exception {
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

  private final AtomicInteger coldDrinkCounter = new AtomicInteger();    // 10

  @Autowired
  private CafeAggregator cafeAggregator;                                 // 11

  @Bean(name = PollerMetadata.DEFAULT_POLLER)
  public PollerMetadata poller() {                                       // 12
    return Pollers.fixedDelay(1000).get();
  }

  @Bean
  @SuppressWarnings("unchecked")
  public IntegrationFlow orders() {                                      // 13
    return IntegrationFlows.from("orders.input")                         // 14
        .split("payload.items", (Consumer) null)                           // 15
        .channel(MessageChannels.executor(Executors.newCachedThreadPool()))// 16
        .route("payload.iced",                                             // 17
            new Consumer<RouterSpec<ExpressionEvaluatingRouter>>() {         // 18

              @Override
              public void accept(RouterSpec<ExpressionEvaluatingRouter> spec) {
                spec.channelMapping("true", "iced")
                    .channelMapping("false", "hot");                         // 19
              }

            })
        .get();                                                            // 20
  }

  @Bean
  public IntegrationFlow icedFlow() {                                    // 21
    return IntegrationFlows.from(MessageChannels.queue("iced", 10))      // 22
        .handle(new GenericHandler<OrderItem>() {                          // 23

          @Override
          public Object handle(OrderItem payload, Map<String, Object> headers) {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            System.out.println(Thread.currentThread().getName()
                + " prepared cold drink #" + coldDrinkCounter.incrementAndGet()
                + " for order #" + payload.getOrderNumber() + ": " + payload);
            return payload;                                                // 24
          }

        })
        .channel("output")                                                 // 25
        .get();
  }

  @Bean
  public IntegrationFlow hotFlow() {                                     // 26
    return IntegrationFlows.from(MessageChannels.queue("hot", 10))
        .handle(new GenericHandler<OrderItem>() {

          @Override
          public Object handle(OrderItem payload, Map<String, Object> headers) {
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);    // 27
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
  public IntegrationFlow resultFlow() {                                  // 28
    return IntegrationFlows.from("output")                               // 29
        .transform(new GenericTransformer<OrderItem, Drink>() {            // 30

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
