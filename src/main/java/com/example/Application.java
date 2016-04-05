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

  /**
   * @MessagingGateway is the annotation to mark a business interface to indicate it is a gateway between the
   * end-application and integration layer.
   * It is an analogue of <gateway /> component from Spring Integration XML configuration.
   * Spring Integration creates a Proxy for this interface and populates it as a bean in the application context.
   * The purpose of this Proxy is to wrap parameters in a Message<?> object and send it to the MessageChannel according to the provided options.
   */
  @MessagingGateway
  public interface Cafe {
    /**
     * The method level annotation to distinct business logic by methods as well as by the target integration flows.
     * In this sample, we use a requestChannel reference of orders.input, which is a MessageChannel bean name of our
     * IntegrationFlow input channel.
     */
    @Gateway(requestChannel = "orders.input")
    /**
     * The interface method is a central point to interact from end-application with the integration layer. This method
     * has a void return type. It means that our integration flow is one-way and we just send messages to the
     * integration flow, but don’t wait for a reply.
     */
    void placeOrder(Order order);
  }

  private final AtomicInteger hotDrinkCounter = new AtomicInteger();

  private final AtomicInteger coldDrinkCounter = new AtomicInteger();

  /**
   * The POJO for the Aggregator logic.
   */
  @Autowired
  private CafeAggregator cafeAggregator;

  /**
   * The default poller bean. It is an analogue of <poller default="true"> component from Spring Integration XML
   * configuration.
   * Required for endpoints where the inputChannel is a PollableChannel. In this case, it is necessary for the two
   * Cafe queues - hot and iced.
   * Here we use the Pollers factory from the DSL project and use its method-chain fluent API to build the poller
   * metadata. Note that Pollers can be used directly from an IntegrationFlow definition, if a specific poller (rather
   * than the default poller) is needed for an endpoint.
   */
  @Bean(name = PollerMetadata.DEFAULT_POLLER)
  public PollerMetadata poller() {
    return Pollers.fixedDelay(1000).get();
  }

  /**
   * The IntegrationFlow bean definition. It is the central component of the Spring Integration Java DSL, although
   * it does not play any role at runtime, just during the bean registration phase. All other code below registers
   * Spring Integration components (MessageChannel, MessageHandler, EventDrivenConsumer, MessageProducer,
   * MessageSource etc.)  in the IntegrationFlow object, which is parsed by the IntegrationFlowBeanPostProcessor to
   * process those components and register them as beans in the application context as necessary (some elements,
   * such as channels may already exist).
   */
  @Bean
  @SuppressWarnings("unchecked")
  public IntegrationFlow orders() {
    /**
     * The IntegrationFlows is the main factory class to start the IntegrationFlow.
     * It provides a number of overloaded .from() methods to allow starting a flow
     * from a SourcePollingChannelAdapter for a MessageSource<?> implementations, e.g. JdbcPollingChannelAdapter;
     * from a MessageProducer, e.g. WebSocketInboundChannelAdapter;
     * or simply a MessageChannel.
     * All “.from()” options have several convenient variants to configure the appropriate component for the start
     * of the IntegrationFlow.
     * Here we use just a channel name, which is converted to a DirectChannel bean definition during the bean
     * definition phase while parsing the IntegrationFlow. In the Java 8 variant, we used here a Lambda definition -
     * and this MessageChannel has been implicitly created with the bean name based on the IntegrationFlow bean name.
     */
    return IntegrationFlows.from("orders.input")
        /**
         * Since our integration flow accepts messages through the orders.input channel, we are ready to consume
         * and process them. The first EIP-method in our scenario is .split(). We know that the message payload from
         * orders.input channel is an Order domain object, so we can simply use here a Spring (SpEL) Expression to
         * return Collection<OrderItem>. So, this performs the split EI pattern, and we send each collection entry as
         * a separate message to the next channel.
         * In the background, the .split() method registers a ExpressionEvaluatingSplitter MessageHandler
         * implementation and an EventDrivenConsumer for that MessageHandler, wiring in the orders.input channel as
         * the inputChannel.
         * The second argument for the .split() EIP-method is for an endpointConfigurer to customize options like
         * autoStartup, requiresReply, adviceChain etc. We use here null to show that we rely on the default options
         * for the endpoint. Many of EIP-methods provide overloaded versions with and without endpointConfigurer.
         * Currently .split(String expression) EIP-method without the endpointConfigurer argument is not available;
         * this will be addressed in a future release.
         */
        .split("payload.items", (Consumer) null)
        /**
         * The .channel() EIP-method allows the specification of concrete MessageChannels between endpoints, as it is
         * output-channel/input-channel attributes pair with Spring Integration XML configuration. By default,
         * endpoints in the DSL integration flow definition are wired with DirectChannels, which get bean names based
         * on the IntegrationFlow bean name and index in the flow chain. In this case we select a
         specific MessageChannel implementation from the Channels factory class; the selected
         channel here is an ExecutorChannel, to allow distribution of messages from the splitter to separate Threads,
         to process them in parallel in the downstream flow.
         */
        .channel(MessageChannels.executor(Executors.newCachedThreadPool()))
        /**
         * The next EIP-method in our scenario is .route(), to send hot/iced order items to different Cafe kitchens.
         * We again use here a SpEL expression to get the routingKey from the incoming message.
         * In the Java 8 variant, we used a method-reference Lambda expression, but for pre Java 8 style we must use SpEL or an inline interface implementation. Many anonymous classes in a flow can make the flow difficult to read so we prefer SpEL in most cases.
         */
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