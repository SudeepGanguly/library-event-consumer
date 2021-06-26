package com.learnkafka.config;

import com.learnkafka.services.LibraryEventsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {
    private final KafkaProperties properties;
    private final LibraryEventsService libraryEventsService;

    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory
                .getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(this.properties.buildConsumerProperties())));
        //Get access to containerProperties
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        //Confiuure multiple KafkaListeners
        //Since we have three partitions , we provided the value of 3
        factory.setConcurrency(3);

        //Adding the error handling function
        factory.setErrorHandler(((thrownException, consumerRecord) -> {
            log.info("Exception in ConsumerConfig is {} and the record is {}",thrownException.getMessage(),consumerRecord);
        }));

        //Set Retry
        factory.setRetryTemplate(retryTemplate());

        //Set Recovery Callback
        factory.setRecoveryCallback(context -> {
            //Checking if Exception is recoverable
             if(context.getLastThrowable().getCause() instanceof RecoverableDataAccessException){
                 //Implement Recovery
                 log.info("Inside the Recoverable Exception block");
                 //Dont need these
//                 Arrays.asList(context.attributeNames())
//                         .forEach(attributeName -> {
//                             log.info("Attribute name is : {} ",attributeName);
//                             log.info("Attribute is : {} ",context.getAttribute(attributeName));
//                         });
                ConsumerRecord<Integer,String> record =
                                (ConsumerRecord<Integer, String>) context.getAttribute("record");
                libraryEventsService.handleRecovery(record);
             } else {
                 //Throw exception back to Error Handler
                 log.info("Inside the Non Recoverable Exception block");
                 throw new RuntimeException(context.getLastThrowable().getMessage());
             }

            return null;
        });
        return factory;
    }

    private RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(simpleRetryPolicy());

        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000);
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        return retryTemplate;
    }

    private RetryPolicy simpleRetryPolicy() {
        Map<Class<? extends Throwable>,Boolean> exceptionsmap = new HashMap<>();
         exceptionsmap.put(IllegalArgumentException.class,false);
         exceptionsmap.put(RecoverableDataAccessException.class,true);
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3,exceptionsmap,true);
        //Implement the retry policy to retry three times
        simpleRetryPolicy.setMaxAttempts(3);

        return simpleRetryPolicy;
    }
}