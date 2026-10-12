package org.bp.travel;

import org.apache.camel.saga.CamelSagaService;
import org.apache.camel.saga.InMemorySagaService;
import org.apache.camel.CamelContext;
import org.apache.camel.spring.boot.CamelContextConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SagaConfiguration {
    @Bean
    public CamelSagaService sagaService() {
        return new InMemorySagaService();
    }

    @Bean
    public CamelContextConfiguration sagaContextConfiguration(CamelSagaService sagaService) {
        return new CamelContextConfiguration() {
            @Override
            public void beforeApplicationStart(CamelContext context) {
                try {
                    context.addService(sagaService);
                } catch (Exception exception) {
                    throw new IllegalStateException("Cannot register Saga service", exception);
                }
            }

            @Override
            public void afterApplicationStart(CamelContext context) {
            }
        };
    }
}
