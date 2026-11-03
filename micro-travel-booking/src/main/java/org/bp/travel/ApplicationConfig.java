package org.bp.travel;

import java.util.Arrays;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.HazelcastInstance;
import org.bp.travel.state.ProcessingEvent;
import org.bp.travel.state.ProcessingState;
import org.bp.travel.state.StateMachineBuilder;
import org.bp.travel.state.StateService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class ApplicationConfig {

    @Bean(name="basicStateMachineBuilder")
    public StateMachineBuilder basicStateMachineBuilder() {
    	StateMachineBuilder smb = new StateMachineBuilder();
    	smb.initialState(ProcessingState.NONE)
        .add(ProcessingState.NONE,ProcessingEvent.START,ProcessingState.STARTED)
        .add(ProcessingState.STARTED,ProcessingEvent.FINISH,ProcessingState.FINISHED)
    	.add(ProcessingState.NONE,ProcessingEvent.CANCEL,ProcessingState.CANCELLED)
    	.add(ProcessingState.STARTED,ProcessingEvent.CANCEL,ProcessingState.CANCELLED)
    	.add(ProcessingState.FINISHED,ProcessingEvent.CANCEL,ProcessingState.CANCELLED)
    	.add(ProcessingState.CANCELLED,ProcessingEvent.START,ProcessingState.CANCELLED)
    	.add(ProcessingState.CANCELLED,ProcessingEvent.FINISH,ProcessingState.CANCELLED)
    	.add(ProcessingState.FINISHED,ProcessingEvent.COMPLETE,ProcessingState.COMPLETED)
		.add(ProcessingState.CANCELLED,ProcessingEvent.COMPLETE,ProcessingState.COMPLETED)
         ;
    	return smb;
    }

    @Bean
    public Config hazelcastConfig(
            @Value("${travel.hazelcast.cluster-name}") String clusterName,
            @Value("${travel.hazelcast.members}") String members) {
        Config config = new Config();
        config.setClusterName(clusterName);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        if (members.isBlank()) {
            join.getMulticastConfig().setEnabled(true);
        } else {
            join.getMulticastConfig().setEnabled(false);
            join.getTcpIpConfig().setEnabled(true).setMembers(Arrays.asList(members.split(",")));
        }
        return config;
    }

    @Bean
    public StateService hotelStateService(
            @Qualifier("basicStateMachineBuilder") StateMachineBuilder stateMachineBuilder,
            HazelcastInstance hazelcastInstance) {
    	return new StateService(stateMachineBuilder, hazelcastInstance.getMap("hotelStates"));
    }

    @Bean
    public StateService flightStateService(
            @Qualifier("basicStateMachineBuilder") StateMachineBuilder stateMachineBuilder,
            HazelcastInstance hazelcastInstance) {
    	return new StateService(stateMachineBuilder, hazelcastInstance.getMap("flightStates"));
    }
}
