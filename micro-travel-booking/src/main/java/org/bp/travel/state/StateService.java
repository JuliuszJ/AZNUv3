package org.bp.travel.state;

import com.hazelcast.map.IMap;

public class StateService {
	private final IMap<String, ProcessingState> processingStates;
	private final StateMachineBuilder stateMachineBuilder;

	public StateService(StateMachineBuilder stateMachineBuilder, IMap<String, ProcessingState> processingStates) {
		this.stateMachineBuilder = stateMachineBuilder;
		this.processingStates = processingStates;
	}

	public ProcessingState sendEvent(String bookingId, ProcessingEvent event) {
		processingStates.lock(bookingId);
		try {
			ProcessingState previousState = processingStates.get(bookingId);
			if (previousState==null) {
				previousState=stateMachineBuilder.getInitialState();
			}
			processingStates.set(bookingId, stateMachineBuilder.sendEvent(previousState, event));
			return previousState;
		}
		finally {
			processingStates.unlock(bookingId);
		}
	}

	public void removeState(String bookingId) {
		processingStates.delete(bookingId);
	}

}
