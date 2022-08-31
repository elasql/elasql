package org.elasql.perf.tpart.mdp.bandit;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasql.perf.tpart.CentralRoutingAgent;
import org.elasql.perf.tpart.mdp.State;
import org.elasql.perf.tpart.mdp.TransactionRoutingEnvironment;
import org.elasql.perf.tpart.mdp.bandit.data.BanditTransactionArm;
import org.elasql.perf.tpart.mdp.bandit.data.BanditTransactionContext;
import org.elasql.perf.tpart.mdp.bandit.data.BanditTransactionContextFactory;
import org.elasql.perf.tpart.mdp.bandit.data.BanditTransactionDataCollector;
import org.elasql.perf.tpart.mdp.bandit.data.BanditTransactionReward;
import org.elasql.perf.tpart.mdp.bandit.model.BanditModel;
import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.Route;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;

public class BanditAgent implements CentralRoutingAgent {
	
	private final BanditTransactionDataCollector dataCollector;
	private final BanditTransactionContextFactory contextFactory;
	private final BanditModel model;
	private final RoutingBanditActuator actuator;
	
	private TransactionRoutingEnvironment env = new TransactionRoutingEnvironment();
	private Map<Long, State> cachedStates = new ConcurrentHashMap<Long, State>();
	
	public BanditAgent() {
		dataCollector = new BanditTransactionDataCollector();
		contextFactory = new BanditTransactionContextFactory();
		model = new BanditModel();
		actuator = new RoutingBanditActuator(model);
		Elasql.taskMgr().runTask(actuator);
	}

	@Override
	public Route suggestRoute(TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse) {
		State state = env.getCurrentState(graph, task, metricWarehouse);
		
		// Cache the state for later training
		cachedStates.put(task.getTxNum(), state);
		
		BanditTransactionContext banditTransactionContext = contextFactory.buildContext(task.getTxNum(), state);
		int arm = model.chooseArm(task.getTxNum(), banditTransactionContext.getContext());
		
		dataCollector.addContext(banditTransactionContext);
		dataCollector.addArm(new BanditTransactionArm(task.getTxNum(), arm));
		
		return new Route(arm);
	}

	@Override
	public void onTxRouted(long txNum, int routeDest) {
		// For state transition
		env.onTxRouted(txNum, routeDest);
	}

	@Override
	public void onTxCommitted(long txNum, int masterId, long latency) {
		State state = cachedStates.remove(txNum);
		
		if (state == null)
			throw new RuntimeException("Cannot find cached state for tx." + txNum);
		
		float reward = env.calcReward(state, masterId, latency);
		BanditTransactionReward banditTransactionReward = new BanditTransactionReward(txNum, reward);
		actuator.addTransactionData(
				dataCollector.addRewardAndTakeOut(banditTransactionReward));
	}
}
