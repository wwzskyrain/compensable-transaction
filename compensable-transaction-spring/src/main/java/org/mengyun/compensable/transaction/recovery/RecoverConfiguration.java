package org.mengyun.compensable.transaction.recovery;

import org.apache.commons.lang3.StringUtils;
import org.mengyun.compensable.transaction.SystemException;
import org.mengyun.compensable.transaction.TransactionManager;
import org.mengyun.compensable.transaction.repository.CachableTransactionRepository;
import org.mengyun.compensable.transaction.repository.TransactionRepository;
import org.mengyun.compensable.transaction.support.TransactionConfigurator;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;

/**
 * Created by changming.xie on 11/25/17.
 */
public class RecoverConfiguration implements TransactionConfigurator {


    private TransactionManager transactionManager;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired(required = false)
    private RecoverConfig recoverConfig = DefaultRecoverConfig.INSTANCE;

    @Autowired(required = false)
    private Scheduler scheduler;

    private String jobName;

    private String triggerName;

    @PostConstruct
    public void init() {
        transactionManager = new TransactionManager();
        transactionManager.setTransactionRepository(transactionRepository);

        if (transactionRepository instanceof CachableTransactionRepository) {
            ((CachableTransactionRepository) transactionRepository).setExpireDuration(recoverConfig.getRecoverDuration());
        }

        TransactionRecovery transactionRecovery = new TransactionRecovery();
        transactionRecovery.setTransactionConfigurator(this);

        RecoverScheduledJob recoveryScheduledJob = new RecoverScheduledJob();
        recoveryScheduledJob.setJobName(StringUtils.isEmpty(jobName) ? "compensableRecoverJob" : jobName);
        recoveryScheduledJob.setTriggerName(StringUtils.isEmpty(triggerName) ? "compensableTrigger" : triggerName);

        recoveryScheduledJob.setTransactionRecovery(transactionRecovery);
        recoveryScheduledJob.setCronExpression(getRecoverConfig().getCronExpression());

        if (scheduler == null) {
            SchedulerFactory schedulerFactory = new org.quartz.impl.StdSchedulerFactory();
            try {
                scheduler = schedulerFactory.getScheduler();
            } catch (SchedulerException e) {
                throw new SystemException(e);
            }
        }

        recoveryScheduledJob.setScheduler(scheduler);
        recoveryScheduledJob.init();
    }
    
    @Override
    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    @Override
    public TransactionRepository getTransactionRepository() {
        return transactionRepository;
    }

    @Override
    public RecoverConfig getRecoverConfig() {
        return recoverConfig;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public void setTriggerName(String triggerName) {
        this.triggerName = triggerName;
    }

    public void setTransactionRepository(TransactionRepository transactionRepository) {
        this.transactionRepository = transactionRepository;
    }
}
