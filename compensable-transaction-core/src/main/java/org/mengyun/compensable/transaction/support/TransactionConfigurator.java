package org.mengyun.compensable.transaction.support;

import org.mengyun.compensable.transaction.TransactionManager;
import org.mengyun.compensable.transaction.recovery.RecoverConfig;
import org.mengyun.compensable.transaction.repository.TransactionRepository;

/**
 * Created by changming.xie on 2/24/17.
 */
public interface TransactionConfigurator {

    TransactionManager getTransactionManager();

    TransactionRepository getTransactionRepository();

    RecoverConfig getRecoverConfig();
}
