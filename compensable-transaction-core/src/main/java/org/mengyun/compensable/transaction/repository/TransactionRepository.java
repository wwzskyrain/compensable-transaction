package org.mengyun.compensable.transaction.repository;

import org.mengyun.compensable.transaction.Transaction;
import org.mengyun.compensable.transaction.TransactionXid;

import java.util.Date;
import java.util.List;

/**
 * Created by changmingxie on 11/12/15.
 */
public interface TransactionRepository {

    int create(Transaction transaction);

    int update(Transaction transaction);

    int delete(Transaction transaction);

    Transaction findByXid(TransactionXid xid);

    List<Transaction> findAllUnmodifiedSince(Date date);

    void deleteAll();
}
