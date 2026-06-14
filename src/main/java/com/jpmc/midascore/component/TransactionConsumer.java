package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionConsumer {
    private static final Logger logger = LoggerFactory.getLogger(TransactionConsumer.class);
    private final DatabaseConduit databaseConduit;

    public TransactionConsumer(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    @Transactional
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);

        UserRecord sender = databaseConduit.getUserById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.getUserById(transaction.getRecipientId());

        if (sender == null) {
            logger.warn("Discarding transaction: Sender ID {} not found", transaction.getSenderId());
            return;
        }

        if (recipient == null) {
            logger.warn("Discarding transaction: Recipient ID {} not found", transaction.getRecipientId());
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Discarding transaction: Sender {} has insufficient balance ({} < {})",
                    sender.getName(), sender.getBalance(), transaction.getAmount());
            return;
        }

        // Process valid transaction
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        databaseConduit.save(sender);
        databaseConduit.save(recipient);

        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount());
        databaseConduit.saveTransaction(record);

        logger.info("Transaction processed and recorded: {}", record);
    }
}
