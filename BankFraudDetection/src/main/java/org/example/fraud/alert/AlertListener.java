package org.example.fraud.alert;

import org.example.fraud.config.KafkaTopicConfig;
import org.example.fraud.model.FraudAlert;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AlertListener {

    private final AlertRepository repository;

    public AlertListener(AlertRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = KafkaTopicConfig.FRAUD_ALERTS, groupId = "alert-store")
    public void onAlert(FraudAlert alert) {
        repository.save(new AlertEntity(
                alert.alertId(),
                alert.transactionId(),
                alert.accountId(),
                alert.score(),
                alert.severity(),
                String.join(",", alert.triggeredRules()),
                alert.explanation(),
                alert.detectedAt()
        ));
    }
}
