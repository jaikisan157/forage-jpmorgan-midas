package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveService {
    private final RestTemplate restTemplate;
    private final String incentiveUrl = "http://localhost:8080/incentive";

    public IncentiveService() {
        this.restTemplate = new RestTemplate();
    }

    public float getIncentiveAmount(Transaction transaction) {
        try {
            Incentive response = restTemplate.postForObject(incentiveUrl, transaction, Incentive.class);
            if (response != null) {
                return response.getAmount();
            }
        } catch (Exception e) {
            // Log or handle the error appropriately
            System.err.println("Error calling incentive API: " + e.getMessage());
        }
        return 0f;
    }
}
