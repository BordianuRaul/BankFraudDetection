package org.example.fraud.alert;

import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * REST view of the stored alerts.
 *   GET /api/alerts   -> all alerts, newest first
 */
@RestController
@RequestMapping("/api/alerts")
public class AlertController {

    private final AlertRepository repository;

    public AlertController(AlertRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    public List<AlertEntity> all() {
        return repository.findAll(Sort.by(Sort.Direction.DESC, "detectedAt"));
    }
}
