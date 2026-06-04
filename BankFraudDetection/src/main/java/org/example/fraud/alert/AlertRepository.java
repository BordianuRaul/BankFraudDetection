package org.example.fraud.alert;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data gives us CRUD for free from this interface — no implementation needed.
 * Key type is String because AlertEntity's @Id (alertId) is a String.
 */
public interface AlertRepository extends JpaRepository<AlertEntity, String> {
}
