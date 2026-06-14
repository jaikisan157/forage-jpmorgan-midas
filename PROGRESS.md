# JPMorgan Chase – Midas Core Virtual Internship (Forage)
## Progress Tracker & Interview Notes

---

## 🗂️ Program Overview

**Program:** JPMorgan Chase Advanced Software Engineering Virtual Experience (Forage)  
**Project:** Midas Core — a backend financial transaction processing service  
**Tech Stack:** Java 17, Spring Boot 3.2.5, Apache Kafka, H2 Database, Maven, Testcontainers  
**Repo Location:** `C:\Users\jaikisan\Downloads\forage-jpmorgan-midas`

---

## ✅ Task 1 — Project Setup & Environment Configuration
### Status: COMPLETE ✅

### 🎯 Goal
Set up the local development environment so the Spring Boot application boots successfully and passes the automated verification test.

---

### 🔧 What We Actually Did (Step by Step)

#### Step 1 — Checked Java Installation
- Verified **Java 17 (Temurin)** was already installed at `C:\Program Files\Java\jdk-17`
- Command: `java -version` → `openjdk version "17.0.12"`
- **Why Java 17?** Spring Boot 3.x requires Java 17 minimum. In financial systems, a stable LTS version is required for production.

#### Step 2 — Explored the Project Structure
The project scaffold already included:
```
forage-jpmorgan-midas/
├── pom.xml                          ← Maven build config with all dependencies
├── application.yml                  ← Was empty — needed to be configured!
├── mvnw / mvnw.cmd                  ← Maven wrapper scripts (no global Maven needed)
├── src/main/java/com/jpmc/midascore/
│   ├── MidasCoreApplication.java    ← Spring Boot entry point (@SpringBootApplication)
│   ├── component/DatabaseConduit.java  ← Saves user records to DB
│   ├── entity/UserRecord.java       ← JPA entity (maps to DB table)
│   ├── foundation/Transaction.java  ← Transaction data model
│   ├── foundation/Balance.java      ← Balance data model
│   └── repository/UserRepository.java  ← Spring Data JPA repository
└── src/test/java/com/jpmc/midascore/
    ├── TaskOneTests.java            ← ✅ Test we needed to pass
    ├── KafkaProducer.java           ← Helper to send Kafka messages in tests
    ├── UserPopulator.java           ← Loads test users into DB
    └── ...more test helpers
```

#### Step 3 — Checked the pom.xml (Maven Dependencies)
The `pom.xml` already had all required dependencies:

| Dependency | Purpose |
|---|---|
| `spring-boot-starter-web` | Exposes REST API endpoints |
| `spring-boot-starter-data-jpa` | ORM for database operations |
| `spring-kafka` | Kafka producer/consumer integration |
| `h2` | Lightweight in-memory SQL database (for testing) |
| `spring-boot-starter-test` | JUnit + Mockito for testing |
| `spring-kafka-test` | Embedded Kafka for testing |
| `testcontainers:kafka` | Docker-based Kafka for integration tests |

> **Interview point:** In real banking systems, Kafka is used to decouple services — transactions are published as events so multiple downstream services (fraud detection, ledger updates, notifications) can react independently.

#### Step 4 — Ran the Test (First Attempt — Failed)
- Ran: `mvnw clean test -Dtest=TaskOneTests`
- **Error:** `Unknown host: repo.maven.apache.org` — couldn't download dependencies
- **Root cause:** The local `.m2/repository` cache was missing all the actual JAR files for Spring Boot 3.2.5. Only some POMs were cached from a previous failed attempt.
- **Also found:** A broken cached POM for `jetty-bom-9.4.53` was blocking the dependency resolution chain.

#### Step 5 — Fixed the log4j Version Issue
- **Problem:** The old `log4j-2.21.1.pom` referenced `jetty-bom 9.4.53` which had a failed download stub (`.lastUpdated` file) in the cache.
- **Fix:** Added `<log4j2.version>2.25.3</log4j2.version>` to `pom.xml` to use a newer log4j version that was fully cached locally.
- **File changed:** `pom.xml`

#### Step 6 — Downloaded All Dependencies (Online Run)
- Removed `-o` (offline flag) and ran Maven with internet access
- Maven successfully downloaded the full dependency tree:
  - Spring Boot 3.2.5 JARs (~30+ artifacts)
  - Kafka & Netty libraries
  - Testcontainers (docker-java)
  - H2 database JAR

#### Step 7 — Second Test Run (Failed — Missing Config)
- **Error:** `Could not resolve placeholder 'general.kafka-topic'`
- **Root cause:** The `application.yml` at the root of the project was completely **empty**, and Spring Boot only looks for config in `src/main/resources/`, not the root folder.
- The `KafkaProducer` test bean uses `@Value("${general.kafka-topic}")` which requires this property to be defined.

#### Step 8 — Created `src/main/resources/application.yml` ✅ KEY FIX
Created the missing configuration file with all required settings:

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:testdb        # In-memory H2 database
    driver-class-name: org.h2.Driver
    username: sa
    password:
  jpa:
    database-platform: org.hibernate.dialect.H2Dialect
    hibernate:
      ddl-auto: create-drop        # Auto-create tables on startup, drop on shutdown
  h2:
    console:
      enabled: true
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: midas-core-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer

general:
  kafka-topic: trader-updates        # ← This was the missing property!

server:
  port: 8080
```

#### Step 9 — Final Test Run (PASSED ✅)
```
BUILD SUCCESS
Tests run: 1, Failures: 0, Errors: 0
```
**Submission output generated:**
```
---begin output ---
1142725631254665682354316777216387420489
---end output ---
```

---

### 💡 Interview Talking Points for Task 1

> **"What does this task simulate in a real job?"**  
> Onboarding to an existing codebase — understanding the project structure, resolving dependency issues, and configuring the environment before writing any feature code.

> **"Why Spring Boot?"**  
> Spring Boot is the dominant enterprise Java framework. It handles boilerplate (web server, DB connections, Kafka wiring) so engineers can focus on business logic.

> **"Why Kafka?"**  
> In high-volume financial systems, you can't process millions of transactions synchronously. Kafka acts as a message queue — transactions are published once and consumed by multiple services asynchronously.

> **"Why H2 for testing?"**  
> H2 is an in-memory database that spins up and tears down with each test run — no external DB needed. Real environments use PostgreSQL/Oracle, but H2 makes tests fast and self-contained.

## ✅ Task 2 — Kafka Consumer Integration
### Status: COMPLETE ✅

**Goal:** Listen to transactions from the Kafka topic and deserialize them into `Transaction` objects.

### 🔧 What We Did
- Created `TransactionConsumer.java` in `com.jpmc.midascore.component`.
- Used `@KafkaListener(topics = "${general.kafka-topic}")` to dynamically consume from the configured topic (`trader-updates`).
- Verified implementation with `TaskTwoTests` using embedded Kafka.

### 📊 Verification Results (First 4 Transactions)
Looking at the console output of the test run, the first four transactions processed were:
1. **122.86**
2. **42.87**
3. **161.79**
4. **22.22**

---

## ✅ Task 3 — Database Integration & Transaction Validation
### Status: COMPLETE ✅

**Goal:** Integrate Midas Core with H2 DB, validate incoming transactions, persist valid transaction records, and adjust sender/recipient user balances.

### 🔧 What We Did
- Created `TransactionRecord.java` JPA entity with `@ManyToOne` relationships to `UserRecord` for sender and recipient.
- Created `TransactionRepository.java` interface extending `CrudRepository`.
- Updated `DatabaseConduit.java` to support retrieving users by ID and persisting transaction records.
- Updated `TransactionConsumer.java` listener to validate transactions (checks sender and recipient existence, and sender balance), update user balances, and persist the transaction inside a transaction context (`@Transactional`).
- Verified implementation with `TaskThreeTests` using embedded Kafka.

### 📊 Verification Results (Waldorf Balance)
Waldorf's balance after processing all transactions:
- **Actual:** `627.86`
- **Submission (Rounded Down):** **`627`**

---

## ✅ Task 4 — External API Integration & Incentives
### Status: COMPLETE ✅

**Goal:** Integrate Midas Core with the external Incentive API to receive and persist incentive amounts for transactions, updating recipient balances without deducting from senders.

### 🔧 What We Did
- Created [Incentive.java](file:///c:/Users/jaikisan/Downloads/forage-jpmorgan-midas/src/main/java/com/jpmc/midascore/foundation/Incentive.java) to model the REST API's response containing the incentive `amount`.
- Created [IncentiveService.java](file:///c:/Users/jaikisan/Downloads/forage-jpmorgan-midas/src/main/java/com/jpmc/midascore/component/IncentiveService.java) utilizing Spring's `RestTemplate` to post serialized `Transaction` payloads to the `/incentive` endpoint.
- Modified [TransactionRecord.java](file:///c:/Users/jaikisan/Downloads/forage-jpmorgan-midas/src/main/java/com/jpmc/midascore/entity/TransactionRecord.java) entity to include a new `incentive` column and updated its persistence constructors.
- Updated [TransactionConsumer.java](file:///c:/Users/jaikisan/Downloads/forage-jpmorgan-midas/src/main/java/com/jpmc/midascore/component/TransactionConsumer.java) to invoke `IncentiveService`, add the returned incentive to the recipient's balance (not deducting from the sender), and save the transaction record with the incentive amount.
- Ran the external API locally and ran `TaskFourTests`.

### 📊 Verification Results (Wilbur Balance)
Wilbur's balance after processing all transactions:
- **Actual:** `3089.42`
- **Submission (Rounded Down):** **`3089`**

---

## 🔲 Task 5 — Full End-to-End Flow
### Status: NOT STARTED

**Goal:** Complete the full pipeline — Kafka → DB → API → balance update with incentives.

