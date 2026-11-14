# Warehouse
## Overview:
*Test project about warehouses with sensors.*

There is a warehouse equipped with various types of sensors that monitor environmental conditions. These sensors provide measurements such as current temperature and humidity, which are transmitted via UDP. The warehouse service interacts with all these sensors and automatically publishes the measurements to a central monitoring service. This service oversees multiple warehouses and activates an alarm if temperature or humidity readings exceed configured thresholds.
## System Design:
- Warehouse Service: Collects data from various sensors and sends it to the Central Monitoring Service.
- Central Monitoring Service: Configured with thresholds for temperature and humidity. Raises an alarm when sensor measurements cross these thresholds. The alarm message should be visible in the logs/console.
## Technical Requirements
- Temperature Sensor
    - UDP Port: `3344`
    - Measurement Syntax: `sensor_id=t1; value=30`
    - Threshold: `35°C`

- Humidity Sensor
    - UDP Port: `3355`
    - Measurement Syntax: `sensor_id=h1; value=40`
    - Threshold: `50%`
## Short description
Project uses core Java 21. All logs are written to console without additional libraries.

### General structure
- **WarehouseService** waits for UDP messages from sensors. When a message is received, it is parsed and sent forward using **RabbitMQ** or **Kafka**.
- **CentralService** consumes **Kafka** messages, analyses them and sends alarm (*i.e. writes **WARN** log to its console*) if values exceed thresholds.
- **Common** library contains common classes used by both services.

Project is covered by **Mockito** tests and **Jacoco** reports.

## Notices
- **Kafka broker** needs to be started before running services.
- There is a configuration file **application.properties** here. It is here for demonstration purposes only, in any real project it have stay on local computer only due to security purposes.