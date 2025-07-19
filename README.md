# Real-Time Credit Card Fraud Detection with PySpark & Kafka

This project implements a real-time fraud detection pipeline using:
- Apache Kafka for streaming transactions
- PySpark Structured Streaming for real-time processing
- XGBoost for fraud detection
- Modular architecture with config, inference, and pre-processing

## Goals

Deploy the CC fraud detection model that was built [here](https://github.com/soutogustavo/cc-fraud-detection) in a real-time environment.

**Note:**<br>
I want to let you know that I split the project into two parts: 
- [1] exploration and model building, and
- [2] model deployment in a real-time environment (i.e., this repository).

Thus, I believe it would be much easier to follow up with the project.

## Project Structure

| Folder         | Purpose                                                  |
|----------------|----------------------------------------------------------|
| `kafka/`       | Kafka producer for test messages, and docker-composer    |
| `scripts/`     | Stream processing logic                                  |
| `models/`      | Pre-trained XGBoost model                                |
| `config/`      | YAML-based configuration                                 |
| `data/`        | Test dataset to be streamed                              |
| `utils/`       | Config loading helper                                    |

## Setup

1. Clone the repo:

```bash
git clone https://github.com/soutogustavo/deployment-cc-fraud-detection.git
cd deployment-cc-fraud-detection
```

2. Create a conda environment:
```bash
conda env create -f environment.yml
```
**Note:** In this environment, I used Python 3.10. Also, **I assume that you have PySpark set up on your machine**

3. Start Kafka (see Docker Compose example or your Kafka setup)
```bash
docker compose up -d
```
You can also stop the (Kafka) container: `docker compose down -v`

**Note:** You can use `docker-desktop` to manage the containers/images.

<img width="1271" height="480" alt="image" src="https://github.com/user-attachments/assets/6e111f65-4c21-4a52-a030-1374a7df6dfd" />

3.1 Set up Kafka topics
Before running the pipeline, we must create two (Kafka) topics:
- `cc.transactions`: the (Kafka) producer sends the CC transactions to this topic.
- `alerts.cc.fraud`: fraudulent transactions are sent to this topic.

Assuming that you use `docker-desktop`, you can open **broker** in the terminal by clicking the three dots (i.e., option *Open in Terminal*).
Thus, you can run the commands below to create the two topics:
```bash
cd /opt/kafka/bin/
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic cc.transactions
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic alerts.cc.fraud
```
You can also check the topics by listing Kafka topics:
```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --list
```

If you want to delete a topic, please run the following command:
```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic <topic-name>
```


4. Run the producer:

**Note 1:** Before running the producer, unzip the dataset in the `data` folder. Thus, the producer can read it.<br>
**Note 2:** Run it from the root folder.

```bash
python kafka/transactions.py
```

5. Start the stream processor:

**Note 1:** Run it from the root folder.

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 stream_processor.py
```

6. Run the CC fraud alert (consumer):

You can check the alerts/frauds that were detected by the model.
**Note 1:** Run it from the root folder.

```bash
python kafka/fraud_alerts_consumer.py
```
You should expect that it shows the detected fraud as follows:
<img width="1635" height="533" alt="image" src="https://github.com/user-attachments/assets/bb404a95-3a50-4e08-a7ca-1e5925981009" />

**[Tip]** I should open different terminal windows to see each script running. Thus, you can follow up with each part of the project (i.e., producer, streaming, and consumer)

