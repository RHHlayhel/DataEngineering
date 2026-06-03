
This is a two-part data engineering project combining historical passenger analysis and an added real-time commodity price monitoring pipeline, raw silk in our case.

## Part 1: Syro-Lebanese Titanic Passenger Analysis

### Overview
The Titanic dataset contains no nationality field. This project uses Lebanese and Syrian surname matching against a curated names dataset to identify probable Syro-Lebanese passengers and analyse their survival patterns.

### Research Question
*How many Syro-Lebanese passengers were aboard the Titanic, and what can we know about them? What is the context of their trip on the Titanic?* 

### Methodology
1. Load and clean the Titanic dataset (main + addendum) from GitHub
2. Extract surnames from passenger names
3. Match against a curated Syro-Lebanese surname list sourced from the `names-dataset` library (491M profiles, LB + SY country codes)
4. Analyse matched passengers by survival rate, class, age, sex and embarkation port
5. Visualize the extracted data using an interactive dashboard

### Known Limitations and Remediation
- **False positives**: While some Syro-Lebanese have common Western first names (e.g. `George`, `Joseph`), surnames are usually distinctly Arabic. That is why we used the surnames for the identification. However, this method of identification remains very broad, and the passengers identified might be of other Arabic origins.
- **Transliteration variants**: names like Khalil/Khaleel or Assaf/Asseff may not match due to different transliteration conventions used at time of registration.
- **Dataset coverage**: the `names-dataset` library is derived from Facebook profiles and may underrepresent certain regional name variations. We also had to limit the entries in the dataset to the ones written in the Latin script. However, we used a list of 100000 top surnames across Syria and Lebanon to give a very broad spectrum across that region.
- **Result**: 47 probable Syro-Lebanese passengers identified IN THIS DATASET; actual number may differ. The Titanic dataset we used has approx. 1400 entries while the total number of the Titanic passengers is around 2200, so it is normal to find less than the total number of Syro-Lebanese passengers (approx. 120).

### Key Findings
- Majority travelled in 3rd class (41/47).
- The Average age of the passengers was 23
- Majority were young men (35/47)
- Only 12 passengers survived (3 men and 9 women)
- Most Syro-Lebanese emigrants boarded at Cherbourg

### Stack
- Apache Spark (PySpark) on Databricks
- `names-dataset` Python library
- Plotly for visualization

---

## Part 2: Silk Price Monitoring Pipeline

### Overview
A real-time data pipeline that fetches the US Producer Price Index for silk from the Federal Reserve (FRED), simulates weekly price fluctuations, streams data through Apache Kafka, stores it in a Delta Lake table, and visualizes it on an interactive dashboard.

### Architecture

![[Architecture.png]]

### Data Sources
- **FRED API** (`WPU034203`): Producer Price Index for Silk and Natural Fiber Textiles, published monthly by the US Bureau of Labor Statistics. Free API key required.
- **Weekly simulation**: realistic ±1.5% weekly fluctuation around the monthly PPI baseline, scaled to USD/MT (~$55,000–65,000/MT range).

### Why Simulated Prices?
Real-time silk spot prices are behind commercial paywalls (Price-Watch™, etc.). The FRED PPI is a validated government index that anchors our simulation to real market conditions. When FRED updates monthly, the baseline automatically adjusts.

### Pipeline Components

| Component      | Tool                            | Purpose                                           |
| -------------- | ------------------------------- | ------------------------------------------------- |
| Producer       | `confluent-kafka` + `requests`  | Fetch FRED data, simulate price, publish to Kafka |
| Message Broker | Confluent Cloud (free tier)     | Managed Kafka cluster                             |
| Consumer       | Databricks Structured Streaming | Read Kafka topic, write to Delta Table            |
| Storage        | Delta Lake                      | Persistent, versioned data storage                |
| Scheduler      | Databricks Workflows            | Weekly automated pipeline execution               |
| Dashboard      | Plotly                          | Interactive price trend visualisation             |

### Scheduling
The pipeline runs automatically every week via Databricks Workflows:
```
Producer (Silk_Price_Monitor) → Consumer (Silk_Price_Fetching)
```
The consumer only runs if the producer succeeds (`Run if dependencies: All succeeded`).

### Stack
- Apache Spark Structured Streaming on Databricks (Serverless)
- Confluent Cloud (Apache Kafka)
- Delta Lake
- FRED API (Federal Reserve Economic Data)
- Plotly

---

## Project Structure

```
├── data/
│   ├── Titanic-Dataset.csv          # Main Titanic dataset
│   └── AddendumData.csv             # Titanic test set addendum
│
├── Titanic_Passenger_Analysis.ipynb # Part 1 — full analysis notebook
│
├── Silk_01_Producer.ipynb           # Part 2 — Kafka producer
├── Silk_02_Consumer.ipynb           # Part 2 — Kafka consumer  
├── Silk_03_Dashboard.ipynb          # Part 2 — Plotly dashboard
│
├── requirements.txt                 # Python dependencies
├── secrets_setup.md                 # Guide for setting up Databricks secrets
├── .gitignore                       # Git ignore rules
└── README.md                        # This file
```

---

## Setup

### Prerequisites
- Databricks workspace (Serverless compute)
- Confluent Cloud account (free tier)
- FRED API key (free — https://fred.stlouisfed.org/docs/api/api_key.html)
- Databricks CLI installed (`pip install databricks-cli`)

### 1. Clone the repository
```bash
git clone https://github.com/RHHlayhel/DataEngineering.git
```

### 2. Upload data files to Databricks
```python
# In a Databricks notebook
import urllib.request
import os

os.makedirs('/Volumes/workspace/default/titanic/Data', exist_ok=True)

urllib.request.urlretrieve(
    'https://raw.githubusercontent.com/RHHlayhel/DataEngineering/main/Projects/Syro_Lebanese_Titanic/data/Titanic-Dataset.csv',
    '/Volumes/workspace/default/titanic/Data/Titanic-Dataset.csv'
)
```

### 3. Configure Databricks secrets

See `secrets_setup.md` for full instructions.

### 4. Run the notebooks in order
**Part 1:**
```
Titanic_Passenger_Analysis.ipynb
```

**Part 2:**
```
Silk_01_Producer.ipynb  →  Silk_02_Consumer.ipynb  →  Silk_03_Dashboard.ipynb
```

---

## Dependencies

See `requirements.txt` for the full list.

---

## Author
Raed Hlayhel  
[GitHub](https://github.com/RHHlayhel)
