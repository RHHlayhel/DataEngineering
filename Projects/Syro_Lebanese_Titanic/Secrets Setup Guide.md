
This project uses Databricks Secrets to store credentials securely.
**Never hardcode credentials in notebooks or commit them to GitHub.**

## Prerequisites

Install the Databricks CLI:
```bash
pip install databricks-cli
```

Authenticate with your Databricks workspace:
```bash
databricks configure --token
# Host: https://your-workspace.cloud.databricks.com
# Token: your Databricks personal access token
```

To get your Databricks token:
**Settings → Developer → Access Tokens → Generate New Token**

## Required Secrets

### 1. Create the secrets scope
```bash
databricks secrets create-scope --scope silk_pipeline
```

### 2. FRED API Key
Get your free API key at: https://fred.stlouisfed.org/docs/api/api_key.html
```bash
databricks secrets put --scope silk_pipeline --key fred_api_key --string-value "your_fred_api_key"
```

### 3. Confluent Cloud Credentials
Sign up for a free account at: https://confluent.cloud

Create a Kafka cluster, then generate a **Cluster API Key** (not Global):
- Go to your cluster → API Keys → Create Key → My Account

```bash
databricks secrets put --scope silk_pipeline --key confluent_api_key --string-value "your_confluent_api_key"
databricks secrets put --scope silk_pipeline --key confluent_api_secret --string-value "your_confluent_api_secret"
databricks secrets put --scope silk_pipeline --key confluent_bootstrap_server --string-value "your_bootstrap_server"
```

Bootstrap server format: `pkc-xxxxxx.region.gcp.confluent.cloud:9092`

---

## Verify Setup
```bash
databricks secrets list --scope silk_pipeline
```

Expected output:
```
KEY                        LAST_UPDATED
fred_api_key               xxxxxxxx
confluent_api_key          xxxxxxxx
confluent_api_secret       xxxxxxxx
confluent_bootstrap_server xxxxxxxx
```

---

## Confluent Cloud Topic Setup

In your Confluent Cloud cluster:
1. Go to **Topics → Create Topic**
2. Name: `silk-prices`
3. Partitions: `1`
4. Click **Create**

---

## Notes
- All secrets are accessed in notebooks via `dbutils.secrets.get(scope='silk_pipeline', key='...')`
- Never print or log secret values
- Rotate credentials periodically for security
