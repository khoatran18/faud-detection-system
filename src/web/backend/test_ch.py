import sys, os
sys.path.insert(0, '.')
os.environ['APP_ENV'] = 'dev'

from clickhouse_driver import Client

# Direct test
c = Client(host='localhost', port=9002, user='default', password='', database='predictions')
print("=== Direct ClickHouse test ===")
print("Version:", c.execute("SELECT version()"))
print("Tables:", c.execute("SHOW TABLES FROM predictions"))
print("Distinct model_ids in fraud_prediction:", c.execute("SELECT DISTINCT model_id FROM predictions.fraud_prediction LIMIT 10"))
print("Distinct model_ids in model_monitor:", c.execute("SELECT DISTINCT model_id FROM predictions.model_monitor LIMIT 10"))
print("Count fraud_prediction:", c.execute("SELECT count() FROM predictions.fraud_prediction"))
print("Count model_monitor:", c.execute("SELECT count() FROM predictions.model_monitor"))
c.disconnect()
print("Done.")
