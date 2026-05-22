"""
ClickHouse client — fixed result parsing.
"""
import logging
from typing import Any
from clickhouse_driver import Client
from settings_loader import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE,
    TABLE_PREDICTIONS, TABLE_MONITOR,
)

logger = logging.getLogger(__name__)

def _safe(v, default=0.0):
    """Convert NaN/Inf floats to a JSON-safe default."""
    try:
        f = float(v)
        import math
        return default if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return default

def _client() -> Client:
    return Client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
        connect_timeout=5, send_receive_timeout=10,
    )

def get_active_models() -> list[str]:
    c = _client()
    try:
        r1 = c.execute(f"SELECT DISTINCT model_id FROM {CLICKHOUSE_DATABASE}.{TABLE_PREDICTIONS}")
        r2 = c.execute(f"SELECT DISTINCT model_id FROM {CLICKHOUSE_DATABASE}.{TABLE_MONITOR}")
        ids = {(row[0] if row[0] != '' else 'model_1') for row in r1} | {(row[0] if row[0] != '' else 'model_1') for row in r2}
        return sorted([x for x in ids if x])
    except Exception as e:
        logger.error("get_active_models: %s", e)
        return []
    finally:
        c.disconnect()

def get_prediction_stats(model_id: str, window_mins: int = 30, bucket_str: str = "1 MINUTE") -> dict[str, Any]:
    c = _client()
    try:
        # Totals (always over the selected window)
        mid_filter = "model_id = %(mid)s OR model_id = ''" if model_id == 'model_1' else "model_id = %(mid)s"
        rows = c.execute(
            f"""SELECT
                count()                           AS total,
                countIf(prediction = 1)           AS fraud_count,
                countIf(prediction = 0)           AS legit_count,
                countIf(prediction = 1) / count() AS fraud_rate
            FROM {CLICKHOUSE_DATABASE}.{TABLE_PREDICTIONS}
            WHERE {mid_filter}
              AND process_timestamp >= now() - INTERVAL {window_mins} MINUTE""",
            {"mid": model_id},
        )
        if rows:
            total, fraud_count, legit_count, fraud_rate = rows[0]
            total = int(total or 0)
            fraud_count = int(fraud_count or 0)
            legit_count = int(legit_count or 0)
            fraud_rate = _safe(fraud_rate)
        else:
            total = fraud_count = legit_count = 0
            fraud_rate = 0.0

        # Timeline
        tl = c.execute(
            f"""SELECT
                toStartOfInterval(process_timestamp, INTERVAL {bucket_str}) AS h,
                count()                                                     AS total,
                countIf(prediction = 1)                                     AS fraud_count
            FROM {CLICKHOUSE_DATABASE}.{TABLE_PREDICTIONS}
            WHERE ({mid_filter})
              AND process_timestamp >= now() - INTERVAL {window_mins} MINUTE
            GROUP BY h ORDER BY h ASC""",
            {"mid": model_id},
        )
        timeline = [{"hour_bucket": str(r[0]), "total": r[1], "fraud_count": r[2]} for r in tl]
        return {
            "total": int(total), "fraud_count": int(fraud_count),
            "legit_count": int(legit_count), "fraud_rate": float(fraud_rate or 0),
            "timeline": timeline,
        }
    except Exception as e:
        logger.error("get_prediction_stats(%s): %s", model_id, e)
        return {"total": 0, "fraud_count": 0, "legit_count": 0, "fraud_rate": 0.0, "timeline": []}
    finally:
        c.disconnect()

def get_monitor_stats(model_id: str, window_mins: int = 30, bucket_str: str = "1 MINUTE") -> dict[str, Any]:
    c = _client()
    try:
        rows = c.execute(
            f"""SELECT
                count()                                          AS total,
                countIf(is_correct = 1)                         AS correct,
                countIf(model_predict = 1 AND actual_result = 1) AS tp,
                countIf(model_predict = 1 AND actual_result = 0) AS fp,
                countIf(model_predict = 0 AND actual_result = 0) AS tn,
                countIf(model_predict = 0 AND actual_result = 1) AS fn
            FROM {CLICKHOUSE_DATABASE}.{TABLE_MONITOR}
            WHERE model_id = %(mid)s
              AND process_timestamp >= now() - INTERVAL {window_mins} MINUTE""",
            {"mid": model_id},
        )
        if rows and rows[0][0]:
            total, correct, tp, fp, tn, fn = rows[0]
            total = int(total); correct = int(correct)
            tp = int(tp); fp = int(fp); tn = int(tn); fn = int(fn)
            accuracy  = _safe(correct / total if total > 0 else 0.0)
            precision = _safe(tp / (tp + fp) if (tp + fp) > 0 else 0.0)
            recall    = _safe(tp / (tp + fn) if (tp + fn) > 0 else 0.0)
            f1        = _safe(2*precision*recall/(precision+recall) if (precision+recall) > 0 else 0.0)
        else:
            total = correct = tp = fp = tn = fn = 0
            accuracy = precision = recall = f1 = 0.0

        tl = c.execute(
            f"""SELECT
                toStartOfInterval(process_timestamp, INTERVAL {bucket_str}) AS h,
                count()                                                     AS total,
                countIf(is_correct = 1)                                     AS correct,
                countIf(model_predict = 1 AND actual_result = 1)            AS tp,
                countIf(model_predict = 1 AND actual_result = 0)            AS fp,
                countIf(model_predict = 0 AND actual_result = 0)            AS tn,
                countIf(model_predict = 0 AND actual_result = 1)            AS fn
            FROM {CLICKHOUSE_DATABASE}.{TABLE_MONITOR}
            WHERE model_id = %(mid)s
              AND process_timestamp >= now() - INTERVAL {window_mins} MINUTE
            GROUP BY h ORDER BY h ASC""",
            {"mid": model_id},
        )
        timeline = []
        for r in tl:
            t_total, t_correct, t_tp, t_fp, t_tn, t_fn = r[1], r[2], r[3], r[4], r[5], r[6]
            timeline.append({
                "hour_bucket": str(r[0]),
                "total": t_total,
                "correct": t_correct,
                "tp": t_tp,
                "fp": t_fp,
                "tn": t_tn,
                "fn": t_fn
            })

        return {
            "total": total, "accuracy": accuracy, "precision": precision,
            "recall": recall, "f1": f1,
            "confusion_matrix": {"tp": tp, "fp": fp, "tn": tn, "fn": fn},
            "timeline": timeline,
        }
    except Exception as e:
        logger.error("get_monitor_stats(%s): %s", model_id, e)
        return {"total": 0, "accuracy": 0.0, "precision": 0.0, "recall": 0.0, "f1": 0.0,
                "confusion_matrix": {"tp": 0, "fp": 0, "tn": 0, "fn": 0}, "timeline": []}
    finally:
        c.disconnect()

def get_recent_predictions(model_id: str, limit: int = 50, offset: int = 0) -> list[dict]:
    c = _client()
    try:
        mid_filter = "model_id = %(mid)s OR model_id = ''" if model_id == 'model_1' else "model_id = %(mid)s"
        rows = c.execute(
            f"""SELECT TransactionID, model_id, prediction, probability,
                       TransactionAmt, process_timestamp
            FROM {CLICKHOUSE_DATABASE}.{TABLE_PREDICTIONS}
            WHERE {mid_filter}
            ORDER BY process_timestamp DESC
            LIMIT %(limit)s OFFSET %(offset)s""",
            {"mid": model_id, "limit": limit, "offset": offset},
        )
        cols = ["TransactionID","model_id","prediction","probability","TransactionAmt","process_timestamp"]
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        logger.error("get_recent_predictions: %s", e)
        return []
    finally:
        c.disconnect()

def get_recent_monitor(model_id: str, limit: int = 50, offset: int = 0) -> list[dict]:
    c = _client()
    try:
        rows = c.execute(
            f"""SELECT TransactionID, model_id, model_predict, actual_result,
                       is_correct, process_timestamp
            FROM {CLICKHOUSE_DATABASE}.{TABLE_MONITOR}
            WHERE model_id = %(mid)s
            ORDER BY process_timestamp DESC
            LIMIT %(limit)s OFFSET %(offset)s""",
            {"mid": model_id, "limit": limit, "offset": offset},
        )
        cols = ["TransactionID","model_id","model_predict","actual_result","is_correct","process_timestamp"]
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        logger.error("get_recent_monitor: %s", e)
        return []
    finally:
        c.disconnect()
