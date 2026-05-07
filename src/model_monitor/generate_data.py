import random
import time
from datetime import datetime


def generate_data():
    """
    Infinite Generator sinh dữ liệu mô phỏng:
    - Actual Result: 90% là 0, 10% là 1.
    - Model Distribution: Model_1 chiếm ~80% (4/5), Model_2 chiếm ~20% (1/5).
    - Accuracy: Model_1 đúng 80%, Model_2 đúng 90%.
    """
    current_id = 1000000
    i = 0
    while True:
        # 1. Get model_id with rate model_1 / model_2 = 4/1
        model_id = "model_1" if random.random() < 0.8 else "model_2"

        # 2. Emulate actual result with rate 90% True
        actual_result = 0 if random.random() < 0.9 else 1

        # 3. Accuracy rate (model_2 is better than model_1)
        accuracy_threshold = 0.8 if model_id == "model_1" else 0.9

        # 4. Generate model prediction
        if random.random() < accuracy_threshold:
            model_predict = actual_result
            is_correct = 1
        else:
            model_predict = 1 - actual_result
            is_correct = 0

        # 5. Return record
        record = {
            "TransactionID": current_id,
            "model_id": model_id,
            "model_predict": model_predict,  # Giá trị 0-1
            "actual_result": actual_result,  # Giá trị 0-1
            "is_correct": is_correct,
            "process_timestamp": datetime.now().isoformat()
        }

        yield record
        i += 1
        current_id += 1
        if current_id > 9999999:
            current_id = 1000000