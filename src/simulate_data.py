"""
Simulador de ingesta de datos para el pipeline.
Genera un fichero JSON con líneas (JSON Lines) en data/raw_tweets.json por defecto.
"""
import os
import argparse
import random
from datetime import datetime, timedelta
import pandas as pd

def generate_simulated_data(num_records=1000, out_path="data/raw_tweets.json"):
    airlines = ["AerolineaA", "AerolineaB", "AerolineaC", "AerolineaD"]
    locations = ["New York", "Los Angeles", "Chicago", "Miami", "Desconocido"]

    positive_texts = [
        "Great flight experience! Highly recommend.",
        "On time and excellent service.",
        "Loved the new seats, comfortable trip.",
        "The staff was very helpful and friendly."
    ]
    negative_texts = [
        "Worst delay ever. Missed my connection.",
        "Lost my luggage, terrible service.",
        "Rude crew and dirty plane.",
        "Never flying with them again, total disaster."
    ]
    neutral_texts = [
        "Flight 789 is currently boarding.",
        "Checking in for my trip today.",
        "Waiting at the gate."
    ]

    data = []
    start_date = datetime.now() - timedelta(days=30)

    for i in range(1, num_records + 1):
        airline = random.choice(airlines)
        tweet_date = start_date + timedelta(hours=i * 0.75)
        sentiment_choice = random.choices(['positive', 'negative', 'neutral'], weights=[40, 40, 20], k=1)[0]

        if sentiment_choice == 'negative':
            text = random.choice(negative_texts)
            neg_reason = random.choice(['Late Flight', 'Customer Service', 'Lost Luggage'])
        elif sentiment_choice == 'positive':
            text = random.choice(positive_texts)
            neg_reason = None
        else:
            text = random.choice(neutral_texts)
            neg_reason = None

        data.append({
            "tweet_id": i,
            "airline": airline,
            "text": text,
            "tweet_date": tweet_date.strftime("%Y-%m-%d %H:%M:%S"),
            "user_location": random.choice(locations),
            "negativereason": neg_reason,
            "raw_sentiment": sentiment_choice
        })

    out_dir = os.path.dirname(out_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df = pd.DataFrame(data)
    df.to_json(out_path, orient='records', lines=True)
    print(f"Simulación completa. {num_records} registros guardados en {out_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generador de datos simulados para pipeline")
    parser.add_argument("--num-records", type=int, default=1000, help="Número de registros a generar")
    parser.add_argument("--out", type=str, default="data/raw_tweets.json", help="Ruta de salida (JSON Lines)")
    args = parser.parse_args()
    generate_simulated_data(num_records=args.num_records, out_path=args.out)
