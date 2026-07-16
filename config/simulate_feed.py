# Databricks notebook source
import shutil, time, os

chunk_dir = "/Volumes/fraud_detection/bronze/raw_source/chunks"
landing_zone = "/Volumes/fraud_detection/bronze/landing_zone"

files = sorted(os.listdir(chunk_dir))
batch_size = 3        # how many files "arrive" per tick
sleep_seconds = 10    # delay between batches

for i in range(0, len(files), batch_size):
    batch = files[i:i + batch_size]
    for f in batch:
        shutil.copy(os.path.join(chunk_dir, f), os.path.join(landing_zone, f))
        print(f"Delivered: {f}")
    time.sleep(sleep_seconds)

print("Feed simulation complete.")