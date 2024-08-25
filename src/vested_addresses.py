import requests
import csv
import time
import logging
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import RequestException

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CHECKPOINT_FILE = 'checkpoint.json'
OUTPUT_FILE = 'data/vested_addresses.csv'
BATCH_SIZE = 100

def load_checkpoint():
    logging.info("Loading checkpoint")
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoint = json.load(f)
            logging.info(f"Checkpoint loaded. Resuming from offset {checkpoint['offset']}")
            return checkpoint
    logging.info("No checkpoint found. Starting from the beginning")
    return {'offset': 0}

def save_checkpoint(offset):
    logging.info(f"Saving checkpoint with offset {offset}")
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({'offset': offset}, f)
    logging.info("Checkpoint saved")

def get_addresses_batch(offset):
    logging.info(f"Fetching addresses batch. Offset: {offset}, Batch size: {BATCH_SIZE}")
    try:
        response = requests.get(
            f"https://api.celenium.io/v1/address?limit={BATCH_SIZE}&offset={offset}",
            timeout=10
        )
        response.raise_for_status()
        addresses = response.json()
        logging.info(f"Successfully fetched {len(addresses)} addresses")
        return addresses
    except RequestException as e:
        logging.error(f"Error fetching addresses: {e}")
        return None

def get_vesting_for_address(address):
    logging.debug(f"Fetching vesting for address: {address['hash']}")
    try:
        response = requests.get(
            f"https://api.celenium.io/v1/address/{address['hash']}/vestings",
            timeout=10
        )
        response.raise_for_status()
        vestings = response.json()
        
        time.sleep(0.33)  # Respect rate limit of 3 requests per second
        
        logging.debug(f"Successfully fetched vesting for address: {address['hash']}")
        return address['hash'], vestings
    except RequestException as e:
        logging.error(f"Error fetching vesting for address {address['hash']}: {e}")
        return address['hash'], None

def process_addresses_batch(addresses):
    logging.info(f"Processing batch of {len(addresses)} addresses")
    vested_addresses = []
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_address = {executor.submit(get_vesting_for_address, address): address for address in addresses}
        for future in as_completed(future_to_address):
            address_hash, vestings = future.result()
            if vestings:
                for vesting in vestings:
                    vested_addresses.append({
                        'address': address_hash,
                        'amount': vesting.get('amount'),
                        'end_time': vesting.get('end_time'),
                        'hash': vesting.get('hash'),
                        'height': vesting.get('height'),
                        'id': vesting.get('id'),
                        'start_time': vesting.get('start_time'),
                        'time': vesting.get('time'),
                        'type': vesting.get('type')
                    })
                    logging.info(f"Found vesting for address: {address_hash}, Amount: {vesting.get('amount')}")
    
    logging.info(f"Batch processing complete. Found {len(vested_addresses)} vesting entries")
    return vested_addresses

def write_to_csv(vested_addresses):
    logging.info(f"Writing {len(vested_addresses)} vesting entries to CSV")
    mode = 'a' if os.path.exists(OUTPUT_FILE) else 'w'
    with open(OUTPUT_FILE, mode, newline='') as csvfile:
        fieldnames = ['address', 'amount', 'end_time', 'hash', 'height', 'id', 'start_time', 'time', 'type']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if mode == 'w':
            writer.writeheader()
            logging.info("Created new CSV file and wrote header")
        for row in vested_addresses:
            writer.writerow(row)
    logging.info(f"Finished writing to CSV. File: {OUTPUT_FILE}")

def process_all_addresses():
    logging.info("Starting to process all addresses")
    checkpoint = load_checkpoint()
    offset = checkpoint['offset']

    try:
        while True:
            logging.info(f"Processing batch starting at offset {offset}")
            addresses = get_addresses_batch(offset)
            
            if not addresses:
                logging.info("No more addresses to process. Finishing up.")
                break
            
            vested_addresses = process_addresses_batch(addresses)
            
            if vested_addresses:
                write_to_csv(vested_addresses)
                logging.info(f"Wrote {len(vested_addresses)} vesting entries to CSV")
            else:
                logging.info("No vesting entries found in this batch")
            
            offset += BATCH_SIZE
            save_checkpoint(offset)
            
            logging.info(f"Completed processing batch. Next offset: {offset}")
            time.sleep(0.33)  # Respect rate limit for the next batch request
        
        logging.info(f"Completed processing all addresses. Final CSV file: {OUTPUT_FILE}")
    except Exception as e:
        logging.error(f"Error in process_all_addresses: {e}")
        raise

if __name__ == "__main__":
    logging.info("Script started")
    process_all_addresses()
    logging.info("Script completed")