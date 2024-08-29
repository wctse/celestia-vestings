import csv
import time
import logging
import json
import os
import requests
from requests.exceptions import RequestException
from time import sleep

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

INPUT_FILE = 'data/vested_addresses.csv'
TRANSACTIONS_OUTPUT_FILE = 'data/withdrawal_transactions.csv'
SUMMARY_OUTPUT_FILE = 'data/withdrawal_summary.csv'
CHECKPOINT_FILE = 'withdrawal_checkpoint.json'
BATCH_SIZE = 100
RATE_LIMIT_DELAY = 1/3  # 1/3 second delay to respect 3 calls per second limit

def load_checkpoint():
    logging.info("Loading checkpoint")
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoint = json.load(f)
            logging.info(f"Checkpoint loaded. Last processed row: {checkpoint['last_processed_row']}")
            return checkpoint
    logging.info("No checkpoint found. Starting from the beginning.")
    return {'last_processed_row': -1}

def save_checkpoint(last_processed_row):
    logging.info(f"Saving checkpoint. Last processed row: {last_processed_row}")
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({'last_processed_row': last_processed_row}, f)

def get_transactions(address, offset=0):
    logging.info(f"Fetching transactions for address {address}, offset {offset}")
    while True:
        try:
            response = requests.get(
                f"https://api.celenium.io/v1/address/{address}/txs",
                params={
                    'limit': BATCH_SIZE,
                    'offset': offset,
                    'msg_type': 'MsgWithdrawDelegatorReward'
                },
                timeout=10
            )
            response.raise_for_status()
            transactions = response.json()
            logging.info(f"Fetched {len(transactions)} transactions for address {address}")
            time.sleep(0.33)  # Respect rate limit
            return transactions
        except RequestException as e:
            logging.error(f"Error fetching transactions for address {address}: {e}")
            time.sleep(5)  # Wait before retrying

def get_transaction_events(tx_hash):
    while True:
        try:
            response = requests.get(
                f"https://api.celenium.io/v1/tx/{tx_hash}/events",
                params={'limit': 100},  # Assuming max 100 events per transaction
                timeout=10
            )
            response.raise_for_status()
            events = response.json()
            time.sleep(0.33)  # Respect rate limit
            return events
        except RequestException as e:
            logging.error(f"Error fetching events for transaction {tx_hash}: {e}")
            time.sleep(5)  # Wait before retrying

def process_address(row_number, address):
    logging.info(f"Processing row {row_number}, address: {address}")
    all_transactions = []
    offset = 0
    total_withdrawn = 0
    withdraw_count = 0

    while True:
        transactions = get_transactions(address, offset)
        if not transactions:
            logging.info(f"No more transactions for address {address}")
            break

        for tx in transactions:
            events = get_transaction_events(tx['hash'])
            withdraw_amount = sum(
                int(event['data'].get('amount', '0').rstrip('utia'))
                for event in events
                if event['type'] == 'withdraw_rewards' and event['data'].get('amount', '').endswith('utia')
            )
            tx['withdraw_amount'] = withdraw_amount
            tx['address'] = address  # Add address to transaction data
            all_transactions.append(tx)
            total_withdrawn += withdraw_amount
            withdraw_count += 1
            logging.info(f"Processed transaction {tx['hash']} for address {address}. Withdraw amount: {withdraw_amount}")

        offset += len(transactions)
        if len(transactions) < BATCH_SIZE:
            logging.info(f"Reached end of transactions for address {address}")
            break

    logging.info(f"Completed processing row {row_number}, address {address}. Total withdrawals: {withdraw_count}, Total amount: {total_withdrawn}")
    return row_number, address, all_transactions, withdraw_count, total_withdrawn

def write_transactions_to_csv(transactions):
    if not transactions:
        logging.info("No transactions to write")
        return

    logging.info(f"Writing {len(transactions)} transactions to CSV")
    mode = 'a' if os.path.exists(TRANSACTIONS_OUTPUT_FILE) else 'w'
    fieldnames = ['id', 'height', 'position', 'gas_wanted', 'gas_used', 'timeout_height', 'events_count', 
                  'messages_count', 'hash', 'fee', 'time', 'message_types', 'status', 'withdraw_amount', 'address']
    
    with open(TRANSACTIONS_OUTPUT_FILE, mode, newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if csvfile.tell() == 0:
            writer.writeheader()
        for tx in transactions:
            tx['hash'] = tx['hash'].upper()
            # Create a new dict with only the specified fields
            filtered_tx = {field: tx.get(field, '') for field in fieldnames}
            writer.writerow(filtered_tx)

def write_summary_to_csv(summary):
    logging.info(f"Writing summary for address {summary['address']} to CSV")
    mode = 'a' if os.path.exists(SUMMARY_OUTPUT_FILE) else 'w'
    with open(SUMMARY_OUTPUT_FILE, mode, newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['address', 'withdraw_count', 'sum_withdrawn_amount'])
        if csvfile.tell() == 0:
            writer.writeheader()
        writer.writerow(summary)
    logging.info("Finished writing summary to CSV")

def process_all_addresses():
    checkpoint = load_checkpoint()
    last_processed_row = checkpoint['last_processed_row']

    logging.info("Reading addresses from input file")
    with open(INPUT_FILE, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        addresses = list(reader)
    logging.info(f"Found {len(addresses)} addresses in total")

    for i, address_data in enumerate(addresses):
        if i <= last_processed_row:
            continue
        
        row_number, address, transactions, withdraw_count, total_withdrawn = process_address(i, address_data['address'])
        
        if transactions:
            write_transactions_to_csv(transactions)
        
        write_summary_to_csv({
            'address': address,
            'withdraw_count': withdraw_count,
            'sum_withdrawn_amount': total_withdrawn
        })
        
        save_checkpoint(row_number)
        logging.info(f"Completed processing row {row_number}, address: {address}, Withdrawals: {withdraw_count}, Total amount: {total_withdrawn}")
        
        sleep(RATE_LIMIT_DELAY)  # Respect rate limit between address processing

if __name__ == "__main__":
    logging.info("Starting withdrawal processing")
    process_all_addresses()
    logging.info("Withdrawal processing completed")