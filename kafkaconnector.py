#!/usr/bin/env python3
"""
Update Confluent Cloud Debezium MySQL CDC v2 connector offsets.

Usage:
  ./kafkaconnector.py --pause
  ./kafkaconnector.py --update-offset
  ./kafkaconnector.py --resume

You can combine them:
  ./kafkaconnector.py --pause --update-offset --resume

Prereqs:
  pip install requests PyMySQL python-dotenv

Env vars required:
  MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD
  CFLT_API_KEY, CFLT_API_SECRET, CFLT_ENV_ID, CFLT_CLUSTER_ID
"""

import argparse
import json
import os
import time
import base64
import requests
import pymysql
from loguru import logger

# Configure loguru
logger.remove()  # Remove default handler
logger.add(
    lambda msg: print(msg, end=""),  # Use print for stdout
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO"
)

API_BASE = "https://api.confluent.cloud"
TIMEOUT_S = 30


def b64_basic(api_key, api_secret):
    token = f"{api_key}:{api_secret}".encode()
    return "Basic " + base64.b64encode(token).decode()


def ccloud_headers():
    return {
        "Authorization": b64_basic(
            os.environ["CFLT_API_KEY"],
            os.environ["CFLT_API_SECRET"]),
        "Content-Type": "application/json"}


def connect_mysql():
    return pymysql.connect(
        host=os.environ["MYSQL_HOST"],
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        charset="utf8mb4",
        autocommit=True
    )


def get_latest_binlog(conn):
    with conn.cursor() as cur:
        cur.execute("SHOW MASTER STATUS")
        row = cur.fetchone()
        if not row:
            raise RuntimeError(
                "SHOW MASTER STATUS returned no rows. Is binlog enabled?")
        return row[0], int(row[1])  # file, pos


def get_offsets(env_id, lkc_id, name):
    url = f"{API_BASE}/connect/v1/environments/{env_id}/clusters/{lkc_id}/connectors/{name}/offsets"
    r = requests.get(url, headers=ccloud_headers(), timeout=TIMEOUT_S)
    r.raise_for_status()
    return r.json()


def request_offset_patch(env_id, lkc_id, name, offsets_payload):
    url = f"{API_BASE}/connect/v1/environments/{env_id}/clusters/{lkc_id}/connectors/{name}/offsets/request"
    r = requests.post(url, headers=ccloud_headers(), data=json.dumps({
        "type": "PATCH",
        "offsets": offsets_payload
    }), timeout=TIMEOUT_S)
    if r.status_code not in (200, 202):
        r.raise_for_status()
    return r.json()


def get_offset_request_status(env_id, lkc_id, name):
    url = f"{API_BASE}/connect/v1/environments/{env_id}/clusters/{lkc_id}/connectors/{name}/offsets/request/status"
    r = requests.get(url, headers=ccloud_headers(), timeout=TIMEOUT_S)
    r.raise_for_status()
    return r.json()


def pause_connector(env_id, lkc_id, name):
    url = f"{API_BASE}/connect/v1/environments/{env_id}/clusters/{lkc_id}/connectors/{name}/pause"
    r = requests.put(url, headers=ccloud_headers(), timeout=TIMEOUT_S)
    if r.status_code not in (200, 202, 204):
        r.raise_for_status()


def resume_connector(env_id, lkc_id, name):
    url = f"{API_BASE}/connect/v1/environments/{env_id}/clusters/{lkc_id}/connectors/{name}/resume"
    r = requests.put(url, headers=ccloud_headers(), timeout=TIMEOUT_S)
    if r.status_code not in (200, 202, 204):
        r.raise_for_status()


def update_offset(env_id, lkc_id, name):
    # Read latest MySQL binlog
    with connect_mysql() as conn:
        file_, pos = get_latest_binlog(conn)
    logger.info(f"Latest MySQL binlog: file={file_}, pos={pos}")

    # Fetch current connector offsets
    current = get_offsets(env_id, lkc_id, name)
    existing = current.get("offsets", [])
    if not existing:
        raise RuntimeError("Connector has no offsets yet; cannot patch.")

    partition = existing[0]["partition"]
    old_offset = existing[0]["offset"]

    new_offset = dict(old_offset)
    new_offset["file"] = file_
    new_offset["pos"] = pos
    new_offset.pop("row", None)
    new_offset.pop("rowsToSkip", None)
    if "snapshot" in new_offset:
        new_offset["snapshot"] = "false"

    patch_offsets = [{
        "partition": partition,
        "offset": new_offset
    }]

    logger.info(f"Submitting connector {name} offset PATCH…")
    resp = request_offset_patch(env_id, lkc_id, name, patch_offsets)
    logger.info(
        f"Connector {name} offset request accepted: "
        f"{json.dumps(resp, indent=2)}")

    for _ in range(30):
        time.sleep(2)
        status = get_offset_request_status(env_id, lkc_id, name)
        state = (
            (status.get("status") or {}).get(
                "phase",
                "") if isinstance(
                status.get("status"),
                dict) else (
                status.get("status") if isinstance(
                    status.get("status"),
                    str) else status.get(
                        "phase",
                    "")))

        logger.debug("Status: " + json.dumps(status, indent=2))
        if str(state).upper() in ("SUCCEEDED", "FAILED"):
            break


def main():

    parser = argparse.ArgumentParser(
        description="Manage Confluent Cloud connector offsets")
    parser.add_argument(
        "--pause",
        action="store_true",
        help="Pause the connector")
    parser.add_argument(
        "--update-offset",
        action="store_true",
        help="Update offset to latest MySQL binlog")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume the connector")
    args = parser.parse_args()

    env_id = os.environ.get("CFLT_ENV_ID", "env-8qo5q")
    lkc_id = os.environ.get("CFLT_CLUSTER_ID", "lkc-ypyo6")
    connectors = json.loads(os.environ.get("CFLT_CONNS"))
    # ["czen-outbox-mysql-cdcs-v2", "czen-member-outbox-mysql-cdcs"]

    for name in connectors:
        if args.pause:
            logger.info(f"Pausing connector {name} …")
            pause_connector(env_id, lkc_id, name)

        if args.update_offset:
            update_offset(env_id, lkc_id, name)

        if args.resume:
            logger.info(f"Resuming connector {name} …")
            resume_connector(env_id, lkc_id, name)


if __name__ == "__main__":
    main()
