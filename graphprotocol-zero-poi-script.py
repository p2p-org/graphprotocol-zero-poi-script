#!/usr/bin/env python3

import docker
import time
from string import Template
import json
import requests
import argparse
import web3
import psycopg2
import logging
import sys
import base58
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from web3.middleware import geth_poa_middleware


#Map for choose correct contract address for testnet or for mainnet
contract_address_map = {
    'rinkeby': "0x2d44C0e097F6cD0f514edAC633d82E01280B4A5c" ,
    'mainnet': "0xF55041E37E12cD407ad00CE2910B8269B01263b9"
}

# Zero POI string
poi = "0x0000000000000000000000000000000000000000000000000000000000000000"

#Name for indexing rules table
indexing_rules_table_name = '"IndexingRules"'

#Url to Staking smart contract ABI
abi_github_url = "https://gist.githubusercontent.com/abarmat/b8f4c66b4d0cdd07e121290894f5dbb6/raw/17274579de002a7fa33c5111ae5a8e355eab5d81/StakingV2.json"

#Gas limit for smartcontract call
gas_limit = 700000


def to_id(hash: str) -> str:
    if len(hash) == 0:
       raise ValueError("Empty subgraph hash")

    bytes_value = base58.b58decode(hash)
    hex_value = bytes_value.hex()
    return "0x"+hex_value[4:]


def get_rule_from_db(db_name: str, db_user: str, db_password: str, db_host: str, subgraph_deployment_id: str) -> str:
    if len(db_name) == 0:
        raise ValueError("db_name is empty")
    if len(db_user) == 0:
        raise ValueError("db_user is empty")
    if len(db_password) == 0:
        raise ValueError("db_password is empty")
    if len(db_host) == 0:
        raise ValueError("db_host is empty")
    if len(subgraph_deployment_id) == 0:
        raise ValueError("subgraph_deployment_id is empty")

    db = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host)
    cursor = db.cursor()
    cursor.execute("SELECT * FROM {0} WHERE deployment = '{1}';".format(indexing_rules_table_name, subgraph_deployment_id))
    rules_to_delete=cursor.fetchall()
    cursor.close()
    db.close()
    return rules_to_delete


def remove_rule_from_db(db_name: str, db_user: str, db_password: str, db_host: str, subgraph_deployment_id: str) -> str:
    if len(db_name) == 0:
        raise ValueError("db_name is empty")
    if len(db_user) == 0:
        raise ValueError("db_user is empty")
    if len(db_password) == 0:
        raise ValueError("db_password is empty")
    if len(db_host) == 0:
        raise ValueError("db_host is empty")
    if len(subgraph_deployment_id) == 0:
        raise ValueError("subgraph_deployment_id is empty")

    db = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host)
    cursor = db.cursor()
    cursor.execute("DELETE FROM {0} WHERE deployment = '{1}';".format(indexing_rules_table_name, subgraph_deployment_id))
    #Verify if rows was deleted
    cursor.execute("SELECT * FROM {0} WHERE deployment = '{1}';".format(indexing_rules_table_name, subgraph_deployment_id))
    rows=cursor.fetchall()
    if len(rows) != 0:
        raise ValueError("Failed to remove data from db")
    #Commit transaction
    db.commit()
    cursor.close()
    db.close()
    return


def get_contract_address(network: str) -> str:
    return contract_address_map[network]


def get_contract_abi_from_github() -> dict:
    logger.debug("Get abi from github")

    #https://stackoverflow.com/questions/15431044/can-i-set-max-retries-for-requests-request
    s = requests.Session()

    retries = Retry(total=6,
                    backoff_factor=0.1,
                    status_forcelist=[ 500, 502, 503, 504, 404 ],
                    allowed_methods=frozenset(['GET', 'POST']))
    s.mount('http://', HTTPAdapter(max_retries=retries))

    request = s.get(abi_github_url)

    if request.status_code != 200:
        raise ValueError("Status code: {0}\nResponse: {1}".format(request.status_code,request.text))

    json_response = json.loads(request.text)

    return json_response


def wait_for_txns(txns: list, ethereum_rpc: str) -> list:
    if len(txns) == 0:
        raise ValueError("Txn list is empty")
    if len(ethereum_rpc) == 0:
        raise ValueError("Ethereum rpc is empty")

    failed_txns = []

    w3 = web3.Web3(web3.Web3.HTTPProvider(ethereum_rpc))

    for txn in txns:
        receipt = w3.eth.waitForTransactionReceipt(txn)
        if receipt.status == 0:
            failed_txns.append(txn)
    return failed_txns


def create_txn(mnemonic: str, allocations: dict, poi: str, ethereum_rpc: str, contract_address: str, abi: dict, gas_limit_for_transaction: int) -> list:
    if len(mnemonic) == 0:
        raise ValueError("Mnemonic is empty")
    if len(allocations) == 0:
        raise ValueError("Allocation is empty")
    if len(poi) == 0:
        raise ValueError("Poi is empty")
    if len(ethereum_rpc) == 0:
        raise ValueError("ethereum_rpc is empty")
    if len(contract_address) == 0:
        raise ValueError("contract_address is empty")
    if len(abi) == 0:
        raise ValueError("contract_address is empty")
    if gas_limit_for_transaction <= 0:
        raise ValueError("gas_limit is zero or below")

    txns=[]

    w3 = web3.Web3(web3.Web3.HTTPProvider(ethereum_rpc))

    #https://stackoverflow.com/questions/68449832/web3-extradatalength-error-on-the-binance-smart-chain-using-python
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    web3.eth.Account.enable_unaudited_hdwallet_features()
    wallet = w3.eth.account.from_mnemonic(mnemonic)

    contract_address = w3.toChecksumAddress(contract_address.lower())
    address = wallet.address

    nonce = w3.eth.getTransactionCount(address)

    contract = w3.eth.contract(address=contract_address, abi=abi)

    gas_price = w3.eth.gas_price

    max_priority_fee = w3.eth.max_priority_fee

    if gas_price > w3.toWei(gas_limit_for_transaction, 'gwei'):
        raise ValueError("Current gas prise {0} is above --gas_limit_for_transaction".format(gas_price))

    for allocation in allocations:
        txn = contract.functions.closeAllocation(w3.toChecksumAddress(allocation["id"]), poi).buildTransaction({
            'gas': gas_limit,
            'maxPriorityFeePerGas': max_priority_fee,
            'from': address,
            'nonce': nonce,
            'type': 2
            })
        signed_txn = wallet.sign_transaction(txn)
        txn = w3.eth.send_raw_transaction(signed_txn.rawTransaction)
        txns.append(txn.hex())
    return txns


def get_allocation_id(indexer_address: str, deployment: str, graphql_endpoint: str) -> list:
    if len(indexer_address) == 0:
        raise ValueError("Indexer_address is empty")
    if len(deployment) == 0:
        raise ValueError("Deployment is empty")
    if len(graphql_endpoint) == 0:
        raise ValueError("Graphql endpoint is empty")

    t = Template("""query MyQuery {
        indexers(where: {id: "$indexer_address"}) {
          allocations(where: {subgraphDeployment: "$deployment", status: Active}) {
            id
          }
        }
       }""")
    query_data = t.substitute(indexer_address=indexer_address.lower(),
                              deployment=deployment)

    #https://stackoverflow.com/questions/15431044/can-i-set-max-retries-for-requests-request
    s = requests.Session()

    retries = Retry(total=6,
                    backoff_factor=0.1,
                    status_forcelist=[ 500, 502, 503, 504, 404 ],
                    allowed_methods=frozenset(['GET', 'POST']))
    s.mount('http://', HTTPAdapter(max_retries=retries))

    request = s.post(graphql_endpoint, json={'query': query_data})

    if request.status_code != 200:
        raise ValueError("Status code: {0}\nResponse: {1}".format(request.status_code,request.text))

    json_response = json.loads(request.text)

    if "errors" in json_response:
        raise ValueError("Response: {1}".format(request.text))

    return json_response["data"]["indexers"][0]["allocations"]


def get_env_vars_from_container(container_name: str) -> dict:
    if len(container_name) == 0:
        raise ValueError("Empty container name")

    env_dict = {}
    client = docker.from_env()
    agent_container = client.containers.get(container_name)
    # Get env variable from agent container to have postgres url password and mnemonic
    env_array = agent_container.exec_run(cmd="env").output.decode("utf-8").split("\n")
    # Remove last empty element
    env_array.pop()
    for i in env_array:
        key_values=i.split("=")
        env_dict[key_values[0]] = key_values[1]
    return env_dict


def stop_agent_container(container_name: str) -> None:
    if len(container_name) == 0:
        raise ValueError("Empty container name")

    client = docker.from_env()
    agent_container = client.containers.get(container_name)
    if agent_container.status == "exited":
        return
    agent_container.stop()
    return


def start_agent_container(container_name: str) -> None:
    if len(container_name) == 0:
        raise ValueError("Empty container name")

    client = docker.from_env()
    agent_container = client.containers.get(container_name)
    if agent_container.status == "running":
        return
    agent_container.start()
    return


if __name__ == "__main__":
    #Log level for script
    log_level="INFO" #Can be DEBUG, INFO, WARNING, ERROR, CRITICAL

    logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=log_level)
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument('--subgraph_ipfs_hash',
        help='subraphs ipfs hash to close allocation for',
        required=True,
        type=str)
    parser.add_argument('--indexer_agent_container_name',
        help='indexer agent container name (default: %(default)s)',
        default='graph_indexer_agent_1',
        type=str)
    parser.add_argument('--gas_limit_for_transaction',
        help='gas limit for transactions (default: %(default)s)',
        default=150,
        type=int)
    args = parser.parse_args()

    subgraph_ipfs_hash = args.subgraph_ipfs_hash
    indexer_agent_container_name = args.indexer_agent_container_name
    gas_limit_for_transaction = args.gas_limit_for_transaction

    try:
        logger.info("Get deployment id from ipfs hash for subgraph: {0}".format(subgraph_ipfs_hash))
        logger.debug("Function to_id({0})".format(subgraph_ipfs_hash))
        subgraph_deployment_id = to_id(subgraph_ipfs_hash)
    except:
        logger.critical("Failed to get deployment id from ipfs hash for subgraph: {0}".format(subgraph_ipfs_hash))
        logging.exception("Exception: ")
        sys.exit(1)

    try:
        logger.info("Get env vars from container: {0}".format(indexer_agent_container_name))
        logger.debug("Function get_env_vars_from_container({0})".format(indexer_agent_container_name))
        agent_vars = get_env_vars_from_container(indexer_agent_container_name)
        if len(agent_vars) == 0:
           raise ValueError("Get empty dict env vars from container: {0}".format(indexer_agent_container_name))
    except:
        logger.critical("Failed to get env vars from conatiner: {0}".format(indexer_agent_container_name))
        logging.exception("Exception: ")
        sys.exit(1)

    try:
        logger.info("Get Staking abi url: {0}".format(abi_github_url))
        logger.debug("Function get_contract_abi_from_github {0}".format(abi_github_url))
        abi = get_contract_abi_from_github()
        if len(abi) == 0:
           raise ValueError("Get empty abi from url: {0}".format(abi_github_url))
    except:
        logger.critical("Failed to get env vars from conatiner: {0}".format(indexer_agent_container_name))
        logging.exception("Exception: ")
        sys.exit(1)

    try:
        logger.info("Get allocations to close")
        logger.debug("Function get_allocation_id({0}, {1}, {2})".format(agent_vars["INDEXER_AGENT_INDEXER_ADDRESS"], subgraph_deployment_id, agent_vars["INDEXER_AGENT_NETWORK_SUBGRAPH_ENDPOINT"]))
        allocations = get_allocation_id(agent_vars["INDEXER_AGENT_INDEXER_ADDRESS"], subgraph_deployment_id, agent_vars["INDEXER_AGENT_NETWORK_SUBGRAPH_ENDPOINT"])
        if len(allocations) == 0:
          logger.info("No active allocations for subgraph: {0}".format(subgraph_ipfs_hash))
          sys.exit(0)
    except:
        logger.critical("Failed to get allocations to close")
        logging.exception("Exception: ")
        sys.exit(1)

    try:
        logger.info("Get Staking Smart Contract address for network: {0}".format(agent_vars["INDEXER_AGENT_ETHEREUM_NETWORK"]))
        logger.debug("Function get_contract_address({0})".format(agent_vars["INDEXER_AGENT_ETHEREUM_NETWORK"]))
        contract_address=get_contract_address(agent_vars["INDEXER_AGENT_ETHEREUM_NETWORK"])
    except:
        logger.critical("Failed to get allocations to close")
        logging.exception("Exception: ")
        sys.exit(1)

    try:
        logger.info("Get rules to delete")
        logger.debug("Function get_rule_from_db({0},{1},{2},{3},{4})".format(agent_vars["INDEXER_AGENT_POSTGRES_DATABASE"], agent_vars["SERVER_DB_USER"],
                            agent_vars["SERVER_DB_PASSWORD"], agent_vars["INDEXER_AGENT_POSTGRES_HOST"],
                            subgraph_deployment_id))
        rules_to_delete = get_rule_from_db(agent_vars["INDEXER_AGENT_POSTGRES_DATABASE"], agent_vars["SERVER_DB_USER"],
                            agent_vars["SERVER_DB_PASSWORD"], agent_vars["INDEXER_AGENT_POSTGRES_HOST"],
                            subgraph_deployment_id)
        logger.info("Script will create {0} txns for close all active allocations\nAllocations: {1}".format(len(allocations),allocations))
        logger.info("Script will delete {1} rules:\n{0}\nIf you make a mistake you have 30 seconds to press CTRL+C.".format(rules_to_delete, len(rules_to_delete)))
        if len(rules_to_delete) == 0:
            raise ValueError("No rules for {0} in db".format(subgraph_ipfs_hash))
        time.sleep(15)
    except:
        logger.critical("Failed to get rules from db")
        logging.exception("Exception: ")
        sys.exit(0)

    try:
        logger.info("Stop agent container: {0}".format(indexer_agent_container_name))
        logger.debug("Function stop_agent_container({0})".format(indexer_agent_container_name))
        stop_agent_container(indexer_agent_container_name)
    except:
        logger.critical("Failed to stop agent container: {0}".format(indexer_agent_container_name))
        logging.exception("Exception: ")
        sys.exit(1)

    try:
        logger.info("Remove all rules for subgraph from db".format(subgraph_ipfs_hash))
        logger.debug("Function remove_rule_from_db({0},{1},{2},{3},{4})".format(agent_vars["INDEXER_AGENT_POSTGRES_DATABASE"], agent_vars["SERVER_DB_USER"],
                                                                agent_vars["SERVER_DB_PASSWORD"], agent_vars["INDEXER_AGENT_POSTGRES_HOST"],
                                                                subgraph_deployment_id))

        remove_rule_from_db(agent_vars["INDEXER_AGENT_POSTGRES_DATABASE"], agent_vars["SERVER_DB_USER"],
                            agent_vars["SERVER_DB_PASSWORD"], agent_vars["INDEXER_AGENT_POSTGRES_HOST"],
                            subgraph_deployment_id)
    except:
        logger.critical("Failed remove rules for subgraph: {0}".format(subgraph_ipfs_hash))
        logging.exception("Exception: ")
        sys.exit(1)

    try:
        logger.info("Script will create {0} txns for close all active allocations\nAllocations: {1}".format(len(allocations),allocations))
        logger.debug("Function create_txn({0},{1},{2},{3},{4},{5},{6})".format(agent_vars["INDEXER_AGENT_MNEMONIC"], allocations, poi, agent_vars["INDEXER_AGENT_ETHEREUM"], contract_address, abi, gas_limit_for_transaction))
        txns=create_txn(agent_vars["INDEXER_AGENT_MNEMONIC"], allocations, poi, agent_vars["INDEXER_AGENT_ETHEREUM"], contract_address, abi, gas_limit_for_transaction)
        logger.info("Txns: {0}".format(txns))
    except:
        logger.critical("Failed to create txn")
        logging.exception("Exception: ")
        logger.critical("Don't forget to close allocation manually or to add rule to db again and start indexer agent container")
        sys.exit(1)

    try:
        logger.info("Wait for txns to be included in block:\nTxn list {0}".format(txns))
        logger.debug("Function wait_for_txns({0},{1})".format(txns, agent_vars["INDEXER_AGENT_ETHEREUM"]))
        failed_txns = wait_for_txns(txns, agent_vars["INDEXER_AGENT_ETHEREUM"])
        if len(failed_txns) > 0:
            raise ValueError("One or more failed txns\nFailed txns list: {0}".format(failed_txns))
        logger.info("Wait for block to be confirmed, overwise after starting indexer agent he will create another txns for closing allocations")
        time.sleep(180)
    except:
        logger.critical("Txns failed to execute")
        logging.exception("Exception: ")
        logger.critical("Don't forget to close allocation manually or to add rule to db again and start indexer agent container")
        sys.exit(1)

    try:
        logger.info("Start agent container: {0}".format(indexer_agent_container_name))
        logger.debug("Function start_agent_container({0})".format(indexer_agent_container_name))
        start_agent_container(indexer_agent_container_name)
    except:
        logger.critical("Failed to start agent container: {0}".format(indexer_agent_container_name))
        logging.exception("Exception: ")
        logging.critical("Failed to start agent container. Don't forget to start it manually.")
        sys.exit(1)

