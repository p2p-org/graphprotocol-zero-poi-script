# graphprotocol-zero-poi-script
Python3 script to close allocations using zero poi for provided subgraph.
You can use this script only if indexer-agent is running in docker container.

## Action which performed by script.
1. Getting env variables (mnemonic, network, ethereum endpoint, postgresql credentials and url) from indexer agent container
2. Get abi from https://gist.github.com/abarmat/b8f4c66b4d0cdd07e121290894f5dbb6
3. Stop indexer agent container
4. Show rules which will be deleted from database. At this step you have 30 seconds to cancel future steps.
5. Delete rules from db.
- **If script fails on this step you need to do next steps manually**
6. Create transaction. You can find txn hash in script's output.
- **If script fails on this step you need to create txn manually using [remix](https://remix.ethereum.org/) or using [etherscan](https://etherscan.io/)**
7. Start indexer agent container.
- **If script fails on this step you need to start indexer agent manually**

## Usage example:
```bash
usage: graphprotocol-zero-poi-script.py [-h] --subgraph_ipfs_hash SUBGRAPH_IPFS_HASH [--indexer_agent_container_name INDEXER_AGENT_CONTAINER_NAME]
                                         [--gas_limit_for_transaction GAS_LIMIT_FOR_TRANSACTION]

optional arguments:
  -h, --help            show this help message and exit
  --subgraph_ipfs_hash SUBGRAPH_IPFS_HASH
                        subraphs ipfs hash to close allocation for
  --indexer_agent_container_name INDEXER_AGENT_CONTAINER_NAME
                        indexer agent container name (default: graph_indexer_agent_1)
  --gas_limit_for_transaction GAS_LIMIT_FOR_TRANSACTION
                        gas limit for transactions (default: 150)
```

## Requirements:

This package need for python psycopg2

```apt-get install libpq-dev```

```pip3 install -r requirements.txt```

If you have problems with psycopg2, you can install psycopg2-binary

```pip3 install psycopg2-binary```


