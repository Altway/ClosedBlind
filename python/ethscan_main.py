import requests
import json
from collections import defaultdict

# Configuration
ETHERSCAN_API_KEY = "66ZT4HPVRR26ZUJTFCUG93AI9YD3KH1E5D"  # Remplace par ta clé API
POOL_ADDRESS = "0xCBCdF9626bC03E24f779434178A73a0B4bad62eD"
ETHERSCAN_BASE_URL = "https://api.etherscan.io/api"


def get_pool_transactions(contract_address):
    """Récupère les transactions associées à un contrat Uniswap."""
    params = {
        "module": "account",
        "action": "tokentx",
        "contractaddress": contract_address,
        "apikey": ETHERSCAN_API_KEY,
    }
    response = requests.get(ETHERSCAN_BASE_URL, params=params)
    data = response.json()
    print(data)
    return data.get("result", [])


def analyze_liquidity_providers(transactions):
    """Analyse les transactions pour déterminer les fournisseurs de liquidité et leurs montants."""
    liquidity_map = defaultdict(float)
    for tx in transactions:
        address = tx["from"].lower()
        value = int(tx["value"]) / (10 ** int(tx["tokenDecimal"]))

        if tx["to"].lower() == POOL_ADDRESS.lower():  # Dépôt de liquidité
            liquidity_map[address] += value
        elif address == POOL_ADDRESS.lower():  # Retrait de liquidité
            recipient = tx["to"].lower()
            liquidity_map[recipient] -= value

    return dict(liquidity_map)


if __name__ == "__main__":
    transactions = get_pool_transactions(POOL_ADDRESS)
    liquidity_distribution = analyze_liquidity_providers(transactions)

    # Afficher la répartition des liquidités
    print(json.dumps(liquidity_distribution, indent=4))
