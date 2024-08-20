from eth_abi import decode
import pandas as pd
from brownie import web3

wl_event_signature = web3.keccak(text='WhitelistToken(address,address,bool)').hex()

def fetch_logs(event_signature, start_block, end_block, contract=None):
    if contract:
        return web3.eth.getLogs({
            'fromBlock': start_block,
            'toBlock': end_block,
            'topics': [event_signature],
            'address': contract
        })
    else:
        return web3.eth.getLogs({
            'fromBlock': start_block,
            'toBlock': end_block,
            'topics': [event_signature]
        })

def fetch_tokens(blk, chain_name):
    while True:
        try:
            if chain_name == 'base':
                results = fetch_logs(wl_event_signature, 0, blk , '0x16613524e02ad97eDfeF371bC883F2F5d6C480A5')
            elif chain_name == 'op':
                results = fetch_logs(wl_event_signature, 0, blk , '0x41C914ee0c7E1A5edCD0295623e6dC557B5aBf3C')
            break
        except:
            continue
    records = []
    for res in results:
        token, wl = decode(['address'], res.topics[2])[0], decode(['bool'], res.topics[3])[0]
        records.append({'token': token, 'wl': wl, 'blk': res['blockNumber']})
    records = pd.DataFrame(records)
    records_grouped = records.loc[records.groupby('token')['blk'].idxmax()]
    records_grouped = records_grouped[records_grouped['wl']]
    return records_grouped['token'].tolist()

