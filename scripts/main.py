from brownie import interface, chain
from scripts.block import get_block
from scripts.tokens import fetch_tokens
import json
import math
from joblib import Parallel, delayed
from tqdm import tqdm
import time
import pandas as pd

def round_significant(x, sig):
    if x == 0:
        return 0
    else:
        return round(x, sig - int(math.floor(math.log10(abs(x)))) - 1)

if chain.id == 10:
    chain_name = 'op'
    oracle = interface.IOracle('0x395942C2049604a314d39F370Dfb8D87AAC89e16')
    data = json.load(open('./config.json'))['velo']
    hours = range(1690952400, 1723788000, 3600)
else:
    chain_name = 'base'
    oracle = interface.IOracle('0xe58920a8c684CD3d6dCaC2a41b12998e4CB17EfE')
    data = json.load(open('./config.json'))['aero']
    hours = range(1693231200, 1723788000, 3600)

connectors, dst = data['connectors'], data['dst']

# This method can detect and bypass any token resulting in pricing errors
def fetch_rates_with_retry(start_i, end_i, src, conn, dst_local, blk, initial_chunk_size, prices, src_fetched):
    chunk_size = initial_chunk_size
    current_start_i = start_i
    while current_start_i < end_i:
        try:
            current_end_i = min(current_start_i + chunk_size, end_i)
            in_connectors = src[current_start_i:current_end_i] + conn + [dst_local]
            prices.extend(oracle.getManyRatesWithConnectors(len(src[current_start_i:current_end_i]), in_connectors, block_identifier=blk))
            src_fetched.extend(src[current_start_i:current_end_i])
            current_start_i = current_end_i  # Move to the next chunk
        except:
            if chunk_size == 1:  # If chunk size is 1, we cannot reduce it further, so break
                print(f"Failed to fetch rates for {src[current_start_i]} at block {blk}")
                current_start_i = current_end_i  # Move to the next part, even if this one failed
                chunk_size = initial_chunk_size // 2  # Rescale chunk size after identifying the error position
            else:
                # Reduce the chunk size and try again
                chunk_size = max(1, chunk_size // 2)
            time.sleep(10)

def run(blk):
    call_length = 150
    src = fetch_tokens(blk, chain_name)
    prices = []
    conn = connectors
    dst_local = dst

    src_fetched = [] 
    if chain_name == 'base':
        # USDT was launched on CL, no price feed on vAMM/sAMM, would error out
        if '0xfde4c96c8593536e31f229ea8f37b2ada2699bb2' in src:
            src_fetched.append('0xfde4c96c8593536e31f229ea8f37b2ada2699bb2')
            prices.append(1e18)
        if blk < 5129324:
            conn = [i for i in conn if i != '0xcfa3ef56d303ae4faaba0592388f19d7c3399fb4']
        # initial liquidity used USDbC
        if blk < 10000000:
            dst_local = '0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA'
            conn = [i for i in conn if i != '0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA']
        src = [i for i in src if i != '0xbd0bd2f620bd3c2d03fefca45ea0abe281965528'  # Not an erc-20
                             and i != '0xfde4c96c8593536e31f229ea8f37b2ada2699bb2'  # USDT
                             ]
    
    if chain_name == 'op':
        # initial liquidity used USDC.e
        if blk < 118035812:
            dst_local = '0x7f5c764cbc14f9669b88837ca1490cca17c31607'
            conn = [i for i in conn if i != '0x7f5c764cbc14f9669b88837ca1490cca17c31607']
        src = [i for i in src if i != '0xeb466342c4d449bc9f53a865d5cb90586f405215'  # axlUSDC
               ]
    
    for start_i in range(0,len(src),call_length):
        retry = 0
        while True:
            original_end_i = start_i + call_length
            try:
                fetch_rates_with_retry(start_i, original_end_i, src, conn, dst_local, blk, call_length, prices, src_fetched)
                break
            except:
                if retry > 2:
                    print(blk, start_i)
                    break
                retry += 1
                time.sleep(1)
    prices = [round_significant(p/1e18, 5) for p in prices]
    out = {a:b for a,b in zip(src_fetched, prices) if b != 0}
    return out

def process_hour(hour):
    blk = get_block(hour, chain_name)
    results = run(blk)
    return results

results = Parallel(n_jobs=320, prefer="threads")(delayed(process_hour)(hour) for hour in tqdm(hours))

out = {}
for hour, ps in zip(hours, results):
    if ps:
        out[hour] = ps

breakpoint()

if chain_name == 'op':  
    pd.DataFrame(out).T.to_csv('op_prices_1723784400.csv')
elif chain_name == 'base':
    pd.DataFrame(out).T.to_csv('base_prices_1723784400.csv')