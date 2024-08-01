import asyncio
import websockets
import json
import time
import datetime
import csv

NEW_TOKEN_HEADER = ['signature', 'mint', 'traderPublicKey', 'txType', 'initialBuy', 'bondingCurveKey', 'vTokensInBondingCurve', 'vSolInBondingCurve',
                    'marketCapSol', 'name', 'symbol', 'uri', 'ts']
TOKEN_TRADE_HEADER = ['signature', 'mint', 'traderPublicKey', 'txType', 'tokenAmount', 'newTokenBalance', 'bondingCurveKey', 'vTokensInBondingCurve',
                      'vSolInBondingCurve', 'marketCapSol', 'ts'] 
HEADER = list(set(NEW_TOKEN_HEADER) | set(TOKEN_TRADE_HEADER))

def reset_data(data_file, name):
    data_file.close()
    data_file = open(f'./stream/{name}.csv', 'w', newline='', encoding='utf-8')
    csv_writer = csv.DictWriter(data_file, HEADER)
    csv_writer.writeheader()
    return data_file, csv_writer


async def subscribe():
    uri = "wss://pumpportal.fun/api/data"
    async with websockets.connect(uri) as websocket:
        today = datetime.datetime.today().strftime('%Y%m%d')
        data_file = open(f'./stream/stream_{today}.csv', 'w', newline='', encoding='utf-8')
        csv_writer = csv.DictWriter(data_file, HEADER)
        csv_writer.writeheader()

        # Subscribing to token creation events
        payload = {
            "method": "subscribeNewToken",
        }
        await websocket.send(json.dumps(payload))

        async for message in websocket:
            ts = time.time_ns()
            datapoint = json.loads(message)
            datapoint['ts'] = ts

            if 'txType' in datapoint:
                if datapoint['txType'] == 'create':
                    payload = {
                        "method": "subscribeTokenTrade",
                        "keys": [datapoint['mint']]
                    }
                    await websocket.send(json.dumps(payload))
                csv_writer.writerow(datapoint)

            if datetime.datetime.today().strftime('%Y%m%d') != today:
                data_file, csv_writer = reset_data(data_file, f"stream_{datetime.datetime.today().strftime('%Y%m%d')}")
                today = datetime.datetime.today().strftime('%Y%m%d')
        

# Run the subscribe function
obj = time.gmtime(0)  
epoch = time.asctime(obj)  
print("epoch is:", epoch)  
asyncio.get_event_loop().run_until_complete(subscribe())