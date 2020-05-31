import os
import time
import threading
import logging
import requests
import pandas as pd


URL_BASE = 'https://api.kraken.com'
API_KEY = os.environ['KRAKEN_KEY']


PAIRS = [
    'USDTZUSD',
    'USDCUSD'
]


FORMAT_STRING = '%(asctime)s %(filename)s:%(lineno)-3s %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT_STRING, datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def get_assets():
    r = requests.get(f'{URL_BASE}/0/public/Assets', headers={'API-Key': API_KEY})
    return r.json()


def get_assetpairs():
    r = requests.get(f'{URL_BASE}/0/public/AssetPairs', headers={'API-Key': API_KEY})
    return r.json()


def get_ticker(pair='USDTZUSD'):
    url = f'{URL_BASE}/0/public/Ticker'
    r = requests.get(url, headers={'API-Key': API_KEY}, params={'pair': pair})
    return r.json()


def get_depth(pair):
    url = f'{URL_BASE}/0/public/Depth'
    r = requests.get(url, headers={'API-Key': API_KEY}, params={'pair': pair})
    return r.json()


class KrakenLoader(object):

    def __init__(self, pairs=PAIRS, freq=60):
        self.pairs = pairs
        self.data_dir = f'./data'
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        self.freq = freq  # snap data every 60 seconds.
        self.files = {pair: os.path.join(self.data_dir, f'{pair}.csv') for pair in self.pairs}
        self.thread = None
        self._running = True

    def start(self):
        self.thread = threading.Thread(target=self.snapshot)
        self.thread.start()

    def stop(self):
        self._running = False
        self.thread.join()

    def snapshot(self):
        while self._running:
            now = pd.Timestamp.utcnow()
            for pair in self.pairs:
                logger.info(f'Take snapshot for {pair} at {now}')
                depth = get_depth(pair)
                data = self.parse(depth['result'][pair])
                data['SnapTime'] = now
                filename = self.files[pair]
                if not os.path.exists(filename):
                    data.to_csv(filename, mode='w', index=False)
                else:
                    data.to_csv(filename, mode='a', index=False, header=False)
            time.sleep(self.freq)

    def parse(self, depth):
        dat_ls = list()
        for side in ['bids', 'asks']:
            dat = pd.DataFrame(depth[side])
            dat.columns = ['Price', 'Volume', 'Timestamp']
            dat['Bid/Ask'] = side[:-1].title()
            dat = dat.reset_index().rename(columns={'index': 'Level'})
            dat['Level'] = dat['Level'] + 1
            dat_ls.append(dat)
        combined = pd.concat(dat_ls, axis=0)
        combined['Timestamp'] = pd.to_datetime(combined['Timestamp'], unit='s')
        return combined


if __name__ == '__main__':
    loader = KrakenLoader()
    loader.start()
    loader.stop()
