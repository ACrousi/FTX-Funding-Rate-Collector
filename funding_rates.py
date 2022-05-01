from asyncio import futures
from asyncio.windows_events import NULL
from FTX_api import FtxClient
from functools import reduce
import pandas as pd
import numpy as np
import re
import time
from datetime import date
import json

class FTX_fundingRates_data:
    """Returns: {'coin':{'perp':perp, 'exPerp':exPerp_dict, 'spot':spot_dict},...}"""
    def __init__(self, FTX_API: FtxClient) -> None:
        self.API = FTX_API
        self.All_rates = self.API.get_funding_rates(start_time = time.time()-3600)

    def get_splited_pairs(self) -> dict:
        """Returns: {'coin':{'perp':perp, 'exPerp':exPerp_dict, 'spot':spot_dict, 'rate':rate},...}"""
        all_pairs = self._give_key(self.API.get_markets())
        with_rate_pairs = self._add_last_rates(all_pairs)
        result = {}
        for key, value in with_rate_pairs.items():
            if 'rate' in value:
                result[key] = value
        return result

    def _give_key(self, all_pairs: list) -> dict:
        """Do: {'BTC':[{'name' : 'BTC-PERP',...},
                            {'name' : 'BTC-0624',...}],
                    'ETH':[{'name' : 'ETH-PERP',...},
                            {'name' : 'ETH-0624',...}]}"""
        all_pairs = self.API.get_markets()
        coin_dict = {}
        for pair in all_pairs:
            name = pair['baseCurrency'] if pair['baseCurrency'] != None else pair['underlying']
            if name in coin_dict:
                coin_dict[name].append(pair)
            else:
                coin_dict[name] = [pair]
        return self._splited_pairs(coin_dict)

    def _splited_pairs(self, coin_pairs: dict) -> dict:
        """Returns: {'coin':{'perp':perp, 'exPerp':exPerp_dict, 'spot':spot_dict},...}"""
        splited_dict = {}
        for key, value in coin_pairs.items():
            # perp_list = []
            exPerp_dict = {}
            spot_dict = {}
            for pair in value:
                if pair['type'] == 'future':

                    exf = re.escape(pair['underlying']) + r'-\d{4}'
                    if bool(re.search(exf, pair['name'])):
                        expire_date = pair['name'][-4:]
                        exPerp_dict[expire_date] = pair

                    if bool(re.search(r'PERP$', pair['name'])):
                        # perp_list.append(pair)
                        perp = pair

                if (pair['type'] == 'spot') & (pair['quoteCurrency'] == 'USD'):
                    spot_dict["USD"] = pair

            splited_dict[key] = {'perp':perp, 'exPerp':exPerp_dict, 'spot':spot_dict}
        return splited_dict

    def _add_last_rates(self, coin_pairs: dict) -> dict:
        """ Will filted with perp coins
            Returns: {'coin':{'perp':perp, 'exPerp':exPerp_dict, 'spot':spot_dict, 'rate':rate},...}"""
        # All_rates = self.API.get_funding_rates(start_time = time.time()-3600)
        for item in self.All_rates:
            coin = re.search(r'(.*)(?=-|\/)', item['future']).group()
            coin_pairs[coin]['rate'] = item
        return coin_pairs

class FTX_fundingRates_df(FTX_fundingRates_data):
    def __init__(self, FTX_API: FtxClient) -> None:
        super().__init__(FTX_API)
    
    # 當下APY
    def _now_APY(self, item: dict) -> float:
        return (1+abs(item['rate']))**(24*365)-1

    # 過去500筆之平均APY
    def _avg500_APY(self, item: dict) -> float:
        fr_500 = self.API.get_funding_rates(future = item['future'])
        return reduce(lambda x,y:x*y,[1+abs(i['rate']) for i in fr_500])**17-1

    # 溢價率
    def _premium_rate_cal(self, item: dict, date: str = None, quoteCurrency: str =None) -> dict:
        """Returns: {'spot_prm': spot_prm, 'exfuture_prm': exfuture_prm}"""
        exfuture_prm = None
        spot_prm = None

        perp = item['perp']

        if len(item['exPerp']):
            if date:
                exfuture = item['exPerp'][date]
                exfuture_prm = (perp['price'] - exfuture['price'])/perp['price']
            else:
                exfuture_prm = {}
                for key, value in item['exPerp'].items():
                    exfuture_prm[key] = (perp['price'] - value['price'])/perp['price']


        if len(item['spot']):
            if quoteCurrency:
                spot = item['spot'][quoteCurrency]
                spot_prm = (perp['price'] - spot['price'])/perp['price']
            else:
                spot_prm = {}
                for key, value in item['spot'].items():
                    spot_prm[key] = (perp['price'] - value['price'])/perp['price']

        
        return {'spot_prm': spot_prm, 'exfuture_prm': exfuture_prm}

    # def _premium_rate_cal(item: dict) -> float:
    #     coin = re.search(r'(.*)(?=-|\/)', item['future']).group()
    #     perp = next(filter(lambda x: x['name'] == coin, split_pairs['perp']))
    #     experp = [x for x in split_pairs['exPerp'] if x['name'] == coin]
    #     spot = next(filter(lambda x: x['name'] == coin, split_pairs['spot']))
    #     if item['rate'] < 0:
    #         target = max(max([x['ask'] for x in experp], spot['ask']))
    #     if item['rate'] >= 0:
    #         target = min(min([x['bid'] for x in experp], spot['bid']))
    #     return (perp - target)/perp

    # 選出最接近的季度合約，且離到期日不超過兩天
    def last_exfuture(self, item: dict, today: str) -> str:
        for key, value in item['exPerp'].items():
                if (int(key[0:2]) - int(today[0:2]) <=3) & ((int(key[0:2]) - int(today[0:2]) != 0) | (int(key[2:]) - int(today[2:]) < 2)):
                    return key

    def eligable_to_hedge_check(self, item: dict, future_rate: float) -> bool:
        if not (item['exPerp'] | item['spot']):
            return False

        if (future_rate<0) & (not item['exPerp']): 
            return False
        else:
            return True

    def fundingRates_dataframe(self) -> pd.DataFrame:
        df_lst = []
        split_pairs = self.get_splited_pairs()
        today = date.today().strftime("%m%d")

        for index, (coin, item) in enumerate(split_pairs.items()):

            now_apy = self._now_APY(item['rate'])
            avg_apy = self._avg500_APY(item['rate'])

            exfuture_date = self.last_exfuture(item, today)
            premium_rate = self._premium_rate_cal(item, date = exfuture_date, quoteCurrency = 'USD')

            spot_prm = premium_rate['spot_prm']
            exfuture_prm = premium_rate['exfuture_prm']

            perp_vol = item['perp']['volumeUsd24h']
            exfuture_vol = item['exPerp'][exfuture_date]['volumeUsd24h'] if item['exPerp'] else None
            spot_vol = item['spot']['USD']['volumeUsd24h'] if item['spot'] else None

            exp = "(average_price(\"%(0)s-PERP\", minute)-average_index_price(\"%(0)s-PERP\", minute))/average_index_price(\"%(0)s-PERP\", minute)/24" % {'0': coin}
            predict_rate = self.API.post_quantzone_expression(exp)

            eligable_to_hedge = self.eligable_to_hedge_check(item, predict_rate)

            lst = [coin, item['rate']['rate'], now_apy, avg_apy, predict_rate, perp_vol, exfuture_prm, exfuture_vol, spot_prm, spot_vol, eligable_to_hedge]
            df_lst.append(lst)
            print("success : {} {}/{}".format(coin, index+1, len(self.All_rates)))

        df = pd.DataFrame(df_lst, columns =['coin', 'last_rate', 'now_apy', 'avg_apy', 'predict_rate', 'perp_vol', 'exfuture_prm', 'exfuture_vol', 'spot_prm', 'spot_vol', 'eligable_to_hedge'])
        return df

if __name__ == '__main__':

    def read_setting():
        with open('setting.json') as json_file:
            return json.load(json_file)

    config = read_setting()

    api_key = config["api_key"]
    api_secret = config["api_secret"]
    subaccount_name = config["subaccount_name"]

    API = FtxClient(api_key, api_secret, subaccount_name)
    FTX_fr = FTX_fundingRates_df(API)

    fr_df = FTX_fr.fundingRates_dataframe()
    fr_df.to_csv('fundingRates.csv')
