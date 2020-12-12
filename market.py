import datetime
import time
import os
import ccxt
import pandas as pd


class CryptoMarket:
    """Crypto currency market history fetcher using ccxt module.

    Attribute(s):
        currency_pair: name of the currency pair of interest (only one), e.g., 'LINK-USDT'.
        start_date: start date/time of data to fetch, e.g., datetime.datetime(2019, 6, 29, 00, 00, 00)
        end_date: end date/time of data to fetch, e.g., datetime.datetime(2020, 2, 28, 00, 00, 00)

    """

    def __init__(self,
                 currency_pair: str,
                 exchange_name: str):

        self.currency_pair = currency_pair
        self.exchange_name = exchange_name
        self.exchange = getattr(ccxt, exchange_name)()

    def get_exchange_candle_packet_size(self):
        
        # list should be slowly but surely expanded
        if self.exchange_name == 'coinbasepro':
            return 300
        elif self.exchange_name == 'binance':
            return 1000
        else:
            return 100  # hopefully a safe value for any exchange not in the list above
    
    def get_currency_candle_history(self,
                                    start_date: datetime.datetime,
                                    end_date='now',
                                    ) -> None:
        """Method that fetches candle stick chart history for a given currency and time range and write the data
        to a CSV file via a pandas data frame. The method does not return a pandas data frame given it takes some
        time to fetch all the data, it is therefore more flexible to operate on a CSV file for later use.

        """
#         csv_file_name = self.currency_pair.split('/')[0] + '-' + self.currency_pair.split('/')[1]
        csv_file_name = f'data/{self.exchange_name}_{self.currency_pair.lower()}.csv'
        self.currency_pair = self.currency_pair.split('-')[0] + '/' + self.currency_pair.split('-')[1]
        self.currency_pair = self.currency_pair.upper()
        

        # check if csv file exists
        if os.path.isfile(csv_file_name):
            df_old = pd.read_csv(csv_file_name, index_col='time')
            last_data_date = df_old.index[-1]
            start_date = datetime.datetime.strptime(last_data_date, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(minutes=1)
            create_file = False
        else:
            start_date = start_date
            create_file = True

        if end_date == 'now':
            dnow = datetime.datetime.utcnow()
            end_date = datetime.datetime(dnow.year, dnow.month, dnow.day, dnow.hour, dnow.minute, 0)
        end_date = end_date

        n_days = end_date - start_date  # compute number of day(s) within provided date/time range
        n_minutes = int(n_days.seconds / 60)
        # show user the request
        if create_file:
            print(f'Fetching data for {n_days.days} day(s) {n_minutes} minute(s) in 1-minute batches.')
        else:
            print(f'File already exists. Fetching {n_days.days} day(s) {n_minutes} minute(s) of missing data since last download')

        candle_packet_size = self.get_exchange_candle_packet_size()
        
        # loop over individual request to fetch the entire date/time range
        stop = start_date
        start = start_date
        while stop < end_date:
            try:
                stop = start + datetime.timedelta(minutes=candle_packet_size-1)
                if stop > end_date:
                    stop = end_date + datetime.timedelta(minutes=1)
                print(start, stop)
                df = pd.DataFrame(self.exchange.fetch_ohlcv(
                    self.currency_pair, 
                    '1m', 
                    since=self.exchange.parse8601(datetime.datetime.strftime(start, "%Y-%m-%d %H:%M:%S")), 
                    limit=candle_packet_size))
                
                df.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
                df['time'] = df['time'].apply(lambda x: datetime.datetime.utcfromtimestamp(x / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))
                df.sort_values(by='time', ascending=True, inplace=True)

                if create_file:
                    df.to_csv(csv_file_name, index=False)
                    create_file = False
                else:
                    df.to_csv(csv_file_name, mode='a', header=False, index=False)

                start = datetime.datetime.strptime(df.iloc[-1]['time'], "%Y-%m-%d %H:%M:%S") + datetime.timedelta(minutes=1)
                time.sleep (self.exchange.rateLimit/1000) # rateLimit is in millisecond and sleep excepts unit in second
            except Exception as exc:
                print("Issue with fetching data!", exc.args[0])
                time.sleep(1)