import datetime
import time
import os
import coinbasepro as cbp
import pandas as pd


class CryptoMarket:
    """Crypto currency market history fetcher using Coinbase Pro API.

    Attribute(s):
        currency_exchange: name of the currency exchange of interest (only one), e.g., 'LINK-USD'.
        csv_file_name: name of the csv file to store the fetched date
        start_date: start date/time of data to fetch, e.g., datetime.datetime(2019, 6, 29, 00, 00, 00)
        end_date: end date/time of data to fetch, e.g., datetime.datetime(2020, 2, 28, 00, 00, 00)

    """

    def __init__(self,
                 currency_exchange: str):

        self.currency_exchange = currency_exchange
        self.client_connection = cbp.PublicClient()  # instantiate the public client connection to coinbase pro


    def get_currency_candle_history(self,
                                    start_date: datetime.datetime,
                                    end_date='now',
                                    tranche_size: int=60 # in second, options are: 60, 300, 900, 3600, 21600, 86400 seconds
                                    ) -> None:
        """Method that fetches candle stick chart history for a given currency and time range and write the data
        to a CSV file via a pandas data frame. The method does not return a pandas data frame given it takes some
        time to fetch all the data, it is therefore more flexible to operate on a CSV file for later use.

        """
        crypto_name = self.currency_exchange.split('-')[0]
        csv_file_name = f'data/{crypto_name.lower()}.csv'

        # sample size in seconds --> should be arg input
        # tranche_size = tranche_size

        # check if csv file exists
        if os.path.isfile(csv_file_name):
            df_old = pd.read_csv(csv_file_name, index_col='time')
            last_data_date = df_old.index[-1]
            start_date = datetime.datetime.strptime(last_data_date, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(seconds=tranche_size)
            create_file = False
        else:
            start_date = start_date
            create_file = True

        if end_date == 'now':
            dnow = datetime.datetime.utcnow()
            end_date = datetime.datetime(dnow.year, dnow.month, dnow.day, dnow.hour, dnow.minute, 0)
        end_date = end_date

        # coinbase limit: 300 candles per call
        max_data_per_request = 300

        n_days = end_date - start_date  # compute number of day(s) within provided date/time range
        n_minutes = int(n_days.seconds / 60)
        # show user the request
        if create_file:
            print(f'Fetching data for {n_days.days} day(s) {n_minutes} minute(s). Sample size: {tranche_size} seconds.')
        else:
            print(f'File already exists. Fetching {n_days.days} day(s) {n_minutes} minute(s) of missing data since last download')

        # ask user to approve the request
        # input("Press Enter to continue...")

        # loop over individual request to fetch the entire date/time range
        request_size = tranche_size * max_data_per_request
        stop = start_date
        start = start_date
        while stop < end_date:
            try:
                stop = start + datetime.timedelta(seconds=request_size)
                if stop > end_date:
                    stop = end_date + datetime.timedelta(seconds=tranche_size)
                print(start, stop)
                df = pd.DataFrame(self.client_connection.get_product_historic_rates(
                                      product_id=self.currency_exchange,
                                      start=start.isoformat(),
                                      stop=stop.isoformat(),
                                      granularity=str(tranche_size)))

                df.sort_values(by='time', ascending=True, inplace=True)

                if create_file:
                    df.to_csv(csv_file_name, index=False)
                    create_file = False
                else:
                    df.to_csv(csv_file_name, mode='a', header=False, index=False)

                start += datetime.timedelta(seconds=request_size + tranche_size)
                time.sleep(5.0)
            except Exception as exc:
                print("Issue with fetching data!", exc.args[0])
                time.sleep(1)

    def get_order_book(self, level):

        order_book = self.client_connection.get_product_order_book(self.currency_exchange, level=level)

        return order_book

    def get_time(self):

        return self.client_connection.get_time()

    def get_price(self):

        return self.client_connection.get_product_ticker(product_id=self.currency_exchange)

    def get_currency_trade_history(self):

        client_connection = cbp.PublicClient()  # instantiate the public client connection to coinbase pro

        trade_history_generator = client_connection.get_product_trades(product_id=self.currency_exchange)

        df = pd.DataFrame(columns=['timestamp', 'trade_amount', 'trade_type', 'trade_size'])

        for trade in trade_history_generator:

            timestamp = trade['time']
            print(timestamp, self.start_date)
            if timestamp < self.start_date and timestamp > self.end_date:
                print('good')

            # df = df.append({'timestamp': trade['time'], 'trade_amount': trade['price'], 'trade_type': trade['side'],
            #                 'trade_size': trade['size']}, ignore_index=True)#, 'trade_type': 'Login'}, ignore_index=True)
            # time = trade['time']
            # price.append(float(x['price']))
            # delta = (divmod((now - time).total_seconds(), 1))[0]
            # timestamp.append(delta)
            # # print(delta)
            # if abs(delta) > 100:
            #     break
