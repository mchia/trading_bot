import math
import sqlite3 as sq3
from datetime import date, datetime, timedelta

import backtrader as bt  # backtesting library
import matplotlib.pyplot as plt
import pandas as pd
from backtrader.indicators import RSI


class parameters():
    rsi_period = 14  # Time period in days for RSI
    oversold = 30  # RSI range where stock is considered oversold
    overbought = 70  # RSI Range where stock is considered overbought
    fast = 13  # 13d Moving Average
    fifty_five_ma = 55  # 55d Moving Average
    two_hundred_ma = 200  # 200d Moving Average
    mov_avg = 'EMA'
    ma_type = getattr(bt.indicators, mov_avg)
    fast_plot = str(fast) + '-day ' + mov_avg  # 13d
    mid_plot = str(fifty_five_ma) + '-day ' + mov_avg  # 55d
    slow_plot = str(two_hundred_ma) + '-day ' + mov_avg  # 200d
p = parameters()

def thousand_separator(value, decimals=2):
    return '{:,.{}f}'.format(value, decimals)

class StockBroker():
    def __init__(self, ticker, timeframe):
        # Connect to the database and create a new table
        self.conn = sq3.connect("stock_data.db")  # Connect to the existing db file.
        self.cursor = self.conn.cursor()

        self.sql_query = f"""
            select datetime,
                    open,
                    high,
                    low,
                    close,
                    volume
            from prices_{timeframe}
            where ticker = '{ticker}'
            order by substr(date, 7, 4) || '-' || substr(date, 4, 2) || '-' || substr(date, 1, 2) asc"""

        # Read data from the database into a Pandas DataFrame
        self.df = pd.read_sql_query(self.sql_query, self.conn)
        self.df['datetime'] = pd.to_datetime(self.df['datetime'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
        self.df['ticker'] = ticker
        self.df['interval'] = timeframe
        # Close the database connection
        self.conn.close()

        self.initial_balance = 100000
        self.cerebro = bt.Cerebro()
        self.cerebro.broker.set_cash(self.initial_balance)
        self.cerebro.broker.setcommission(commission=0.001)

        # Create a PandasData feed using the DataFrame
        self.feed = bt.feeds.PandasData(dataname=self.df, datetime='datetime')
        self.cerebro.adddata(self.feed, name=ticker)

class strategies():
    class RSI(bt.Strategy):
        def __init__(self, current_ticker, current_interval):
            # Current ticker and interval
            self.current_ticker = current_ticker
            self.current_interval = current_interval

            # Stratetgy Parameters
            self.dataclose = self.datas[0].close
            self.order = None
            self.buyprice = None
            self.buycomm = None
            self.size_to_buy = None  # Added attribute to store the number of shares to buy
            self.rsi = RSI(period=p.rsi_period)

            # Trade Statistics
            self.trades = 0  # Counter for total trades executed
            self.wins = 0  # Counter for trades that won
            self.losses = 0  # Counter for trades that lost
            self.total_gross_profit = 0
            self.total_gross_losses = 0
            self.total_net_profit = 0
            self.total_net_losses = 0
            self.total_fees = 0
            self.trade_id = 0
            self.balance = sb.initial_balance

            # Data Storage
            self.buy_transactions = []
            self.sell_transactions = []
            self.trade_results = []
            self.trade_transactions = []

        def log(self, txt, trade_id, dt=None):
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, ID: %d, %s' % (dt.isoformat(), trade_id, txt))

        def notify_order(self, order):
            if order.status in [order.Submitted, order.Accepted]:
                return

            if order.status in [order.Completed]:
                if order.isbuy():
                    # Log all buying transactions to a DF.
                    self.buy_transactions.append([self.trade_id, self.datas[0].datetime.date(0), order.executed.price,
                                                order.executed.comm, order.executed.size])
                else:
                    # Log all selling transactions to a  DF.
                    self.sell_transactions.append([self.trade_id, self.datas[0].datetime.date(0), order.executed.price,
                                                order.executed.comm])
                self.bar_executed = len(self)

            # elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            #     self.log('Order Canceled/Margin/Rejected', self.trade_id)

            self.order = None

        def notify_trade(self, trade):
            if not trade.isclosed:
                return

            gross = trade.pnl
            net = trade.pnlcomm
            fees = net - gross

            if gross > 0:
                self.wins += 1
                self.total_gross_profit += gross
                self.total_net_profit += net
            elif gross < 0:
                self.losses += 1
                self.total_gross_losses += abs(gross)
                self.total_net_losses += abs(net)

            self.trades += 1
            self.total_fees += fees

            pnl = thousand_separator(trade.pnl)
            pnlcomm = thousand_separator(trade.pnlcomm)
            sumcomm = thousand_separator(trade.pnlcomm - trade.pnl)
            self.trade_results.append([self.trade_id, round(trade.pnl, 0), round(trade.pnlcomm, 0),
                                    round(sb.cerebro.broker.get_cash(), 0)])

        def print_trade_stats(self):
            ending_balance = self.total_net_profit
            account_growth = round(100 * ((ending_balance - sb.initial_balance) / sb.initial_balance), 2)
            ending_balance = thousand_separator(ending_balance)
            print(
                f'Starting Balance: ${thousand_separator(sb.initial_balance)}, Ending Balance: ${ending_balance}, Account Growth: {account_growth}%')
            print(
                f'Total Gross Profit: ${thousand_separator(self.total_gross_profit - sb.initial_balance)}, Total Net Profit: ${thousand_separator(self.total_net_profit - sb.initial_balance)}')
            print(f'Total Gross Losses: ${thousand_separator(self.total_gross_losses)}, Total Net Losses: ${thousand_separator(self.total_net_losses)}')
            print(f'Total Fees: ${thousand_separator(self.total_fees)}')
            print(f'Total Trades: {self.trades}, Wins: {self.wins}, Losses: {self.losses}')

        def transaction_data(self):
            buy_table = pd.DataFrame(data=self.buy_transactions,
                                    columns=['id', 'entry_date', 'entry_price', 'buying_fee', 'shares'])
            sell_table = pd.DataFrame(data=self.sell_transactions,
                                    columns=['id', 'exit_date', 'exit_price', 'selling_fee'])
            results_table = pd.DataFrame(data=self.trade_results,
                                        columns=['id', 'gross_earnings', 'net_earnings', 'acc_bal'])
            transaction_table = pd.merge(buy_table, sell_table, on='id', how='inner')
            transaction_data = pd.merge(transaction_table, results_table, on='id', how='inner')
            transaction_data['total_fees'] = transaction_data['buying_fee'] + transaction_data['selling_fee']
            transaction_data['percentage_gain'] = round(
                (transaction_data['exit_price'] - transaction_data['entry_price']) / transaction_data['entry_price'] * 100,
                2)
            transaction_data['trade_duration'] = (
                        pd.to_datetime(transaction_data['exit_date']) - pd.to_datetime(transaction_data['entry_date'])).dt.days
            transaction_data['ticker'] = self.current_ticker
            transaction_data['interval'] = self.current_interval
            transaction_data['strategy'] = strategies.GoldenCross.__name__
            transaction_data = transaction_data[
                ['ticker',
                'interval',
                'strategy',
                'entry_date',
                'exit_date',
                'entry_price',
                'exit_price',
                'shares',
                'buying_fee',
                'selling_fee',
                'total_fees',
                'trade_duration',
                'percentage_gain',
                'gross_earnings',
                'net_earnings',
                'acc_bal']
            ]

            # Connect to the database and create a new table
            conn = sq3.connect("stock_data.db")  # Connect to the existing db file.
            cursor = conn.cursor()
            table = 'trade_results_example'

            # # Create the INSERT INTO statement with placeholders
            insert_sql = f"INSERT INTO {table} ({', '.join(transaction_data.columns)}) VALUES ({', '.join([':' + col for col in transaction_data.columns])})"

            # Create a list of dictionaries for the data
            data_to_insert = []
            for index, row in transaction_data.iterrows():
                data = {col: row[col] if col != 'Datetime' else row[col].strftime('%Y-%m-%d %H:%M:%S') for col in
                        transaction_data.columns}
                data_to_insert.append(data)

            # Execute the INSERT INTO statement with parameterized queries
            cursor.executemany(insert_sql, data_to_insert)

            # Commit the changes and close the connection
            conn.commit()
            conn.close()

        def buy_signal(self):
            return (
                    self.position.size == 0 
                    and self.rsi < p.oversold
                    )

        def sell_signal(self):
            return (
                    self.position.size > 0  # There must be an open position to execute a sell
                    and p.overbought
                    )
                    
        def next(self):
            if self.order:
                return

            if not self.position:
                if self.buy_signal():
                    self.size_to_buy = math.floor(self.broker.getvalue() / self.dataclose[0]) * 0.8
                    self.trade_id += 1
                    self.order = self.buy(size=self.size_to_buy, trade_id=self.trade_id)

            elif self.sell_signal():
                self.order = self.sell(size=self.position.size, trade_id=self.trade_id, exectype=bt.Order.StopTrail,
                                    trailpercent=0.05)

class GoldenCross(bt.Strategy):
        def __init__(self, current_ticker, current_interval):
            # Current ticker and interval
            self.current_ticker = current_ticker
            self.current_interval = current_interval

            # Stratetgy Parameters
            self.dataclose = self.datas[0].close
            self.order = None
            self.buyprice = None
            self.buycomm = None
            self.size_to_buy = None  # Added attribute to store the number of shares to buy
            self.fifty_five = p.ma_type(self.datas[0].close, period=p.fifty_five_ma, plotname=p.mid_plot)
            self.two_hundred = p.ma_type(self.datas[0].close, period=p.two_hundred_ma, plotname=p.slow_plot)
            self.goldencross = bt.indicators.CrossOver(self.fifty_five, self.two_hundred)

            # Trade Statistics
            self.trades = 0  # Counter for total trades executed
            self.wins = 0  # Counter for trades that won
            self.losses = 0  # Counter for trades that lost
            self.total_gross_profit = 0
            self.total_gross_losses = 0
            self.total_net_profit = 0
            self.total_net_losses = 0
            self.total_fees = 0
            self.trade_id = 0
            self.balance = sb.initial_balance

            # Data Storage
            self.buy_transactions = []
            self.sell_transactions = []
            self.trade_results = []
            self.trade_transactions = []

        def log(self, txt, trade_id, dt=None):
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, ID: %d, %s' % (dt.isoformat(), trade_id, txt))

        def notify_order(self, order):
            if order.status in [order.Submitted, order.Accepted]:
                return

            if order.status in [order.Completed]:
                if order.isbuy():
                    # Log all buying transactions to a DF.
                    self.buy_transactions.append([self.trade_id, self.datas[0].datetime.date(0), order.executed.price,
                                                order.executed.comm, order.executed.size])
                else:
                    # Log all selling transactions to a  DF.
                    self.sell_transactions.append([self.trade_id, self.datas[0].datetime.date(0), order.executed.price,
                                                order.executed.comm])
                self.bar_executed = len(self)

            # elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            #     self.log('Order Canceled/Margin/Rejected', self.trade_id)

            self.order = None

        def notify_trade(self, trade):
            if not trade.isclosed:
                return

            gross = trade.pnl
            net = trade.pnlcomm
            fees = net - gross

            if gross > 0:
                self.wins += 1
                self.total_gross_profit += gross
                self.total_net_profit += net
            elif gross < 0:
                self.losses += 1
                self.total_gross_losses += abs(gross)
                self.total_net_losses += abs(net)

            self.trades += 1
            self.total_fees += fees

            pnl = thousand_separator(trade.pnl)
            pnlcomm = thousand_separator(trade.pnlcomm)
            sumcomm = thousand_separator(trade.pnlcomm - trade.pnl)
            self.trade_results.append([self.trade_id, round(trade.pnl, 0), round(trade.pnlcomm, 0),
                                    round(sb.cerebro.broker.get_cash(), 0)])

        def print_trade_stats(self):
            ending_balance = self.total_net_profit
            account_growth = round(100 * ((ending_balance - sb.initial_balance) / sb.initial_balance), 2)
            ending_balance = thousand_separator(ending_balance)
            print(
                f'Starting Balance: ${thousand_separator(sb.initial_balance)}, Ending Balance: ${ending_balance}, Account Growth: {account_growth}%')
            print(
                f'Total Gross Profit: ${thousand_separator(self.total_gross_profit - sb.initial_balance)}, Total Net Profit: ${thousand_separator(self.total_net_profit - sb.initial_balance)}')
            print(f'Total Gross Losses: ${thousand_separator(self.total_gross_losses)}, Total Net Losses: ${thousand_separator(self.total_net_losses)}')
            print(f'Total Fees: ${thousand_separator(self.total_fees)}')
            print(f'Total Trades: {self.trades}, Wins: {self.wins}, Losses: {self.losses}')

        def transaction_data(self):
            buy_table = pd.DataFrame(data=self.buy_transactions,
                                    columns=['id', 'entry_date', 'entry_price', 'buying_fee', 'shares'])
            sell_table = pd.DataFrame(data=self.sell_transactions,
                                    columns=['id', 'exit_date', 'exit_price', 'selling_fee'])
            results_table = pd.DataFrame(data=self.trade_results,
                                        columns=['id', 'gross_earnings', 'net_earnings', 'acc_bal'])
            transaction_table = pd.merge(buy_table, sell_table, on='id', how='inner')
            transaction_data = pd.merge(transaction_table, results_table, on='id', how='inner')
            transaction_data['total_fees'] = transaction_data['buying_fee'] + transaction_data['selling_fee']
            transaction_data['percentage_gain'] = round(
                (transaction_data['exit_price'] - transaction_data['entry_price']) / transaction_data['entry_price'] * 100,
                2)
            transaction_data['trade_duration'] = (
                        pd.to_datetime(transaction_data['exit_date']) - pd.to_datetime(transaction_data['entry_date'])).dt.days
            transaction_data['ticker'] = self.current_ticker
            transaction_data['interval'] = self.current_interval
            transaction_data['strategy'] = strategies.GoldenCross.__name__
            transaction_data = transaction_data[
                ['ticker',
                'interval',
                'strategy',
                'entry_date',
                'exit_date',
                'entry_price',
                'exit_price',
                'shares',
                'buying_fee',
                'selling_fee',
                'total_fees',
                'trade_duration',
                'percentage_gain',
                'gross_earnings',
                'net_earnings',
                'acc_bal']
            ]

            # Connect to the database and create a new table
            conn = sq3.connect("stock_data.db")  # Connect to the existing db file.
            cursor = conn.cursor()
            table = 'trade_results_example'

            # # Create the INSERT INTO statement with placeholders
            insert_sql = f"INSERT INTO {table} ({', '.join(transaction_data.columns)}) VALUES ({', '.join([':' + col for col in transaction_data.columns])})"

            # Create a list of dictionaries for the data
            data_to_insert = []
            for index, row in transaction_data.iterrows():
                data = {col: row[col] if col != 'Datetime' else row[col].strftime('%Y-%m-%d %H:%M:%S') for col in
                        transaction_data.columns}
                data_to_insert.append(data)

            # Execute the INSERT INTO statement with parameterized queries
            cursor.executemany(insert_sql, data_to_insert)

            # Commit the changes and close the connection
            conn.commit()
            conn.close()

        def buy_signal(self):
            return (
                    self.position.size == 0  # There must not be an open position to execute a buy
                    and self.goldencross == 1  # # 13d EMA crosses above 55d EMA -- Helps to capture potential Golden Crossover trades earlier
                    and self.data.close[0] > (
                    self.two_hundred[0] and self.two_hundred[-1])  # Price must be above the 200d EMA as this better indicates a bullish trend
            )

        def sell_signal(self):
            return (
                    self.position.size > 0  # There must be an open position to execute a sell
                    and self.goldencross == -1
            )

        def next(self):
            if self.order:
                return

            if not self.position:
                if self.buy_signal():
                    self.size_to_buy = math.floor(self.broker.getvalue() / self.dataclose[0]) * 0.8
                    self.trade_id += 1
                    self.order = self.buy(size=self.size_to_buy, trade_id=self.trade_id)

            elif self.sell_signal():
                self.order = self.sell(size=self.position.size, trade_id=self.trade_id, exectype=bt.Order.StopTrail,
                                    trailpercent=0.05)

current_ticker = 'BTC/USDT'
current_interval = '1d'


sb = StockBroker(ticker=current_ticker, timeframe=current_interval)
sb.cerebro.addstrategy(GoldenCross, current_ticker=current_ticker, current_interval=current_interval)
sb.cerebro.run()
sb.cerebro.plot()
# strategy_instance = sb.cerebro.runstrats[0][0]
# strategy_instance.print_trade_stats()

# # Access the strategy instance and print trade stats
# strategy_instance = sb.cerebro.runstrats[0][0]

# # Insert trade results into the database
# strategy_instance.transaction_data()

# print(f"Successfully processed {current_ticker} - {current_interval}")
