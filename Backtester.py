import inspect
import math
import sqlite3 as sq3
from datetime import date, datetime, timedelta

import backtrader as bt
import pandas as pd
from backtrader.indicators import EMA, MACD, RSI, BollingerBands


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


class BaseStrategy(bt.Strategy):
    insert_table = 'trade_results_example'

    def __init__(self, current_ticker, current_interval):
        # Current ticker and interval
        self.current_ticker = current_ticker
        self.current_interval = current_interval

        # Strategy Parameters
        self.dataclose = self.datas[0].close
        self.order = None
        self.buyprice = None
        self.buycomm = None
        self.size_to_buy = None  # Added attribute to store the number of shares to buy

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
                # Log all selling transactions to a separate DF.
                self.sell_transactions.append([self.trade_id, self.datas[0].datetime.date(0), order.executed.price,
                                              order.executed.comm])
            self.bar_executed = len(self)

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
        self.trade_results.append([self.trade_id, round(trade.pnl, 0), round(trade.pnlcomm, 0),
                                  round(sb.cerebro.broker.get_cash(), 0)])
        
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
        transaction_data['strategy'] = self.__class__.__name__
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
        insert_table = 'trade_results_example'
        # Create the INSERT INTO statement with placeholders
        insert_sql = f"INSERT INTO {insert_table} ({', '.join(transaction_data.columns)}) VALUES ({', '.join([':' + col for col in transaction_data.columns])})"

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

    def next(self):
        if self.order:
            return

        if not self.position:
            if self.buy_signal():
                self.size_to_buy = math.floor(self.broker.getvalue() / self.dataclose[0]) * 0.8
                self.trade_id += 1
                self.order = self.buy(size=self.size_to_buy, trade_id=self.trade_id)

        elif self.sell_signal():
            self.order = self.sell(size=self.position.size, trade_id=self.trade_id, exectype=bt.Order.StopTrail, trailpercent=0.05)


class strategies():
    class RSIStrategy(BaseStrategy):
        # Parameters for RSI
        params = (
            ('rsi_period', 14),
            ('oversold', 30),
            ('overbought', 70)
            )

        def __init__(self, *args, **kwargs):
            super(strategies.RSIStrategy, self).__init__(*args, **kwargs)
            self.rsi = bt.indicators.RSI(period=self.params.rsi_period)

        def buy_signal(self):
            return (
                self.position.size == 0
                and self.rsi < self.params.oversold
            )

        def sell_signal(self):
            return (
                self.position.size > 0
                and self.rsi > self.params.overbought
            )

    class GoldenCross(BaseStrategy):
        params = (
            ('fast', 55),
            ('slow', 200)
            )

        def __init__(self, *args, **kwargs):
            super(strategies.GoldenCross, self).__init__(*args, **kwargs)
            self.fifty_five = EMA(self.datas[0].close, period=self.params.fast)
            self.two_hundred = EMA(self.datas[0].close, period=self.params.slow)
            self.goldencross = bt.indicators.CrossOver(self.fifty_five, self.two_hundred)

        def buy_signal(self):
            return (
                    self.position.size == 0
                    and self.goldencross == 1
                    and self.data.close[0] > (self.two_hundred[0] and self.two_hundred[-1])
                    )

        def sell_signal(self):
            return (
                    self.position.size > 0
                    and self.goldencross == -1
                    )
        
    class BollingerBands(BaseStrategy):
        params = (
            ('period', 20),
            ('stddev', 2)
            )

        def __init__(self, *args, **kwargs):
            super(strategies.BollingerBands, self).__init__(*args, **kwargs)
            self.bbands = BollingerBands(period=self.params.period, devfactor=self.params.stddev)

        def buy_signal(self):
            return (
                    self.position.size == 0
                    and self.data.close[0] <= self.bbands.lines.bot[0]
                    )

        def sell_signal(self):
            return (
                    self.position.size > 0
                    and self.data.close[0] >= self.bbands.lines.top[0]
                    )

class strategy_list():
    strategy_names = [strategy_class.__name__ for strategy_name, strategy_class in inspect.getmembers(strategies) if
                    inspect.isclass(strategy_class) and issubclass(strategy_class, BaseStrategy)]

    strategy_sql = f"strategies as (select '{strategy_names[0]}' as strategy"

    for strategy in strategy_names[1:]:
        strategy_sql += f"\nunion all\nselect '{strategy}'"

    strategy_sql += ")"

    # Connect to the database and get the list of available stocks and intervals
    conn = sq3.connect("stock_data.db")
    cursor = conn.cursor()

    # Define the ticker_list query
    ticker_list_query = f"""
        with ticker_list as (
            select ticker, interval from prices_1d
            union all
            select ticker, interval from prices_4h
            union all
            select ticker, interval from prices_1h
            union all
            select ticker, interval from prices_30m
            union all
            select ticker, interval from prices_15m
        ),

        {strategy_sql}

        select distinct ticker, interval, strategy
        from ticker_list
        cross join strategies
        where ticker || interval || strategy not in 
        (select distinct ticker || interval || strategy
        from {BaseStrategy.insert_table})
    """

    # Execute the query to get the list of available stocks and intervals
    ticker_interval_df = pd.read_sql_query(ticker_list_query, conn)
    # Close the database connection
    conn.close()

# Iterate over each row in the ticker_interval_df DataFrame
for index, row in strategy_list.ticker_interval_df.iterrows():
    current_ticker = row['ticker']
    current_interval = row['interval']
    current_strategy = row['strategy']

    try:
        # Create a new instance of the StockBroker for each strategy
        sb = StockBroker(ticker=current_ticker, timeframe=current_interval)

        # Get the corresponding strategy class based on the strategy name
        strategy_class = getattr(strategies, current_strategy)

        # Add the current strategy to the cerebro
        sb.cerebro.addstrategy(strategy_class, current_ticker=current_ticker, current_interval=current_interval)

        # Run the backtest
        sb.cerebro.run()

        # Get the strategy instance
        strategy_instance = sb.cerebro.runstrats[0][0]

        # Save transaction data to the database
        strategy_instance.transaction_data()

        print(f"Successfully processed {current_ticker} - {current_interval} using {strategy_class.__name__}")

    except Exception as e:
        # Capture errors and where they are so it doesn't abruptly end the loop.
        print(f"Error processing {current_ticker} - {current_interval} - {current_strategy}: {e}")

print("Backtesting completed and inserted into database")