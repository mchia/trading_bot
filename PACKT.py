import datetime

import backtrader as bt
import matplotlib.pyplot as plt
import yfinance as yf
from backtrader.indicators import RSI, BollingerBands

# Define the start and end dates
start_date = datetime.datetime(2010, 1, 1)
end_date = datetime.datetime(2021, 12, 31)

data = bt.feeds.PandasData(
    dataname=yf.download('NAB.AX', start=start_date, end=end_date, interval="1d")
)



class RsiBollingerBands(bt.Strategy):
    params = (
        ('rsi_period', 14),
        ('bb_period', 20),
        ('bb_dev', 2),
        ('oversold', 30),
        ('overbought', 70)
    )

    def __init__(self):
        self.rsi = RSI(period=self.params.rsi_period)

    
    def next(self):
        if not self.position:
            if self.rsi < self.params.oversold:
                self.buy()
        else:
            if self.rsi > self.params.overbought:
                self.close()



if __name__ == "__main__":
    cerebro = bt.Cerebro()

    cerebro.addstrategy(RsiBollingerBands)


    cerebro.adddata(data)

    cerebro.broker.setcash(1000)

    cerebro.broker.setcommission(commission=0.001)
    cerebro.broker.set_slippage_fixed(0.01)
    cerebro.run()

    cerebro.plot()
    plt.show()