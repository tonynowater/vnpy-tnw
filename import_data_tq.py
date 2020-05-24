from vnpy.trader.constant import (Exchange, Interval)
import pandas as pd
from vnpy.trader.database import database_manager
from vnpy.trader.object import (BarData, TickData)
import datetime, time


# 封装函数
def move_df_to_sql(imported_data: pd.DataFrame):
    bars = []
    start = None
    count = 0

    for row in imported_data.itertuples():

        bar = BarData(
            symbol=row.symbol,
            exchange=row.exchange,
            datetime=row.dtime,
            interval=row.interval,
            volume=row.volume,
            open_price=row.open,
            high_price=row.high,
            low_price=row.low,
            close_price=row.close,
            open_interest=row.o_interest,
            gateway_name="DB",
        )
        bars.append(bar)

        # do some statistics
        count += 1
        if not start:
            start = bar.datetime
    end = bar.datetime

    # insert into database
    database_manager.save_bar_data(bars)
    print(f'Insert Bar: {count} from {start} - {end}')


def move_csvs_to_db(filepath: str, sym:str):
    # 读取需要入库的csv文件，该文件是用gbk编码
    imported_data = pd.read_csv(filepath, encoding='gbk')
    imported_data.columns = [
        'dtime', 'open', 'high', 'low', 'close', 'volume', 'o_interest',
        'c_interest'
    ]
    # 增加一列'exchange',市场代码，为 Exchange.SHFE SHFE
    imported_data['exchange'] = Exchange.SHFE
    # 增加一列数据 `inteval`，且该列数据的所有值都是 Interval.MINUTE
    imported_data['interval'] = Interval.MINUTE
    #至此，列头如下：
    # columns:['dtime','open','high','low','close','volume','o_interest','c_interest','exchange','interval']

    # 明确需要是float数据类型的列
    float_columns = [
        'open', 'high', 'low', 'close', 'volume', 'o_interest', 'c_interest'
    ]
    for col in float_columns:
        imported_data[col] = imported_data[col].astype('float')

    # 明确时间戳的格式
    # %Y/%m/%d %H:%M:%S 代表着你的csv数据中的时间戳必须是 2020/05/01 08:32:30 格式
    datetime_format = '%Y%m%d %H:%M:%S'
    imported_data['dtime'] = pd.to_datetime(imported_data['dtime'],
                                            format=datetime_format)
    # imported_data['dtime'] = imported_data['dtime'].dt.strftime('%Y%m%d %H:%M:%S')
    imported_data['symbol'] = sym
    move_df_to_sql(imported_data)


if __name__ == "__main__":
    hq_dir = 'C:\\HQ\\1MIN\\'
    filelist = ['au_1min.csv', 'rb_1min.csv']
    for filename in filelist:
        symb=filename.split('_')[0]+'88'
        file_path = hq_dir + filename
        print('start importing %s ,symol:%s' % (filename,symb))
        starttime = datetime.datetime.now()
        move_csvs_to_db(file_path,symb)
        endtime = datetime.datetime.now()
        timecost = endtime - starttime
        print('%s is completed importing , time cost(s): %d ' % (filename, timecost.seconds))

'''
https://mp.weixin.qq.com/s/5NmGO5enaUrCHbaTBuY3Ww
如果在进行Sqlite数据入库的时候，出现peewee.InterfaceError: Error binding parameter 2 - probably unsupported type错误，解决方法如下：
找到imported_data['时间'] = pd.to_datetime(imported_data['时间'],format=datetime_format)代码所在行

在该行代码下键入imported_data['时间'] = imported_data['时间'].dt.strftime('%Y%m%d %H:%M:%S')
'''