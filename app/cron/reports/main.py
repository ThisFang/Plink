# -- coding: UTF-8
import time
import datetime
import json
import sys
import getopt
import os


def get_date():
    # get date
    date = str(datetime.date.today())
    try:
        opts, args = getopt.getopt(sys.argv[1:-1], 'd:')
    except Exception as e:
        print(e)
        exit()
    else:
        for op, value in opts:
            if op == '-d':
                date = value

    # verify date
    if date == 'today':
        date = str(datetime.date.today())
    elif date == 'yesterday':
        date = str(datetime.date.today() - datetime.timedelta(days=1))
    else:
        try:
            time.strptime(date, '%Y-%m-%d')
        except Exception as e:
            print(e)
            exit()

    return date


if __name__ == '__main__':
    sys.path.append(os.getcwd())
    from app.cron.reports.stat_agg_pay import StatAggPayOrderReports
    from app.cron.reports.stat_deposit import StatDeposit
    from app.cron.reports.stat_view_time import StatViewTimeReports

    date = get_date()
    stat_agg_pay_order_reports = StatAggPayOrderReports(date)
    stat_agg_pay_order_reports.get_report()
    stat_agg_pay_order_reports.save_report()
    print('stat_agg_pay over!')

    stat_deposit_reports = StatDeposit(date)
    stat_deposit_reports.get_report()
    stat_deposit_reports.save_report()
    print('stat_deposit over!')

    stat_view_time_reports = StatViewTimeReports(date)
    stat_view_time_reports.get_report()
    stat_view_time_reports.save_report()
    print('stat_view_time over!')
