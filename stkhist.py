# Download "Full Bhavcopy and Security Deliverable data" before executing this script.
# Archive old files to limit data processing.
# Usage: python3 stkhist.py marico or python3 stkhist.py infy n  
# use parameter "N" in argument for efficiency
import csv
import glob
import os
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime, timedelta
from bokeh.models import DatetimeTickFormatter, NumeralTickFormatter
from bokeh.plotting import figure, show
from bokeh.models import LinearAxis, Range1d, CrosshairTool, Span, HoverTool
from bokeh.layouts import row, column
from bokeh.models.widgets import TextInput
from bokeh.models import TextInput, CustomJS, ColumnDataSource, Slider
#

def user_input():
    n = len(sys.argv)
    symbol = "RELIANCE"    # default value
    option = "N"           # default value "N" (for efficiency). "Y" Reads all csv files to create sorted.csv
    if (n >= 2): symbol = sys.argv[1]
    if (n >= 3): option = sys.argv[2]
    symbol = symbol.upper()
    option = option.upper()
    print("Processing parms: ", symbol, ",", option)
    return symbol, option

def is_float(string):
    if string.replace(".", "").isnumeric():
        return True
    else:
        return False

def csv_stkscreener(max_date, stk_symbol, curr_tqnt, max_tqnt, curr_delv):
    if (curr_tqnt > (0.9 * max_tqnt) and curr_delv.strip() != "-"):       # Fish out big player activity today. if tqntis within 50% of max value
        #print(max_date, ':', stk_symbol, ':', curr_tqnt, ':', max_tqnt, ':', curr_delv)
        FILE_PATH = Path('/home/asarkar/Documents/Python/PriceHistory/screener.csv')
        column_names = ["DATE", "SYMBOL", "CURR_TQNT", "MAX_TQNT", "DELV"]
        print_data = {'DATE': [max_date],
                      'SYMBOL': [stk_symbol],
                      'CURR_TQNT': [curr_tqnt],
                      'MAX_TQNT': [max_tqnt], 
                      'DELV%': [curr_delv]
                      }
        df = pd.DataFrame(print_data)
        if not os.path.isfile(FILE_PATH):
            df.to_csv(FILE_PATH, header='column_names', index=False)
        else:
            df.to_csv(FILE_PATH, mode='a', header=False, index=False)
    return

def cash_mf(df):
    stk_symbol = " "
    prev_netmf = 0
    prev_avgprice = 0
    max_tqnt = 0
    curr_tqnt = 0
    curr_delv = 0
    max_date = " "
    filt_row = pd.DataFrame(columns=['DATE','SYMBOL','CURR_TQNT','MAX_TQNT','DELV%'])
    #if (df.iloc[0]['LOW_PRICE'] < df.iloc[0]['HIGH_PRICE']):
    #    df.iloc[0]['MULTIPLIER'] = -1
    #else
    #    df.iloc[0]['MULTIPLIER'] = 1
    #df.iloc[0]['NET_MF'] = df['MF'] * df['MULTIPLIER']
    for index, row in df.iterrows():
        if (row['SYMBOL'] != stk_symbol):
            if (row['LOW_PRICE'] < row['HIGH_PRICE']):
                row['MULTIPLIER'] = -1
            else:
                row['MULTIPLIER'] = 1
            row['NET_MF'] = row['MF'] * row['MULTIPLIER']
            csv_stkscreener(max_date, stk_symbol, curr_tqnt, max_tqnt, curr_delv)
            curr_tqnt = 0
            max_tqnt = 0
            stk_symbol = row['SYMBOL']
            prev_netmf = row['NET_MF']
            prev_avgprice = row['AVG_PRICE']
            max_date = row['DATE1'].strftime("%Y %b %d")
        elif (row['SYMBOL'] == stk_symbol):
            if (row['AVG_PRICE'] == prev_avgprice):
                if (row['NET_MF'] >= 0):
                    row['MULTIPLIER'] = 1
                else:
                    row['MULTIPLIER'] = -1
            elif (row['AVG_PRICE'] > prev_avgprice):
                row['MULTIPLIER'] = 1
            else:
                row['MULTIPLIER'] = -1
            row['NET_MF'] = row['MULTIPLIER'] * row['MF'] + prev_netmf
            prev_avgprice = row['AVG_PRICE']
            prev_netmf = row['NET_MF']
            curr_delv = row['DELIV_PER']
        else:
            print(stk_symbol)
        curr_tqnt = row['TQ/NT']
        if (row['TQ/NT'] > max_tqnt):
            max_tqnt = row['TQ/NT']
            max_date = row['DATE1'].strftime("%Y %m %d")
        df.at[index,'MULTIPLIER'] = row['MULTIPLIER']
        df.at[index, 'NET_MF'] = row['NET_MF']
    return df


def process_csv():
    path = '/home/asarkar/Documents/Python/PriceHistory'
    symbol, option = user_input()
    if (option == "N"):
        df = pd.read_csv('sorted.csv', parse_dates=['DATE1'])
    else:
        all_files = glob.glob(os.path.join(path, "sec*.csv"))
        df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
        df.columns = df.columns.str.lstrip()                                         # strip leading spaces from Column Names
        df = df.drop(['PREV_CLOSE', 'OPEN_PRICE', 'LAST_PRICE', 'CLOSE_PRICE', 'TURNOVER_LACS'], axis=1)
        df['DATE1'] = pd.to_datetime(df['DATE1'], format = ' %d-%b-%Y')              # convert to datetime datatype so that sorting is done correctly
        df = df.sort_values(['SYMBOL', 'DATE1'], ascending=[True, True])
        df = df.drop_duplicates(subset=['SYMBOL', 'DATE1'], keep='first')
        df['TQ/NT'] = df['TTL_TRD_QNTY']/df['NO_OF_TRADES']
        df['TQ/NT'] = df['TQ/NT'].round(2)                                           # Round off TQ/ NT to 2 decimal places
        df['MF'] = df['TTL_TRD_QNTY'] * df['AVG_PRICE']/10000000                     # MF in Cr
        df['MF'] = df['MF'].round(0)                                                 # Round of MF to 0 decimal places
        df['MULTIPLIER'] = 0
        df['NET_MF'] = 0
        df = cash_mf(df)
        df.to_csv('sorted.csv', encoding='utf-8', index=False)
    if not (df['SYMBOL'].eq(symbol)).any():                                          # check if data for stock exists
        print("Invalid Symbol")
        exit()
    df = df[df['SYMBOL'] == symbol]
    df['DELIV_PER'] = pd.to_numeric(df['DELIV_PER'])
    return df

def plot_stock_hist(df):
    dates = df['DATE1']
    width = Span(dimension="width", line_dash="dashed", line_width=2)
    height = Span(dimension="height", line_dash="dotted", line_width=2)
    fig = figure(
        title=df['SYMBOL'].iloc[0],
        x_axis_type="datetime",
        sizing_mode="stretch_width",
        max_width=1500,
        height=500,
        tools="hover,pan,wheel_zoom,box_zoom,reset", 
        #toolbar_location=None, # Hides Bokeh toolbar
    )
    fig.add_tools(CrosshairTool(overlay=[width, height]))
    fig.xaxis.axis_label = 'Date'
    fig.yaxis.axis_label = 'Average Price'
    fig.xaxis[0].ticker.desired_num_ticks = 40                                # increase the number of ticks in x axis to see more dates
    fig.y_range = Range1d(start=int(df['AVG_PRICE'].min()-1), end=int(df['AVG_PRICE'].max()+1))
    fig.extra_y_ranges['Delv'] = Range1d(start=0, end=100)
    fig.add_layout(LinearAxis(y_range_name='Delv', axis_label='Delivery %'), 'right')
    fig.extra_y_ranges['TQNT'] = Range1d(start=int(df['TQ/NT'].min()-1), end=int(df['TQ/NT'].max()+1))
    fig.add_layout(LinearAxis(y_range_name='TQNT', axis_label='TQ/NT'), 'right')
    fig.extra_y_ranges['NetMF'] = Range1d(start=int(df['NET_MF'].min()), end=int(df['NET_MF'].max()))
    fig.add_layout(LinearAxis(y_range_name='NetMF', axis_label='Net MF'), 'right')    
    fig.line(dates, df['AVG_PRICE'], color="navy", legend_label="Avg Price")
    fig.line(dates, df['DELIV_PER'], y_range_name = 'Delv', color="red", legend_label="Delv %")
    fig.line(dates, df['TQ/NT'], y_range_name = 'TQNT', color="brown", legend_label="TQ/ NT")
    fig.line(dates, df['NET_MF'], y_range_name = 'NetMF', color="green", legend_label="Net MF")
    fig.legend.location = "top_left"
    fig.legend.click_policy="hide"
    show(fig)
    return

def plot_textinput(df):
    symbol = 'RELIANCE'
    df1 = df[df['SYMBOL'] == symbol]
    dates = pd.to_datetime(df1['DATE1'], format = ' %d-%b-%Y')
    text = TextInput(value = "", title = "Stock:")
    x = dates
    y = df1[df1['SYMBOL'] == symbol]
    y1 = df1['AVG_PRICE']
    y2 = pd.to_numeric(df1['DELIV_PER'])
    y3 = df1['TQ/NT']
    source = ColumnDataSource(data=dict(x=x, y=y, y1=y1, y2=y2, y3=y3))
    p1 = figure(width=1500, height=300, x_axis_type="datetime", tools="hover,pan,wheel_zoom,box_zoom,reset", )
    p1.line('x', 'y1', source=source, color="navy", legend_label="Avg Price")
    p2 = figure(width=1500, height=300, x_axis_type="datetime", tools="hover,pan,wheel_zoom,box_zoom,reset", )
    p2.line('x', 'y2', source=source, color="red", legend_label="Delv %")
    p3 = figure(width=1500, height=300, x_axis_type="datetime", tools="hover,pan,wheel_zoom,box_zoom,reset", )
    p3.line('x', 'y3', source=source, color="brown", legend_label="TQ/ NT")
    callback = CustomJS(args=dict(source=source), code="""
        const stock = cb_obj.value
        // const x = source.data.x
        // const y = Array.from(x, (x) => Math.pow(x, f))
        filteredSource = source.filter(source.x === stock);
        source.data = filteredSource
    """)
    show(column(text, p1, p2, p3))
    return

def main():
    df = process_csv()
    plot_stock_hist(df)

if __name__ == "__main__":
    main()
    exit()

