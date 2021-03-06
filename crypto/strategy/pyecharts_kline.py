#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-12-14 上午10:28
'''
import datetime
import logging

import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import Line, Bar, Grid, Kline

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger = logging.getLogger(" btc ")


def dateparse(time_str):
    return datetime.datetime.strptime(time_str[:-3], '%Y-%m-%d %H:%M:%S.%f')


# 读取数据
data = pd.read_csv('btc.csv', parse_dates=True, date_parser=dateparse, index_col=[1], header=None)
data.columns = ['symbol', 'open', 'high', 'low', 'close', 'v_btc', 'v_usdt']
data.index.name = 'time'
data = data.sort_values(by='time', ascending=True)

# 处理缺失值
idx = pd.date_range(start=data.index.min(), end=data.index.max(), freq='60s')
tdata = data.reindex(idx)
for col in ['symbol', 'open', 'high', 'low', 'close']:
    tdata[col].ffill(inplace=True)
for col in ['v_btc', 'v_usdt']:
    tdata[col].fillna(0, inplace=True)
tdata.index.name = 'time'
tdata = tdata.sort_values(by='time', ascending=True)

date_start = pd.Timestamp('2017-08-18 00:00:00')
dlast = tdata[tdata.index >= date_start]

dlast_4h = dlast.resample('4h', label='left').agg(
    {'open': lambda x: x[0], 'high': lambda x: x.max(), 'low': lambda x: x.min(), 'close': lambda x: x[-1],
     'v_btc': lambda x: x.sum(), 'v_usdt': lambda x: x.sum()})

dlast_4h['Boll'] = dlast_4h['close'].rolling(20).mean()
dlast_4h['boll_t'] = dlast_4h['close'].rolling(20).std()
dlast_4h['UBoll'] = dlast_4h['Boll'] + 2 * dlast_4h['boll_t']
dlast_4h['LBoll'] = dlast_4h['Boll'] - 2 * dlast_4h['boll_t']

line_data = dlast_4h[dlast_4h.index >= pd.Timestamp('2019-05-09 00:00:00')]
x_data = list(map(str, line_data.index.tolist()))

k_plot_value = line_data.apply(lambda x: [x['open'], x['close'], x['low'], x['high']], axis=1).tolist()
k_v_usdt = line_data.apply(lambda x: [x['v_usdt']], axis=1).tolist()

data = k_plot_value
boll = line_data['Boll'].tolist()
lb = line_data['LBoll'].tolist()
ub = line_data['UBoll'].tolist()

data = [
    [2320.26, 2320.26, 2287.3, 2362.94],
    [2300, 2291.3, 2288.26, 2308.38],
    [2295.35, 2346.5, 2295.35, 2345.92],
    [2347.22, 2358.98, 2337.35, 2363.8],
    [2360.75, 2382.48, 2347.89, 2383.76],
    [2383.43, 2385.42, 2371.23, 2391.82],
    [2377.41, 2419.02, 2369.57, 2421.15],
    [2425.92, 2428.15, 2417.58, 2440.38],
    [2411, 2433.13, 2403.3, 2437.42],
    [2432.68, 2334.48, 2427.7, 2441.73],
    [2430.69, 2418.53, 2394.22, 2433.89],
    [2416.62, 2432.4, 2414.4, 2443.03],
    [2441.91, 2421.56, 2418.43, 2444.8],
    [2420.26, 2382.91, 2373.53, 2427.07],
    [2383.49, 2397.18, 2370.61, 2397.94],
    [2378.82, 2325.95, 2309.17, 2378.82],
    [2322.94, 2314.16, 2308.76, 2330.88],
    [2320.62, 2325.82, 2315.01, 2338.78],
    [2313.74, 2293.34, 2289.89, 2340.71],
    [2297.77, 2313.22, 2292.03, 2324.63],
    [2322.32, 2365.59, 2308.92, 2366.16],
    [2364.54, 2359.51, 2330.86, 2369.65],
    [2332.08, 2273.4, 2259.25, 2333.54],
    [2274.81, 2326.31, 2270.1, 2328.14],
    [2333.61, 2347.18, 2321.6, 2351.44],
    [2340.44, 2324.29, 2304.27, 2352.02],
    [2326.42, 2318.61, 2314.59, 2333.67],
    [2314.68, 2310.59, 2296.58, 2320.96],
    [2309.16, 2286.6, 2264.83, 2333.29],
    [2282.17, 2263.97, 2253.25, 2286.33],
    [2255.77, 2270.28, 2253.31, 2276.22],
]

def calculate_ma(day_count: int, d):
    result: List[Union[float, str]] = []
    for i in range(len(d)):
        if i < day_count:
            result.append("-")
            continue
        sum_total = 0.0
        for j in range(day_count):
            sum_total += float(d[i - j][1])
        result.append(abs(float("%.3f" % (sum_total / day_count))))
    return result

x_data = ["2017-7-{}".format(i + 1) for i in range(31)]




kline = (
    Kline()
        .add_xaxis(xaxis_data=x_data)
        .add_yaxis(
        series_name="Dow-Jones index",
        y_axis=data,
        itemstyle_opts=opts.ItemStyleOpts(color="#ec0000", color0="#00da3c"),
    )
        .set_global_opts(
        title_opts=opts.TitleOpts(
            title="复杂版 Kline 示例 (Kline + Line + Bar)",
            subtitle="MA 就以(2, 4, 6, 8为例)",
        ),
        xaxis_opts=opts.AxisOpts(type_="category"),
        yaxis_opts=opts.AxisOpts(
            is_scale=True,
            splitarea_opts=opts.SplitAreaOpts(
                is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)
            ),
        ),
        legend_opts=opts.LegendOpts(
            is_show=False, pos_bottom=10, pos_left="center"
        ),
        datazoom_opts=[
            opts.DataZoomOpts(
                is_show=False,
                type_="inside",
                xaxis_index=[0, 1],
                range_start=0,
                range_end=100,
            ),
            opts.DataZoomOpts(
                is_show=True,
                xaxis_index=[0, 1],
                type_="slider",
                pos_top="90%",
                range_start=0,
                range_end=100,
            ),
        ],
        tooltip_opts=opts.TooltipOpts(
            trigger="axis",
            axis_pointer_type="cross",
            background_color="rgba(245, 245, 245, 0.8)",
            border_width=1,
            border_color="#ccc",
            textstyle_opts=opts.TextStyleOpts(color="#000"),
        ),
        visualmap_opts=opts.VisualMapOpts(
            is_show=False,
            dimension=2,
            series_index=5,
            is_piecewise=True,
            pieces=[
                {"value": 1, "color": "#ec0000"},
                {"value": -1, "color": "#00da3c"},
            ],
        ),
        axispointer_opts=opts.AxisPointerOpts(
            is_show=True,
            link=[{"xAxisIndex": "all"}],
            label=opts.LabelOpts(background_color="#777"),
        ),
        brush_opts=opts.BrushOpts(
            x_axis_index="all",
            brush_link="all",
            out_of_brush={"colorAlpha": 0.1},
            brush_type="lineX",
        ),
    )
)

line = (
    Line()
        .add_xaxis(xaxis_data=x_data)
        .add_yaxis(
        series_name="ub",
        y_axis=line_data.UBoll.tolist()[:31],
        is_smooth=True,
        is_hover_animation=False,
        linestyle_opts=opts.LineStyleOpts(width=3, opacity=0.5),
        label_opts=opts.LabelOpts(is_show=False),
    )
        .add_yaxis(
        series_name="lb",
        y_axis=line_data.LBoll.tolist()[:31],
        is_smooth=True,
        is_hover_animation=False,
        linestyle_opts=opts.LineStyleOpts(width=3, opacity=0.5),
        label_opts=opts.LabelOpts(is_show=False),
    )
        .add_yaxis(
        series_name="boll",
        y_axis=line_data.Boll.tolist()[:31],
        is_smooth=True,
        is_hover_animation=False,
        linestyle_opts=opts.LineStyleOpts(width=3, opacity=0.5),
        label_opts=opts.LabelOpts(is_show=False),
    )
        .add_yaxis(
        series_name="MA8",
        y_axis=calculate_ma(day_count=8, d=data),
        is_smooth=True,
        is_hover_animation=False,
        linestyle_opts=opts.LineStyleOpts(width=3, opacity=0.5),
        label_opts=opts.LabelOpts(is_show=False),
    )
        .set_global_opts(xaxis_opts=opts.AxisOpts(type_="category"))
)

bar = (
    Bar()
        .add_xaxis(xaxis_data=x_data)
        .add_yaxis(
        series_name="Volume",
        yaxis_data=[
            [i, data[i][3], 1 if data[i][0] > data[i][1] else -1]
            for i in range(len(data))
        ],
        xaxis_index=1,
        yaxis_index=1,
        label_opts=opts.LabelOpts(is_show=False),
    )
        .set_global_opts(
        xaxis_opts=opts.AxisOpts(
            type_="category",
            is_scale=True,
            grid_index=1,
            boundary_gap=False,
            axisline_opts=opts.AxisLineOpts(is_on_zero=False),
            axistick_opts=opts.AxisTickOpts(is_show=False),
            splitline_opts=opts.SplitLineOpts(is_show=False),
            axislabel_opts=opts.LabelOpts(is_show=False),
            split_number=20,
            min_="dataMin",
            max_="dataMax",
        ),
        yaxis_opts=opts.AxisOpts(
            grid_index=1,
            is_scale=True,
            split_number=2,
            axislabel_opts=opts.LabelOpts(is_show=False),
            axisline_opts=opts.AxisLineOpts(is_show=False),
            axistick_opts=opts.AxisTickOpts(is_show=False),
            splitline_opts=opts.SplitLineOpts(is_show=False),
        ),
        legend_opts=opts.LegendOpts(is_show=False),
    )
)

# Kline And Line
overlap_kline_line = kline.overlap(line)

# Grid Overlap + Bar
grid_chart = Grid()
grid_chart.add(
    overlap_kline_line,
    grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", height="50%"),
)
grid_chart.add(
    bar,
    grid_opts=opts.GridOpts(
        pos_left="10%", pos_right="8%", pos_top="70%", height="16%"
    ),
)
