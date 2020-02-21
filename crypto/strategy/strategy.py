#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-12-10 下午5:34
'''
import datetime
import logging
import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import Line, Bar, Grid, Kline
from pyecharts.globals import ThemeType


logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger =logging.getLogger(" btc ")


def dateparse (time_str):
    return datetime.datetime.strptime(time_str[:-3],'%Y-%m-%d %H:%M:%S.%f' )


#读取数据
data=pd.read_csv('btc.csv',parse_dates=True, date_parser=dateparse, index_col=[1],header=None)
data.columns = ['symbol','open','high','low','close','v_btc','v_usdt']
data.index.name = 'time'
data = data.sort_values(by='time',ascending=True)

#处理缺失值
idx = pd.date_range(start=data.index.min(), end=data.index.max(),freq='60s')
tdata = data.reindex(idx)
for col in ['symbol','open','high','low','close']:
    tdata[col].ffill(inplace=True)
for col in ['v_btc','v_usdt']:
    tdata[col].fillna(0,inplace=True)
tdata.index.name = 'time'
tdata = tdata.sort_values(by='time',ascending=True)





date_start = pd.Timestamp('2017-08-18 00:00:00')
dlast = tdata[tdata.index>=date_start]
# #获取其他周期的线
#5分钟线
# dlast_5m = dlast.resample('5T').agg(
#     {'open': lambda x: x[0], 'high': lambda x: x.max(), 'low': lambda x: x.min(), 'close': lambda x: x[-1],
#      'v_btc': lambda x: x.sum(), 'v_usdt': lambda x: x.sum()})

#4小时线
dlast_4h = dlast.resample('4h',label='left').agg(
    {'open': lambda x: x[0], 'high': lambda x: x.max(), 'low': lambda x: x.min(), 'close': lambda x: x[-1],
     'v_btc': lambda x: x.sum(), 'v_usdt': lambda x: x.sum()})

#ma均线

dlast_4h['ma5'] = dlast_4h['close'].shift(5).rolling(5, min_periods=5).mean()
dlast_4h['ma10'] = dlast_4h['close'].shift(10).rolling(10, min_periods=10).mean()

#boll线
dlast_4h['Boll']=dlast_4h['close'].rolling(20).mean()
dlast_4h['boll_t']=dlast_4h['close'].rolling(20).std()
dlast_4h['UBoll']=dlast_4h['Boll']+1.95*dlast_4h['boll_t']
dlast_4h['LBoll']=dlast_4h['Boll']-1.95*dlast_4h['boll_t']
dlast_4h['Boll'] = round(dlast_4h['Boll'],2)
dlast_4h['UBoll'] = round(dlast_4h['UBoll'],2)
dlast_4h['LBoll'] = round(dlast_4h['LBoll'],2)
dlast_4h['Rate'] = round((dlast_4h['close']-dlast_4h['open'])/dlast_4h['open']*100,2)
dlast_4h['ma10'] = round(dlast_4h['ma10'],2)
dlast_4h['Rate'] = dlast_4h['Rate'].apply(lambda x:'%.2f%%' % x)
line_data = dlast_4h
#[dlast_4h.index>=pd.Timestamp('2019-07-09 00:00:00')]

#画图
date = list(map(str,line_data.index.tolist()))
k_plot_value = line_data.apply(lambda x: [x['open'], x['close'], x['low'], x['high']], axis=1).tolist()
k_v_usdt = line_data.apply(lambda x: [round(x['v_usdt'],2)], axis=1).tolist()

bg_color = "#21202D"

kline = (
    Kline(init_opts=opts.InitOpts(bg_color=bg_color,theme=ThemeType.DARK,width="3840px",height="2160px"))
        .add_xaxis(xaxis_data=date)
        .add_yaxis(
        series_name="btc price",
        y_axis=k_plot_value,
        itemstyle_opts=opts.ItemStyleOpts(color="#FD1050",color0="#0CF49B",border_color="#FD1050",border_color0="#0CF49B"),
    )
        .set_global_opts(
        title_opts=opts.TitleOpts(
            title="btc 4小时图",
            subtitle="boll 线",
            title_textstyle_opts = opts.TextStyleOpts(color="#fff"),
            subtitle_textstyle_opts = opts.TextStyleOpts(color="#fff"),
        ),
        xaxis_opts=opts.AxisOpts(type_="category",axisline_opts = opts.AxisLineOpts(linestyle_opts = opts.LineStyleOpts(color="#8392A5"))),
        yaxis_opts=opts.AxisOpts(
            is_scale=True,
            axisline_opts = opts.AxisLineOpts(linestyle_opts = opts.LineStyleOpts(color="#8392A5")),
            splitline_opts = opts.SplitLineOpts(is_show=False)
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
            ),],
        #     opts.DataZoomOpts(
        #         is_show=True,
        #         xaxis_index=[0, 1],
        #         type_="slider",
        #         pos_top="90%",
        #         range_start=0,
        #         range_end=100,
        #     ),
        # ],
        toolbox_opts=opts.ToolboxOpts(is_show=False),
        tooltip_opts=opts.TooltipOpts(
            is_show = True,
            trigger="axis",
            axis_pointer_type="cross",
            background_color="rgba(245, 245, 245, 0.8)",
            border_width=1,
            border_color="#ccc",
            textstyle_opts=opts.TextStyleOpts(font_size=10,font_family="Microsoft YaHei",color="#000"),
        ),
        visualmap_opts=opts.VisualMapOpts(
            is_show=False,
            dimension=2,
            series_index=5,
            is_piecewise=True,
            pieces=[
                {"value": 1, "color": "#0CF49B"},
                {"value": -1, "color": "#FD1050"},
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
    ).set_series_opts()
)

line = (
    Line()
        .add_xaxis(xaxis_data=date)
        .add_yaxis(
        series_name="boll上轨",
        y_axis=line_data.UBoll.tolist(),
        is_smooth=True,
        is_symbol_show=False,
        is_hover_animation=False,
        linestyle_opts=opts.LineStyleOpts(width=1),
        label_opts=opts.LabelOpts(is_show=False),
    )
        .add_yaxis(
        series_name="boll下轨",
        y_axis=line_data.LBoll.tolist(),
        is_smooth=True,
        is_symbol_show=False,
        is_hover_animation=False,
        linestyle_opts=opts.LineStyleOpts(width=1),
        label_opts=opts.LabelOpts(is_show=False),
    )
        .add_yaxis(
        series_name="boll中轨",
        y_axis=line_data.Boll.tolist(),
        is_smooth=True,
        is_symbol_show=False,
        is_hover_animation=False,
        linestyle_opts=opts.LineStyleOpts(width=1),
        label_opts=opts.LabelOpts(is_show=False),
    )
        .add_yaxis(
        series_name="涨跌幅",
        y_axis=[line_data.Rate.tolist()],
        is_smooth=True,
        is_symbol_show=False,
        is_hover_animation=False,
        linestyle_opts=opts.LineStyleOpts(width=0.1),
        label_opts=opts.LabelOpts(is_show=True),
    )
        .set_global_opts(xaxis_opts=opts.AxisOpts(type_="line"))
)

bar = (
    Bar()
        .add_xaxis(xaxis_data=date)
        .add_yaxis(
        series_name="Volume",
        yaxis_data=[[i,k_v_usdt[i][0],1 if k_plot_value[i][0]>k_plot_value[i][1] else -1] for i in range(len(k_v_usdt))],
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

overlap_kline_line = kline.overlap(line)

grid_chart = Grid()
grid_chart.add(
    overlap_kline_line,
    grid_opts=opts.GridOpts(pos_left="10%", pos_right="5%", height="50%"),
)
grid_chart.add(
    bar,
    grid_opts=opts.GridOpts(
        pos_left="10%", pos_right="5%", pos_top="70%", height="25%"
    ),
)
grid_chart.render("btc_min.html")


btc_line = dlast_4h[dlast_4h.index>pd.Timestamp("2017-08-21 00:00:00")]


bar = (
    Bar()
        .add_xaxis(xaxis_data=date)
        .add_yaxis(
        series_name="Volume",
        yaxis_data=[[i,k_v_usdt[i][0],1 if k_plot_value[i][0]>k_plot_value[i][1] else -1] for i in range(len(k_v_usdt))],
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