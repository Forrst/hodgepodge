# coding: utf-8

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.integrate import odeint
import MySQLdb
con = MySQLdb.connect(host="192.168.5.106", user="root", passwd="zunjiazichan123", db="app_data", charset="utf8")
sql_us = "select date_time,total_cases,total_deaths,total_recovered from worldometers_old_data where country = 'USA' order by date_time asc"
us_data = pd.read_sql(sql_us,con)

# 人群感染数据
# data = np.array([23214, 17205, 14380, 11791, 9692,
#                  7711, 5974, 4515, 2744, 1975, 1287])[::-1]
data = np.array(us_data['total_cases'].astype('float'))
# 定义初始情况，易感染人数1000万人，感染人1，恢复人0
S0, I0, R0 = 1000000, 15, 0
# 定义11天
days = len(data)
t = np.linspace(0, days-1, days)
def SIR(sir, t, beta, gamma):
    "SIR模型的微分方程"
    S, I, R = sir
    dsdt = - beta * S * I
    didt = beta * S * I - gamma * I
    drdt = gamma * I
    return [dsdt, didt, drdt]

def f(beta, gamma):
    # 求解时序变化
    corr = []
    for a, b in zip(beta, gamma):
        result = odeint(SIR, [S0, I0, R0], t, args=(a, b))
        St, It, Rt = result[:, 0], result[:, 1], result[:, 2]
        corr.append(np.mean((It-data)**2))
    return np.array(corr)
# 定义粒子个数
N = 20

# 定义惯性因子
w = 0.1

# 定义C1，C2
c1, c2 = 2, 2

# 初始化位置
x = np.random.uniform(0, 1, [N, 2])
x[:, 0] *= 0.04
x[:, 1] *= 0.25

# 初始化速度
v = np.random.uniform(0, 1, [N, 2])
v[:, 0] *= 0.04 * 0.03
v[:, 1] *= 0.25 * 0.03
# 个体最佳位置
p_best = np.copy(x)

fitness = f(x[:, 0], x[:, 1])
fitness = np.expand_dims(fitness, 1)
# 群体最佳位置
g_best = p_best[np.argmin(fitness)]
N_step = 200
store = np.zeros([N, N_step, 2])
for step in range(N_step):
    # 计算速度v
    store[:, step, :] = x
    r1, r2 = np.random.random([N, 1]), np.random.random([N, 1])
    v = w * v + (1-w)*(c1 * r1 * (p_best - x) + c2 * r2 * (g_best - x))
    # 更新位置
    x = x + v
    x = np.clip(x, 0, 0.5)
    # 计算适应度
    fitness_new = f(x[:, 0], x[:, 1])
    fitness_new = np.expand_dims(fitness_new, 1)
    fit = np.concatenate([fitness, fitness_new], 1)
    fitness = fitness_new
    # 计算个体最优解
    p_best_for_sel = np.concatenate([
        np.expand_dims(x, 1),
        np.expand_dims(p_best, 1)], 1)
    p_best = p_best_for_sel[[i for i in range(N)], np.argmin(fit, 1), :]
    fit_p = f(p_best[:, 0], p_best[:, 1])
    # 计算全局最优解
    g_best = x[np.argmin(fitness[:, 0])]
    print(g_best)
a, b = g_best
print(f"best a={a}\t best b={b}")

#预测未来的天数90
number_day = 60
dt = np.linspace(0, days-1+number_day, 1000)
result = odeint(SIR, [S0, I0, R0], dt, args=(a, b))
St, It, Rt = result[:, 0], result[:, 1], result[:, 2]

itlist = It.tolist()
index = itlist.index(max(itlist))
import datetime
start = datetime.datetime(year=2020,month=2,day=15)
max_day = start+datetime.timedelta(days=dt[index])
print(f" max predict day:{str(max_day.date())} \t max predict infected:{max(itlist)}")


# 绘图
fig, ax1 = plt.subplots()
ax1.plot(t, data, c="g", label="Real infected")
ax1.plot(dt, It, c="r", linestyle="--", label="Predict infected")

itincrease = It[1:]-It[:-1]
itincreaselist = itincrease.tolist()
itincrease_index = itincreaselist.index(max(itincreaselist))
increase_max_day = start+datetime.timedelta(days=dt[itincrease_index])
print(f" max predict increased day:{str(increase_max_day.date())} \t max predict increased infected:{max(itincreaselist)}")




ax2 = ax1.twinx()
ax2.plot(dt[1:], It[1:]-It[:-1], c="b", label="Number increased")
ax2.legend()
ax2.set_ylabel("Number increased", fontsize=10)
ax1.set_title("SIR MODEL", fontsize=10)
ax1.set_xlabel("Days", fontsize=10)

ax1.set_ylabel("People Number", fontsize=10)
ax1.legend(fontsize=10)
plt.grid(True)
time = np.linspace(0, days-1+number_day, days+number_day)
import datetime
name = []
# for i in range(108):
#     name.append(str(i))
# now = datetime.datetime(year=2020,month=2,day=15)
# for i in range(91):
#     next = now+datetime.timedelta(days=i)
#     name.append(next.strftime("%m-%d"))
plt.xticks(time, name)
plt.show()