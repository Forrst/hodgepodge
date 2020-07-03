# coding: utf-8
import numpy as np
import random
import matplotlib.pyplot as plt
from scipy.integrate import odeint
# 人群感染数据
data = np.array([23214, 17205, 14380, 11791, 9692,
                 7711, 5974, 4515, 2744, 1975, 1287])[::-1]
# 定义初始情况，易感染人数1000万人，感染人1，恢复人0
S0, I0, R0 = 100000, 1287, 0
# 定义11天
t = np.linspace(0, 10, 11)
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
N_step = 100
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
dt = np.linspace(0, 30, 1000)
result = odeint(SIR, [S0, I0, R0], dt, args=(a, b))
St, It, Rt = result[:, 0], result[:, 1], result[:, 2]

# 绘图
fig, ax1 = plt.subplots()
ax1.plot(t, data, c="g", label="real Infected")
ax1.plot(dt, It, c="r", linestyle="--", label="predict Infected")

ax2 = ax1.twinx()
ax2.plot(dt[1:], It[1:]-It[:-1], c="b", label="increasing number")
ax2.legend()
ax2.set_ylabel("increasing number", fontsize=18)
ax1.set_title("SIR fit", fontsize=18)
ax1.set_xlabel("days", fontsize=18)

ax1.set_ylabel("number of people", fontsize=18)
ax1.legend(fontsize=18)
plt.grid(True)
time = np.linspace(0, 30, 31)
name = [f"1-{itr}" for itr in range(24, 32)]+[f"2-{itr}" for itr in range(1, 23)]
plt.xticks(time, name)
plt.show()