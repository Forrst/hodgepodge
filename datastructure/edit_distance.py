#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2020-05-09 上午8:47
'''

#计算编辑距离
import numpy as np
a = 'kitten'
b = 'sitting'

def computeDistance(str1,str2):
    x_length = len(str1)
    y_length = len(str2)
    matrix = np.zeros((x_length+1,y_length+1))
    for i in range(x_length+1):
        for j in range(y_length+1):
            if i==0 or j==0:
                matrix[i][j] = i+j
            else:
                if str1[i-1] == str2[j-1]:
                    matrix[i][j] = min(matrix[i-1,j]+1,matrix[i,j-1]+1,matrix[i-1,j-1]+0)
                else:
                    matrix[i][j] = min(matrix[i-1,j]+1,matrix[i,j-1]+1,matrix[i-1,j-1]+1)
    return matrix

#编辑距离矩阵
distance_matrix = computeDistance(a,b)
print(distance_matrix)

#距离公式
distance = 1-distance_matrix[-1][-1]/max(len(a),len(b))
print(distance)