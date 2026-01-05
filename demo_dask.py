#!/usr/bin/env python3

import dask
import dask.array as da

print("Dask 版本:", dask.__version__)

# 示例: 使用 Dask Array 进行并行计算
print("\n=== Dask Array 并行计算示例 ===")
print("创建一个 1000x1000 的数组，分为 100x100 的块")
x = da.ones((1000, 1000), chunks=(100, 100))
print(f"数组结构: {x}")

print("\n执行操作: y = x + x.T (数组加上其转置)")
y = x + x.T
print(f"结果结构: {y}")

print("\n计算数组的平均值")
z = y.mean()
result = z.compute()
print(f"计算结果: {result}")
print("\n✅ Dask 库已成功运行！")