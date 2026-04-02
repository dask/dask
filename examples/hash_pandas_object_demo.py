"""
Bilingual demo for pd.util.hash_pandas_object collision and HashDoS risks.

双语演示脚本：展示 pd.util.hash_pandas_object 的碰撞现象，以及在
Dask 分桶/路由场景下可能带来的 HashDoS 风险。
"""

from __future__ import annotations

import pandas as pd

from dask.dataframe.hyperloglog import compute_hll_array, estimate_count
from dask.dataframe.shuffle import partitioning_index


def print_section(title_cn: str, title_en: str) -> None:
    print("\n" + "=" * 88)
    print(f"{title_cn} / {title_en}")
    print("=" * 88)


def print_kv(label_cn: str, label_en: str, value) -> None:
    print(f"{label_cn} / {label_en}: {value}")


def demo_scalar_collisions() -> None:
    print_section(
        "场景 1：跨 dtype 的确定性碰撞",
        "Scenario 1: Deterministic cross-dtype collisions",
    )

    cases = [
        (
            pd.Series([True], dtype="bool"),
            pd.Series([1], dtype="int64"),
            "布尔 True 与整数 1",
            "bool True vs int 1",
        ),
        (
            pd.Series([pd.Timestamp("1970-01-01 00:00:00.000000001")]),
            pd.Series([1], dtype="int64"),
            "纳秒时间戳 1ns 与整数 1",
            "timestamp 1ns vs int 1",
        ),
        (
            pd.Series([5], dtype="int8"),
            pd.Series([5], dtype="int64"),
            "int8(5) 与 int64(5)",
            "int8(5) vs int64(5)",
        ),
    ]

    for left, right, label_cn, label_en in cases:
        left_hash = int(pd.util.hash_pandas_object(left, index=False).iloc[0])
        right_hash = int(pd.util.hash_pandas_object(right, index=False).iloc[0])
        print(f"- {label_cn} / {label_en}")
        print(f"  left={left.iloc[0]!r}, dtype={left.dtype}, hash={left_hash}")
        print(f"  right={right.iloc[0]!r}, dtype={right.dtype}, hash={right_hash}")
        print(f"  collision / 发生碰撞: {left_hash == right_hash}")

    print(
        "\n结论 / Takeaway: pandas 会先把多种数值/时间 dtype 规整到统一的 "
        "uint64 表示，再做 64 位混洗，因此这类碰撞是确定性的。"
    )


def demo_dataframe_row_collision() -> None:
    print_section(
        "场景 2：整行组合哈希碰撞",
        "Scenario 2: Whole-row combined-hash collision",
    )

    df1 = pd.DataFrame({"A": [1604090909467468979, 2], "B": [4, 4]})
    df2 = pd.DataFrame({"A": [1, 2], "B": [3, 4]})

    h1 = pd.util.hash_pandas_object(df1, index=False)
    h2 = pd.util.hash_pandas_object(df2, index=False)

    print("df1")
    print(df1)
    print("df2")
    print(df2)
    print_kv("df1 行哈希", "df1 row hashes", h1.tolist())
    print_kv("df2 行哈希", "df2 row hashes", h2.tolist())
    print_kv(
        "第 0 行哈希是否相同",
        "Whether row-0 hashes collide",
        int(h1.iloc[0]) == int(h2.iloc[0]),
    )

    print(
        "\n说明 / Note: 这不是单列值相等，而是 DataFrame 多列通过 "
        "combine_hash_arrays 组合后出现的整行碰撞。"
    )


def demo_hll_undercount() -> None:
    print_section(
        "场景 3：HyperLogLog 低估唯一值数量",
        "Scenario 3: HyperLogLog undercounts unique rows",
    )

    rows = pd.DataFrame(
        {
            "A": [1604090909467468979, 1],
            "B": [4, 3],
        }
    )
    row_hashes = pd.util.hash_pandas_object(rows, index=False)
    state = compute_hll_array(rows, b=10)
    approx = estimate_count(state, b=10)
    exact = len(rows.drop_duplicates())

    print(rows)
    print_kv("整行哈希", "row hashes", row_hashes.tolist())
    print_kv("精确唯一行数", "exact unique row count", exact)
    print_kv("HLL 估计值", "HLL estimate", round(float(approx), 6))
    print(
        "\n结论 / Takeaway: 如果两个不同的输入在进入 HLL 前就已经拥有相同哈希，"
        "那么 HLL 会把它们当成同一个元素，导致近似去重被低估。"
    )


def collect_keys_for_bucket(
    npartitions: int, target_bucket: int, count: int
) -> list[str]:
    keys: list[str] = []
    i = 0
    while len(keys) < count:
        key = f"attack_{i}"
        frame = pd.DataFrame({"key": [key]})
        bucket = int(partitioning_index(frame[["key"]], npartitions).iloc[0])
        if bucket == target_bucket:
            keys.append(key)
        i += 1
    return keys


def demo_hashdos_bucket_hotspot() -> None:
    print_section(
        "场景 4：分桶热点与 HashDoS 风险",
        "Scenario 4: Bucket hotspot and HashDoS risk",
    )

    npartitions = 16
    target_bucket = 0
    crafted_keys = collect_keys_for_bucket(
        npartitions=npartitions, target_bucket=target_bucket, count=64
    )

    attack_df = pd.DataFrame(
        {
            "key": crafted_keys * 64,
            "payload": range(len(crafted_keys) * 64),
        }
    )
    buckets = partitioning_index(attack_df[["key"]], npartitions)
    counts = pd.Series(buckets).value_counts().sort_index()

    print_kv("目标分区", "target bucket", target_bucket)
    print_kv("分区总数", "number of partitions", npartitions)
    print_kv("命中目标桶的不同 key 数量", "distinct crafted keys", len(crafted_keys))
    print_kv("攻击样本总行数", "total crafted rows", len(attack_df))
    print_kv("前 10 个命中的 key", "first 10 matching keys", crafted_keys[:10])
    print("\n分桶分布 / Bucket distribution:")
    print(counts.to_string())
    print_kv(
        "目标桶负载占比",
        "target-bucket load ratio",
        f"{counts.get(target_bucket, 0) / len(attack_df):.2%}",
    )

    print(
        "\n最大风险 / Primary risk: 如果系统把同一 bucket 路由到同一 worker/node，"
        "那么攻击者可以构造大量不同 key 落到同一个分区，形成热点节点。"
    )
    print(
        "这会导致 / This can cause: 内存放大、单节点 CPU 打满、shuffle/merge 延迟上升、"
        "任务失败，严重时表现为 HashDoS。"
    )


def main() -> None:
    print(
        "pd.util.hash_pandas_object collision and HashDoS demo\n"
        "pd.util.hash_pandas_object 碰撞与 HashDoS 双语演示"
    )
    demo_scalar_collisions()
    demo_dataframe_row_collision()
    demo_hll_undercount()
    demo_hashdos_bucket_hotspot()


if __name__ == "__main__":
    main()
