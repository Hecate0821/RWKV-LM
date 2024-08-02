import pandas as pd
import pyarrow.parquet as pq
import glob
import os
import time
import argparse

# 创建命令行参数解析器
parser = argparse.ArgumentParser(description='Merge Parquet files from a folder and its subfolders.')
parser.add_argument('input_folder', type=str, help='Path to the input folder containing Parquet files.')
parser.add_argument('output_file', type=str, help='Path to the output merged Parquet file.')

args = parser.parse_args()

# 开始计时
start_time = time.time()

# 使用 glob 查找所有子文件夹中的 .parquet 文件
parquet_files = glob.glob(os.path.join(args.input_folder, '**', '*.parquet'), recursive=True)

# 读取并合并所有 Parquet 文件
df = pd.concat([pd.read_parquet(f) for f in parquet_files])

# 将合并后的 DataFrame 写入新的 Parquet 文件
df.to_parquet(args.output_file, index=False)

# 结束计时
end_time = time.time()
elapsed_time = end_time - start_time

print(f'Merged {len(parquet_files)} Parquet files into {args.output_file}')
print(f'Time taken: {elapsed_time:.2f} seconds')
