import dask.dataframe as dd
import argparse

# 创建命令行参数解析器
parser = argparse.ArgumentParser(description='Read and merge Parquet files using Dask.')

# 添加输入和输出文件路径参数
parser.add_argument('input_path', type=str, help='Path to the input Parquet files (e.g., Data/**/*.parquet)')
parser.add_argument('output_path', type=str, help='Path to the output Parquet file (e.g., out/merged.parquet)')

# 解析命令行参数
args = parser.parse_args()

# 读取输入的 Parquet 文件
df = dd.read_parquet(args.input_path + "/**/*.parquet")

# 输出合并后的 Parquet 文件
df.to_parquet(args.output_path + "/merged.parquet")

print(f'Merged Parquet files from {args.input_path} to {args.output_path}')
