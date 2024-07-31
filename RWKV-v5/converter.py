import os
import pandas as pd
import pyarrow.parquet as pq
import json
import argparse
from tqdm import tqdm
import gc


def convert_parquet_to_jsonl(parquet_file, output_file):
    # 使用pyarrow逐行读取Parquet文件
    table = pq.read_table(parquet_file)
    df = table.to_pandas()

    # 确保有text列
    if 'text' not in df.columns:
        print(f"No 'text' column in {parquet_file}")
        return

    # 将DataFrame转换为JSON行格式
    with open(output_file, 'a', encoding='utf-8') as f:
        for record in tqdm(df['text'], desc=f'Processing {parquet_file}'):
            json_record = json.dumps({"text": record}, ensure_ascii=False)
            f.write(json_record + '\n')

    # 删除源Parquet文件
    os.remove(parquet_file)
    print(f"\nDeleted {parquet_file}")

    # 释放内存
    del table
    del df
    gc.collect()


def process_directory(root_dir, output_file):
    # 不删除，接着往里写就行
    # 如果output.jsonl文件已经存在，则先删除它
    # if os.path.exists(output_file):
    #     os.remove(output_file)

    parquet_files = []
    # 遍历根目录下的所有子文件夹，收集所有Parquet文件
    for subdir, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.parquet'):
                parquet_files.append(os.path.join(subdir, file))

    total_files = len(parquet_files)

    # 添加总进度条
    with tqdm(total=total_files, desc='Total Progress') as pbar:
        for parquet_file in parquet_files:
            gc.collect()
            print(f"Processing {parquet_file}")
            convert_parquet_to_jsonl(parquet_file, output_file)
            pbar.update(1)  # 更新总进度条


if __name__ == "__main__":
    # 设置命令行参数解析
    parser = argparse.ArgumentParser(description="Convert Parquet files to JSONL format.")
    parser.add_argument('root_dir', type=str, help='The root directory containing Parquet files.')
    parser.add_argument('output_file', type=str, help='The output JSONL file name.')

    args = parser.parse_args()

    root_dir = os.path.expanduser(args.root_dir)  # 处理用户目录符号 '~'
    output_file = args.output_file
    try:
        process_directory(root_dir, output_file)
    except MemoryError:
        print("Memory error")
        gc.collect()
        process_directory(root_dir, output_file)
