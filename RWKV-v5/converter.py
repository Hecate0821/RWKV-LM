import os
import pandas as pd
import pyarrow.parquet as pq
import json


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
        for record in df['text']:
            json_record = json.dumps({"text": record}, ensure_ascii=False)
            f.write(json_record + '\n')

    # 删除源Parquet文件
    os.remove(parquet_file)
    print(f"Deleted {parquet_file}")


def process_directory(root_dir, output_file):
    # 如果output.jsonl文件已经存在，则先删除它
    if os.path.exists(output_file):
        os.remove(output_file)

    # 遍历根目录下的所有子文件夹
    for subdir, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.parquet'):
                parquet_file = os.path.join(subdir, file)
                print(f"Processing {parquet_file}")
                convert_parquet_to_jsonl(parquet_file, output_file)


if __name__ == "__main__":

    root_dir = '~/data'  # 替换为你的根目录
    output_file = 'output0730.jsonl'
    process_directory(root_dir, output_file)
