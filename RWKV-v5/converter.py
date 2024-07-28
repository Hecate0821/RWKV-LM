import os
import json
import pyarrow as pa
import pyarrow.parquet as pq


def convert_parquet_to_jsonl(input_folder, output_file):
    all_tables = []

    # 遍历文件夹中的所有文件
    for filename in os.listdir(input_folder):
        if filename.endswith('.parquet'):
            file_path = os.path.join(input_folder, filename)
            # 读取 Parquet 文件
            table = pq.read_table(file_path)

            # 只保留 'text' 列
            if 'text' in table.column_names:
                table = table.select(['text'])  # 使用 select 方法保留 'text' 列
                all_tables.append(table)

    # 合并所有表
    if all_tables:
        combined_table = pa.concat_tables(all_tables)

        # 写入 JSONL 文件，指定编码为 'utf-8'
        with open(output_file, 'w', encoding='utf-8') as jsonl_file:
            for row in combined_table.to_pylist():
                jsonl_file.write(f"{json.dumps(row)}\n")  # 使用 json.dumps 确保使用双引号
    else:
        print("没有找到 Parquet 文件.")


if __name__ == "__main__":
    input_folder = "Data"  # 替换为你的文件夹路径
    output_file = "output.jsonl"  # 输出文件名

    convert_parquet_to_jsonl(input_folder, output_file)
    print(f"所有 Parquet 文件已合并并转换为 {output_file}")
