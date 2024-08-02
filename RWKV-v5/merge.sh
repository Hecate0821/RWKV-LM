#!/bin/bash

# 检查参数数量
if [ "$#" -ne 2 ]; then
    echo "用法: \$0 <输入文件夹> <输出文件>"
    exit 1
fi

# 输入和输出文件夹
input_folder="\$1"
output_file="\$2"

# 临时文件，用于存储所有输入文件
temp_file_list="temp_parquet_files.txt"

# 清空临时文件（如果存在）
> "$temp_file_list"

# 查找所有子文件夹中的 Parquet 文件并将其写入临时文件
find "$input_folder" -name "*.parquet" >> "$temp_file_list"

# 合并所有 Parquet 文件
pqrs merge --input $(cat "$temp_file_list") --output "$output_file"

# 清理临时文件
rm "$temp_file_list"

echo "合并完成，输出文件为: $output_file"
