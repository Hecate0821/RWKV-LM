import json
import os


def split_jsonl_file(input_file, chunk_size, output_path):
    output_path = output_path + '_size_' + str(chunk_size)
    if not os.path.isdir(output_path):
        os.mkdir(output_path)
    log_file_name = 'split.log'
    log_file = os.path.join(output_path, log_file_name)

    # 读取已完成的分片序号
    if os.path.exists(log_file):
        with open(log_file, 'r') as log:
            last_split_index = int(log.read().strip())
    else:
        last_split_index = -1

    current_index = last_split_index + 1
    output_file = None
    current_chunk = []

    with open(input_file, 'r', encoding='utf-8') as infile:
        for line_number, line in enumerate(infile):

            if line_number // chunk_size == current_index:
                if output_file:
                    output_file.close()

                output_file_name = f'chunk_{current_index}.jsonl'
                output_file = os.path.join(output_path, output_file_name)
                output_file = open(output_file, 'w', encoding='utf-8')
                current_index += 1
                if current_index % 10 == 0:
                    print(current_index)
            if output_file is not None:
                output_file.write(line)

        if output_file:
            output_file.close()

    # 更新分片序号到日志文件
    with open(log_file, 'w') as log:
        log.write(str(last_split_index + (line_number // chunk_size) + 1))


if __name__ == '__main__':
    input_file = 'output0730.jsonl'  # 替换为你的JSONL文件路径
    output_path = 'jsonl_chunk'
    chunk_size = 3000000  # 替换为你想要的分片行数
    split_jsonl_file(input_file, chunk_size, output_path)
