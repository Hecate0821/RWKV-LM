import os


def merge_files(folder_path, output_path):
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    bin_files = {}
    idx_files = {}

    # 遍历文件夹，分类存储 bin 和 idx 文件
    for filename in os.listdir(folder_path):
        if filename.endswith('.bin'):
            key = filename[:-4]  # 去掉 .bin
            bin_files[key] = os.path.join(folder_path, filename)
        elif filename.endswith('.idx'):
            key = filename[:-4]  # 去掉 .idx
            idx_files[key] = os.path.join(folder_path, filename)

    # 合并文件
    for key in bin_files.keys():
        if key in idx_files:  # 确保同时存在 .bin 和 .idx 文件
            bin_path = bin_files[key]
            idx_path = idx_files[key]

            # 读取并合并 bin 文件
            with open(bin_path, 'rb') as bin_file:
                bin_data = bin_file.read()

            with open(os.path.join(output_path, f'{key}_merged.bin'), 'wb') as merged_bin:
                merged_bin.write(bin_data)

            # 读取并合并 idx 文件
            with open(idx_path, 'rb') as idx_file:
                idx_data = idx_file.read()

            with open(os.path.join(output_path, f'{key}_merged.idx'), 'wb') as merged_idx:
                merged_idx.write(idx_data)

            print(f'Merged {key} files into {output_path}/{key}_merged.bin and {output_path}/{key}_merged.idx')


if __name__ == "__main__":
    folder_path = "./"
    output_path = "./"
    merge_files(folder_path, output_path)
