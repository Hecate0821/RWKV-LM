import os


def merge_files(bin1, bin2, output_bin):
    with open(output_bin, 'wb') as wfd:
        for f in [bin1, bin2]:
            with open(f, 'rb') as fd:
                wfd.write(fd.read())


def merge_idx_files(idx1, idx2, output_idx):
    with open(output_idx, 'a') as wfd:
        for f in [idx1, idx2]:
            with open(f, 'r') as fd:
                wfd.write(fd.read())


def main():
    input_path = './'  # 修改为你的输入文件夹路径
    output_path = './output'  # 修改为你的输出文件夹路径

    bin_files = sorted([f for f in os.listdir(input_path) if f.endswith('.bin')])
    idx_files = sorted([f for f in os.listdir(input_path) if f.endswith('.idx')])

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    merged_bin = bin_files.pop(0)
    merged_idx = idx_files.pop(0)

    merged_bin_path = os.path.join(input_path, merged_bin)
    merged_idx_path = os.path.join(input_path, merged_idx)

    for bin_file, idx_file in zip(bin_files, idx_files):
        next_bin = os.path.join(input_path, bin_file)
        next_idx = os.path.join(input_path, idx_file)

        new_merged_bin = os.path.join(output_path, f'merged_{merged_bin.split(".")[0]}_{bin_file.split(".")[0]}.bin')
        new_merged_idx = os.path.join(output_path, f'merged_{merged_idx.split(".")[0]}_{idx_file.split(".")[0]}.idx')

        merge_files(merged_bin_path, next_bin, new_merged_bin)
        merge_idx_files(merged_idx_path, next_idx, new_merged_idx)

        # 更新路径
        merged_bin_path = new_merged_bin
        merged_idx_path = new_merged_idx

        # 更新文件名
        merged_bin = f'merged_{merged_bin.split(".")[0]}_{bin_file.split(".")[0]}.bin'
        merged_idx = f'merged_{merged_idx.split(".")[0]}_{idx_file.split(".")[0]}.idx'


if __name__ == '__main__':
    main()
