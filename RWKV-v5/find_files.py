import os
import argparse

def find_parquet_files(directory):
    parquet_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.parquet'):
                full_path = os.path.join(root, file)
                parquet_files.append(full_path)
    return parquet_files

def write_parquet_list_to_file(parquet_files, output_file):
    with open(output_file, 'w') as f:
        f.write(' '.join(parquet_files))

def main():
    parser = argparse.ArgumentParser(description='Find all parquet files in a directory and its subdirectories.')
    parser.add_argument('directory', type=str, help='The directory to search for parquet files.')
    args = parser.parse_args()

    directory = args.directory
    parquet_files = find_parquet_files(directory)

    output_file = 'parquet_list.txt'
    write_parquet_list_to_file(parquet_files, output_file)

    print(f'Found {len(parquet_files)} parquet files. List written to {output_file}.')

if __name__ == '__main__':
    main()
