# coding: utf-8

import os
from datetime import datetime

import pandas as pd


def read_file(file_name):
    return pd.read_csv(file_name).dropna(how='any')


def get_files():
    files = os.listdir(os.curdir)
    csv_files = []
    for f in files:
        if f.endswith('csv'):
            csv_files.append(f)
    csv_files.sort()
    return csv_files


def get_duplicate_files(f, files):
    files.remove(f)
    check_files = list(files)
    check_files.sort()
    return check_files


def check_poetry(f):
    check_files = get_duplicate_files(f, set(get_files()))
    check_df = read_file(f)
    check_content = set(check_df['内容'].values.tolist())
    for _f in get_duplicate_files(f, set(get_files())):
        _df = read_file(_f)
        _content = set(_df['内容'].values.tolist())
        data = check_content & _content
        if data:
            print(data)


if __name__ == '__main__':
    for f in get_files():
        check_poetry(f)
