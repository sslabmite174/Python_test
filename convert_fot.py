# convert_fot.py

import sys
import os
import pathlib
import logging
import gc
import datetime
import warnings
import asammdf
import numpy as np
import pandas as pd
from asammdf.blocks.utils import MdfException
from tqdm import tqdm
from mf42csv import df_converter_v1, df_converter_v2  # Import your conversion functions
import re
warnings.simplefilter("ignore")

# サンプリング周期dict
mf4_interval_dic = {
    "GPS.mf4": ["100ms", "v1"],
    "Monitoring.mf4": ["100ms", "v1"],
    "XCP_AURIX.MF4": ["10ms", "v2"],
    "XCP_RCAR.MF4": ["200ms", "v2"],
}

mf4_dict_key_list = list(mf4_interval_dic.keys())

# logファイルの設定
sub_logger = logging.getLogger("log").getChild("MF4convert")

# timestampがバグっている場合、バグファイルとして扱うことにするため、start_timestampとのintervalを設定
day_interval = 1
# 外れ値除去用のパラメータ
minute_interval = 20


def main(mf4_file, FOT_PATH, OUTPUT_PATH):
    # mf4ファイルが読み込みできるかチェック
    try:
        v2mf = asammdf.mdf.MDF(mf4_file)
    except MdfException as me:
        print(f"Cannot read {mf4_file}: {me}")
        sub_logger.error(f"error mf4 file name : {mf4_file}")
        sub_logger.error(f"reason : {me}")
        return None

    # osごとのスラッシュを自動検知
    dirslash = os.path.sep
    # ファイルごとにパラメータ設定
    pickup_list = [i for i in mf4_dict_key_list if i in mf4_file]
    if len(pickup_list) == 1:
        # サンプリングタイムの指定
        sampling_sec_str = mf4_interval_dic[pickup_list[0]][0]
        # dataframe変換のための関数のタイプ指定
        def_type = mf4_interval_dic[pickup_list[0]][1]
    else:
        print("file name error")
        sub_logger.error("error mf4 file name : {}".format(mf4_file))
        sub_logger.error("reason : file name error")
        return None

    # mf4からdataframeを作成
    if def_type == "v1":
        mf4_df = df_converter_v1(v2mf, sampling_sec_str, mf4_file)
    elif def_type == "v2":
        mf4_df = df_converter_v2(v2mf, sampling_sec_str, mf4_file)

    # csvファイルパスを作成
    mf4_file = mf4_file.replace(FOT_PATH, OUTPUT_PATH)
    csv_file_name = re.sub(r".[Mm][Ff]4", ".csv", mf4_file.split(dirslash)[-1])
    csv_dir = os.path.dirname(mf4_file)
    os.makedirs(csv_dir, exist_ok=True)
    csv_file_path = os.path.join(csv_dir, csv_file_name)

    # if isinstance(mf4_df, pd.DataFrame):
        # csv保存
    mf4_df.to_csv(csv_file_path)

    # メモリ節約のための後処理
    del v2mf
    del mf4_df
    gc.collect()
    print(csv_file_path)
    return csv_file_path

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: convert_fot.py <input_file> <output_directory>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_directory = sys.argv[2]

    # Call main function with command-line arguments
    main(input_file, FOT_PATH, output_directory)
