import datetime
import gc
import glob
import logging
import os
import re
import time
import warnings
from glob import glob

import asammdf
import numpy as np
import pandas as pd
from asammdf.blocks.utils import MdfException
from tqdm import tqdm

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


# mf4の物理値をいい感じにdataframeに変換する関数(v1)
def df_converter_v1(v2mf, sampling_sec_str, mf4_file):
    # 計測開始時刻を格納、この時刻からの経過時間をそれぞれの計測時間として算出するため
    start_timestamp = v2mf.header.start_time
    # タイムゾーン削除
    start_timestamp = start_timestamp.replace(tzinfo=None)
    # groupの数を格納、forループでgroupごとにデータを抽出
    groups = v2mf.info()["groups"]
    convert_df_list = []
    for group_num in range(groups):
        # groupの中からデータを抽出できるかチェック
        try:
            v2mf.get(group=group_num, index=1)
        except MdfException:
            # print("can not get group {} data!!".format(group_num))
            continue
        # データが存在するかチェック
        timestamp_num = len(v2mf.get(group=group_num, index=0))
        if timestamp_num == 0:
            # データなければ何もしない
            # print("No data")
            continue
        else:
            signal_num = v2mf.info()["group {}".format(group_num)]["channels count"]
            # 各信号データ取得
            # print("signal num : ", signal_num-1)
            df_list = []
            # signalごとにtimestampが異なるためfor文でsignalごとにデータ抽出を行う
            # for idx in tqdm(range(1, signal_num)):
            for idx in range(1, signal_num):
                # print(idx)
                timestamps_list = v2mf.get(group=group_num, index=idx).timestamps
                # timestampsがバグってる場合あり、そのときはデータが壊れていると判断して変換中止
                try:
                    time_list = [
                        start_timestamp + datetime.timedelta(seconds=stamp)
                        for stamp in timestamps_list
                    ]
                except OverflowError as oe:
                    print(
                        "Python int too large to convert to C int {} file!!".format(
                            mf4_file
                        )
                    )
                    sub_logger.error("error mf4 file name : {}".format(mf4_file))
                    sub_logger.error("reason : {}".format(oe))
                    return None
                # 変数名取得
                column_name = v2mf.get(group=group_num, index=idx).name
                # 物理値取得
                samples_list = v2mf.get(group=group_num, index=idx).samples.tolist()
                # 取得したsampleがtuple形式の場合あり、if文で分岐処理
                # sampleがvec/matlixの場合
                if type(samples_list[0]) == tuple:
                    # vec形式のデータの場合
                    if len(np.shape(samples_list[0][0])) == 1:
                        column_name = [
                            column_name + ".{}".format(i)
                            for i in range(np.shape(samples_list[0][0])[0])
                        ]
                        # matlix形式のデータの場合
                    elif len(np.shape(samples_list[0][0])) == 2:
                        column_name = [
                            column_name + ".{0}.{1}".format(j, i)
                            for i in range(np.shape(samples_list[0][0])[0])
                            for j in range(np.shape(samples_list[0][0])[1])
                        ]
                    samples_list = [rawdata[0].flatten() for rawdata in samples_list]
                    df = pd.DataFrame(
                        samples_list, index=time_list, columns=column_name
                    )
                # sampleが数値の場合
                else:
                    df = pd.DataFrame(
                        samples_list, index=time_list, columns=[column_name]
                    )
                # 明らかにtimestampがバグっている場合、resamplingでメモリエラーが発生しちゃうので、バグファイルとして変換を終わらせる例外処理(timestampsのmaxがstart_timestampから1日ずれてる)
                if max(df.index) - start_timestamp > datetime.timedelta(
                    days=day_interval
                ):
                    print(
                        "timestamp bug, interval is over {} day!!!".format(day_interval)
                    )
                    sub_logger.error("error mf4 file name : {}".format(mf4_file))
                    sub_logger.error(
                        "reason : timestamp bug, interval is over {} day!!!".format(
                            day_interval
                        )
                    )
                    return None
                # 外れ値除去
                df = df[
                    df.index - start_timestamp
                    < datetime.timedelta(days=minute_interval)
                ]
                # 指定した周期でリサンプリング
                df = df.resample(sampling_sec_str, label="right", closed="right").last()
                df_list.append(df)
            # 1 groupのdataframeを作成
            df_list = pd.concat(df_list, axis=1)
            # print("data shape : ", df_list.shape)
            convert_df_list.append(df_list)
    if len(convert_df_list) == 0:
        sub_logger.error("error mf4 file name : {}".format(mf4_file))
        sub_logger.error("reason : no data")
        return None
    # mf4ファイルから抽出できる物理値を格納したdataframeを作成
    convert_df_list = pd.concat(convert_df_list, axis=1)
    convert_df_list.fillna(method="ffill", inplace=True)
    convert_df_list.index.name = "Time"
    # print(convert_df_list.shape)
    return convert_df_list


# mf4の物理値をいい感じにdataframeに変換する関数(v2)
def df_converter_v2(v2mf, sampling_sec_str, mf4_file):
    # 計測開始時刻を格納、この時刻からの経過時間をそれぞれの計測時間として算出するため
    start_timestamp = v2mf.header.start_time
    # タイムゾーン削除
    start_timestamp = start_timestamp.replace(tzinfo=None)
    # 各groupのindexをkeyとしたchannel名のリストdictを作成
    coldict = {}
    for idx in range(1, v2mf.info()["groups"]):
        onegroup_info = v2mf.info()["group {}".format(idx)]
        if onegroup_info["cycles"] == 0:
            continue
        channel_count = onegroup_info["channels count"]
        colname_list = [
            re.findall('".*"', onegroup_info["channel {}".format(cnum)])[0][1:-1]
            for cnum in range(1, channel_count)
            if "type=VALUE" in onegroup_info["channel {}".format(cnum)]
        ]
        if len(colname_list) > 0:
            coldict[idx] = colname_list
    # 各groupのdataframeを作成
    df_list = []
    for key, value in coldict.items():
        # timestampsがバグってる場合あり、そのときはデータが壊れていると判断して変換中止
        try:
            pre_df = v2mf.get_group(key, time_as_date=True, raw=True)
        except Exception as e:
            sub_logger.error("error mf4 file name : {}".format(mf4_file))
            sub_logger.error("reason : {}".format(e))
            return None
        # 明らかにtimestampがバグっている場合、resamplingでメモリエラーが発生しちゃうので、バグファイルとして変換を終わらせる例外処理(timestampsのmaxがstart_timestampから1日ずれてる)
        if max(pre_df.index) - start_timestamp > datetime.timedelta(days=day_interval):
            print("timestamp bug, interval is over {} day!!!".format(day_interval))
            sub_logger.error("error mf4 file name : {}".format(mf4_file))
            sub_logger.error(
                "reason : timestamp bug, interval is over {} day!!!".format(
                    day_interval
                )
            )
            return None
        # 外れ値除去
        pre_df = pre_df[
            pre_df.index - start_timestamp < datetime.timedelta(days=minute_interval)
        ]
        # 指定した周期でリサンプリング
        pre_df = pre_df.resample(sampling_sec_str, label="right", closed="right").last()
        pre_df.fillna(method="ffill", inplace=True)
        # vec/mtxで格納されているカラムを展開
        df = pd.DataFrame([], index=pre_df.index)
        # for colname in tqdm(pre_df.columns):
        for colname in pre_df.columns:
            values_list = pre_df[colname].to_list()
            fin_value = values_list[-1]
            # vec/mtx形式ではない場合、そのままカラム名に物理地を格納
            if type(fin_value) != np.ndarray:
                df[colname] = pre_df[colname]
            # vec/mtx形式の場合、arrayの中身を展開して、変数名.0 .1 ...のようにナンバリングを行う
            else:
                if len(fin_value.shape) == 1:
                    colname = [
                        colname + ".{}".format(i) for i in range(fin_value.shape[0])
                    ]
                    df[colname] = values_list
                elif len(fin_value.shape) == 2:
                    colname = [
                        colname + ".{0}.{1}".format(j, i)
                        for i in range(fin_value.shape[0])
                        for j in range(fin_value.shape[1])
                    ]
                    values_list = [rawdata.flatten() for rawdata in values_list]
                    df[colname] = values_list
        df_list.append(df)
    if len(df_list) == 0:
        sub_logger.error("error mf4 file name : {}".format(mf4_file))
        sub_logger.error("reason : no data")
        return None
    # mf4ファイルから抽出できる物理値を格納したdataframeを作成
    df_list = pd.concat(df_list, axis=1)
    df_list.fillna(method="ffill", inplace=True)
    df_list.index.name = "Time"
    # print(df_list.shape)
    return df_list


def main(mf4_file, FOT_PATH, OUTPUT_PATH):
    # print("======================================================")
    # mf4ファイルが読み込みできるかチェック
    try:
        v2mf = asammdf.mdf.MDF(mf4_file)
    except MdfException as me:
        print("can not get {} file!!".format(mf4_file))
        sub_logger.error("error mf4 file name : {}".format(mf4_file))
        sub_logger.error("reason : {}".format(me))
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
    csv_file_name = re.sub(".[Mm][Ff]4", ".csv", mf4_file.split(dirslash)[-1])
    csv_dir = os.path.dirname(mf4_file)
    os.makedirs(csv_dir, exist_ok=True)
    csv_file_path = os.path.join(csv_dir, csv_file_name)
    if type(mf4_df) == pd.core.frame.DataFrame:
        # csv保存
        mf4_df.to_csv(csv_file_path)
    # メモリ節約のための後処理
    del v2mf
    del mf4_df
    gc.collect()
    return csv_file_path