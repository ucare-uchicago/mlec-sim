import os
import sys
import pandas as pd
import numpy as np

def compute_time_diff(df, col_name=None, group=False):
    if group is True:
        if type(col_name) == list:
            df['diff'] = df.groupby(col_name)['failure_time'].diff().dt.total_seconds()
        else:
            df['diff'] = df.groupby([col_name])['failure_time'].diff().dt.total_seconds()
    else:
        df['diff'] = df['failure_time'].diff().dt.total_seconds()
    return df

def burst_global_group(df, col_name, time=1800):
    grouped = df.groupby(col_name)
    res_df = pd.DataFrame()
    burst_id = -1
    for col, group in grouped:
        for index, row in group.iterrows():
            if row['diff'] == -1:
                burst_id += 1
                group.loc[index, 'burst_glob_id'] = burst_id
            elif row['diff'] > time:  # 30mins
                burst_id += 1
                group.loc[index, 'burst_glob_id'] = burst_id
            else:
                group.loc[index, 'burst_glob_id'] = burst_id
        res_df = pd.concat([res_df, group], axis=0)
    return res_df

def find_failure_group(df, col_name, time):
    # col_name is node_id or rack_id
    df['failure_time'] = pd.to_datetime(df['failure_time'])
    df = df.sort_values(by=['failure_time'])
    df = compute_time_diff(df, col_name, True)
    df['diff'] = df['diff'].fillna(value=-1)
    df = df.sort_values([col_name, 'failure_time'])
    df['burst_glob_id'] = -1
    df = burst_global_group(df, col_name, time)
    return df

def separate_failures(df):
    df1 = df.groupby(['burst_glob_id'])['disk_id'].count().reset_index()
    intra_failures = df[df['burst_glob_id'].isin(df1[df1['disk_id'] > 1]['burst_glob_id'].values)]
    non_intra_failures = df[~df['disk_id'].isin(intra_failures['disk_id'])]
    return (intra_failures, non_intra_failures)

def get_intra_failures(df):
    # Section 4.1 - Finding 1
    time = 30*60 # 30 mins
    res_df = find_failure_group(df, 'node_id', time)
    (intra_df, non_intra_df) = separate_failures(res_df)
    intra_df.to_csv("results/intra_node_failures_30mins.csv", index=False)
    intra_node = intra_df.groupby(['burst_glob_id'])['disk_id'].count().reset_index().rename(
            columns={'disk_id': 'group_size'}).groupby(['group_size']).count().reset_index().rename(
                    columns={'burst_glob_id': 'count'})
    intra_node['percent (%)'] = intra_node['group_size'] * intra_node['count'] / df.shape[0] * 100
    intra_node.to_csv("results/finding_1_node.csv", index=False)

    # res_df = find_failure_group(df, 'rack_id', time)
    # (intra_df, non_intra_df) = separate_failures(res_df)
    # intra_df.to_csv("results/intra_rack_failures_30mins.csv", index=False)
    # intra_rack = intra_df.groupby(['burst_glob_id'])['disk_id'].count().reset_index().rename(
    #         columns={'disk_id': 'group_size'}).groupby(['group_size']).count().reset_index().rename(
    #                 columns={'burst_glob_id': 'count'})
    # intra_rack['percent (%)'] = intra_rack['group_size'] * intra_rack['count'] / df.shape[0] * 100
    # intra_rack.to_csv("results/finding_1_rack.csv", index=False)


if __name__ == '__main__':
    prefix_dir = sys.argv[1]
    # try:
    #     os.mkdir("results/")
    # except Exception as e:
    #     print(e)
    # df = pd.read_csv(prefix_dir + "ssd_failure_tag.csv")
    df_topo = pd.read_csv(prefix_dir + "location_info_of_ssd.csv")

    # # Section 4.1 - Finding 1
    # get_intra_failures(df)
    df_topo

    by_rack = {}
    for index, row in df_topo.iterrows():
        disk = row['disk_id']
        node = row['node_id']
        rack = row['rack_id']
        if rack not in by_rack:
            by_rack[rack] = {}
        if node not in by_rack[rack]:
            by_rack[rack][node] = {}
        by_rack[rack][node][disk] = 1
    disk_counts = {}
    for rack in by_rack:
        disk_counts[rack] = 0
        for node in by_rack[rack]:
            disk_counts[rack] += len(by_rack[rack][node])
    print(disk_counts.values())