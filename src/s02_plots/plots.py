# /usr/bin/env python3

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


def main():

    path = Path.cwd() / 'results'
    filepath = path / 'results.csv'

    df = pd.read_csv(filepath)
    assert isinstance(df, pd.DataFrame)

    # horizontal bar plot plots first row of dataframe at bottom of plot; 
    #   reverse the row indices so that the "first" row is at the top
    reverse_idx = list(range(len(df)))[::-1]
    df = df.iloc[reverse_idx, :]

    
    # PLOT ELAPSED TIMES ONLY 
    ##################################################

    labels_dict = {
        'by_for_loop': 'Loop One-by-One',
        'by_eval_string_query': 'String Query, Eval Function'}
    labels = [
        labels_dict[e] + '\nElapsed Time'
        for e in df.loc[:, 'moving_averages_calculation_method']]

    plt.grid(axis='x', zorder=0)
    plt.barh(
        labels,
        df['elapsed_time'], 
        zorder=2, 
        color=['darkorange', 'blue'])
    plt.xlabel('Run time (sec)')
    plt.tight_layout()

    output_filepath = path / 'elapsed_time_plot.png'
    plt.savefig(output_filepath)

    notebook_path = Path.cwd() / 'notebook' / 'img'
    notebook_path.mkdir(exist_ok=True)

    output_filepath = notebook_path / 'elapsed_time_plot.png'
    plt.savefig(output_filepath)

    plt.clf()
    plt.close()


    # PLOT ALL TIMES:  ELAPSED AND SYSTEM+USER TIMES
    ##################################################

    labels02 = labels + [''] + labels
    labels02[0] = labels02[0].replace('Elapsed Time', 'System+User Time')
    labels02[1] = labels02[1].replace('Elapsed Time', 'System+User Time')

    times = (
        df['system_user_time'].to_list() + 
        [0] +
        df['elapsed_time'].to_list())

    plt.grid(axis='x', zorder=0)
    plt.barh(
        labels02,
        times,
        zorder=2, 
        color=['darkorange', 'blue', 'gray','darkorange', 'blue'])
    plt.xlabel('Run time (sec)')
    plt.tight_layout()

    output_filepath = path / 'all_times_plot.png'
    plt.savefig(output_filepath)

    notebook_path = Path.cwd() / 'notebook' / 'img'
    notebook_path.mkdir(exist_ok=True)

    output_filepath = notebook_path / 'all_times_plot.png'
    plt.savefig(output_filepath)

    plt.clf()
    plt.close()


if __name__ == '__main__':
    main()
