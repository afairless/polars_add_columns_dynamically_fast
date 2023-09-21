
import time
import datetime
import numpy as np
import polars as pl
from pathlib import Path


def seconds_to_formatted_time_string(seconds: float) -> str:
    """
    Given the number of seconds, returns a formatted string showing the time
        duration
    """

    hour = int(seconds / (60 * 60))
    minute = int((seconds % (60 * 60)) / 60)
    second = seconds % 60

    return '{}:{:>02}:{:>05.2f}'.format(hour, minute, second)


def print_loop_status_with_elapsed_time(
    the_iter: int, every_nth_iter: int, total_iter: int, start_time: float):
    """
    Prints message providing loop's progress for user

    :param the_iter: index that increments by 1 as loop progresses
    :param every_nth_iter: message should be printed every nth increment
    :param total_iter: total number of increments that loop will run
    :param start_time: starting time for the loop, which should be
        calculated by 'import time; start_time = time.time()'
    """

    current_time = time.ctime(int(time.time()))

    every_nth_iter_integer = max(round(every_nth_iter), 1)

    if the_iter % every_nth_iter_integer == 0:
        print('Processing loop iteration {i} of {t}, which is {p:0f}%, at {c}'
              .format(i=the_iter + 1,
                      t=total_iter,
                      p=(100 * (the_iter + 1) / total_iter),
                      c=current_time))
        elapsed_time = time.time() - start_time

        print('Elapsed time: {}'.format(seconds_to_formatted_time_string(
            elapsed_time)))


def generate_data(
    start_datetime: datetime.datetime=datetime.datetime(1970, 1, 1, 0, 0, 0), 
    end_datetime: datetime.datetime=datetime.datetime(1970, 1, 1, 0, 1, 0),
    interval: str='1s',
    column_n: int=1):
    """
    Generates of Polars DataFrame with a 'timestamp' column and a provided
        number ('column_n') of columns of standard-normally distributed data
    For acceptable input values for 'interval', see:
        https://pola-rs.github.io/polars/py-polars/html/reference/expressions/api/polars.date_range.html
    """

    timestamp = pl.date_range(
        start_datetime, end_datetime, interval, eager=True).alias('timestamp')

    data = [
        pl.Series(np.random.normal(size=len(timestamp))).alias(f'data{i}') 
        for i in range(column_n)]

    df = pl.concat(
        [pl.DataFrame(timestamp), pl.DataFrame(data)], how='horizontal')

    return df


def calculate_moving_averages_hard_coded(
    df: pl.DataFrame, data_colname: str='data') -> pl.DataFrame:
    """
    Calculates moving averages of 'data_colname' column in dataframe 'df' with
        3 window sizes, so that each window size corresponds to a single column 
        of moving averages
    The 3 window sizes are hard/statically-coded as 1, 2, and 3 to serve as an
        example to contrast with the dynamically-coded functions that follow
    """

    mas_df = df.with_columns(
        ma_1=pl.col(data_colname).rolling_mean(window_size=1),
        ma_2=pl.col(data_colname).rolling_mean(window_size=2),
        ma_3=pl.col(data_colname).rolling_mean(window_size=3))

    return mas_df


def calculate_moving_averages_loop(
    df: pl.DataFrame, 
    data_colname: str='data',
    window_sizes: list[int]=[1, 2],
    ) -> pl.DataFrame:
    """
    Calculates moving averages of 'data_colname' column in dataframe 'df' with
        window sizes in 'window_sizes', so that each window size corresponds to
        a single column of moving averages
    Calculates each column of moving averages sequentially in a 'for' loop
    """

    mas = [
        df[data_colname].rolling_mean(window_size=w).alias(f'ma_{w}') 
        for w in window_sizes]

    mas_df = pl.concat([df, pl.DataFrame(mas)], how='horizontal')

    return mas_df


def calculate_moving_averages_eval(
    df: pl.DataFrame, 
    data_colname: str='data',
    window_sizes: list[int]=[1, 2],
    ) -> pl.DataFrame:
    """
    Calculates moving averages of 'data_colname' column in dataframe 'df' with
        window sizes in 'window_sizes', so that each window size corresponds to
        a single column of moving averages
    Creates single query string to calculate the columns of moving averages, 
        then executes it, allowing Polars dataframe library to calculate columns
        in parallel
    """

    ##################################################
    # composes command to create output dataframe 'mas_df' as a string, so that
    #   a variable number of moving averages can be calculated simultaneously,
    #   instead of using a loop to calculate them sequentially
    ##################################################
    #
    # COMMAND TEMPLATE:
    #
    # mas_df = df.with_columns(
    #     ma=pl.col('data').rolling_mean(window_size=window_size),)
    ##################################################

    mas_df_create_command = [
        "pl.DataFrame("
            "[df['timestamp'], df[data_colname]]).with_columns("]

    for window_size in window_sizes:

        ma_cmd = (f"ma_{window_size}=pl.col(data_colname).rolling_mean("
            f"window_size={window_size}),")
        mas_df_create_command.append(ma_cmd)

    mas_df_create_command.append(')')

    mas_df = eval(''.join(mas_df_create_command))

    return mas_df


def time_function(repeat_n, time_func, func, *args) -> float:
    """
    Times function ('func') with provided arguments using provided timing
        function ('time_func') 
    Runs timing 'repeat_n' times and returns the lowest/fastest time
    """

    min_time = float('inf')
    loop_start_time = time.time()
    for idx in range(repeat_n):

        print_loop_status_with_elapsed_time(
            idx, repeat_n//10, repeat_n, loop_start_time)

        start_time = time_func()
        result = func(*args)
        end_time = time_func()

        elapsed_time = end_time - start_time
        if elapsed_time < min_time:
            min_time = elapsed_time

    return min_time


def main():
    """
    Times calculations of moving averages in a Polars DataFrame by two different 
        methods:

        1) Calculating each column of moving averages sequentially in a 'for' 
            loop
        2) Creating a single query string to calculate the columns of moving 
            averages, then executing it to allow Polars dataframe library to 
            parallelize the calculations
    """


    # GENERATE DATA
    ##################################################

    start_datetime = datetime.datetime(2020, 1, 1, 0, 0, 0)
    end_datetime = datetime.datetime(2020, 2, 1, 0, 0, 0)
    interval = '1s'

    data_column_n = 1
    df = generate_data(start_datetime, end_datetime, interval, data_column_n)
    window_sizes = [i for i in range(1, 200)]


    # TIME CALCULATIONS OF MOVING AVERAGES
    ##################################################

    timing_repeat_n = 400

    print('\nRecording elapsed time of calculating moving averages by for '
        'loop\n')
    elapsed_time_ma_loop = time_function(
        timing_repeat_n, time.perf_counter, 
        calculate_moving_averages_loop, df, 'data0', window_sizes)

    print('\nRecording elapsed time of calculating moving averages by '
        'evaluating string query\n')
    elapsed_time_ma_eval = time_function(
        timing_repeat_n, time.perf_counter, 
        calculate_moving_averages_eval, df, 'data0', window_sizes)

    print('\nRecording system+user time of calculating moving averages by '
        'for loop\n')
    system_user_time_ma_loop = time_function(
        timing_repeat_n, time.process_time, 
        calculate_moving_averages_loop, df, 'data0', window_sizes)

    print('\nRecording system+user time of calculating moving averages by '
        'evaluating string query\n')
    system_user_time_ma_eval = time_function(
        timing_repeat_n, time.process_time, 
        calculate_moving_averages_eval, df, 'data0', window_sizes)


    # SAVE RESULTS
    ##################################################

    results_dict = {
        'moving_averages_calculation_method': [
            'by_for_loop', 'by_eval_string_query'],
        'elapsed_time': [elapsed_time_ma_loop, elapsed_time_ma_eval],
        'system_user_time': [
            system_user_time_ma_loop, system_user_time_ma_eval]}

    results_df = pl.DataFrame(results_dict)

    output_path = Path.cwd() / 'results'
    output_path.mkdir(parents=True, exist_ok=True)
    output_filepath = output_path / 'results.csv'
    results_df.write_csv(output_filepath)


if __name__ == '__main__':
    main()

