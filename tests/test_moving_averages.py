 
import datetime
import polars as pl

from src.s01_timing.moving_averages import (
    calculate_moving_averages_hard_coded,
    calculate_moving_averages_loop,
    calculate_moving_averages_eval,
    )


def test_calculate_moving_averages_hard_coded_01():
    """
    Test valid dataframe with 3 window sizes
    """

    df = pl.DataFrame({
        'timestamp': [
            datetime.datetime(2022, 2,  1, 10,  0, 0),
            datetime.datetime(2022, 2,  1, 10,  0, 1),
            datetime.datetime(2022, 2,  1, 10,  0, 2),
            datetime.datetime(2022, 2,  2,  9, 40, 0),
            datetime.datetime(2022, 3, 10,  9, 31, 0)],
        'data': [1, 2, 3, 4, 5]})

    correct_df = pl.DataFrame({
        'timestamp': [
            datetime.datetime(2022, 2,  1, 10,  0, 0),
            datetime.datetime(2022, 2,  1, 10,  0, 1),
            datetime.datetime(2022, 2,  1, 10,  0, 2),
            datetime.datetime(2022, 2,  2,  9, 40, 0),
            datetime.datetime(2022, 3, 10,  9, 31, 0)],
        'data': [1, 2, 3, 4, 5],
        'ma_1': [1, 2, 3, 4, 5],
        'ma_2': [None, 1.5, 2.5, 3.5, 4.5],
        'ma_3': [None, None, 2, 3, 4]})

    result = calculate_moving_averages_hard_coded(
        df, 
        data_colname='data')

    assert result.frame_equal(correct_df)


def test_calculate_moving_averages_loop_01():
    """
    Test valid dataframe with 3 window sizes
    """

    df = pl.DataFrame({
        'timestamp': [
            datetime.datetime(2022, 2,  1, 10,  0, 0),
            datetime.datetime(2022, 2,  1, 10,  0, 1),
            datetime.datetime(2022, 2,  1, 10,  0, 2),
            datetime.datetime(2022, 2,  2,  9, 40, 0),
            datetime.datetime(2022, 3, 10,  9, 31, 0)],
        'data': [1, 2, 3, 4, 5]})

    correct_df = pl.DataFrame({
        'timestamp': [
            datetime.datetime(2022, 2,  1, 10,  0, 0),
            datetime.datetime(2022, 2,  1, 10,  0, 1),
            datetime.datetime(2022, 2,  1, 10,  0, 2),
            datetime.datetime(2022, 2,  2,  9, 40, 0),
            datetime.datetime(2022, 3, 10,  9, 31, 0)],
        'data': [1, 2, 3, 4, 5],
        'ma_1': [1, 2, 3, 4, 5],
        'ma_2': [None, 1.5, 2.5, 3.5, 4.5],
        'ma_3': [None, None, 2, 3, 4]})

    result = calculate_moving_averages_loop(
        df, 
        data_colname='data',
        window_sizes=[1, 2, 3])

    assert result.frame_equal(correct_df)


def test_calculate_moving_averages_eval_01():
    """
    Test valid dataframe with 3 window sizes
    """

    df = pl.DataFrame({
        'timestamp': [
            datetime.datetime(2022, 2,  1, 10,  0, 0),
            datetime.datetime(2022, 2,  1, 10,  0, 1),
            datetime.datetime(2022, 2,  1, 10,  0, 2),
            datetime.datetime(2022, 2,  2,  9, 40, 0),
            datetime.datetime(2022, 3, 10,  9, 31, 0)],
        'data': [1, 2, 3, 4, 5]})

    correct_df = pl.DataFrame({
        'timestamp': [
            datetime.datetime(2022, 2,  1, 10,  0, 0),
            datetime.datetime(2022, 2,  1, 10,  0, 1),
            datetime.datetime(2022, 2,  1, 10,  0, 2),
            datetime.datetime(2022, 2,  2,  9, 40, 0),
            datetime.datetime(2022, 3, 10,  9, 31, 0)],
        'data': [1, 2, 3, 4, 5],
        'ma_1': [1, 2, 3, 4, 5],
        'ma_2': [None, 1.5, 2.5, 3.5, 4.5],
        'ma_3': [None, None, 2, 3, 4]})

    result = calculate_moving_averages_eval(
        df, 
        data_colname='data',
        window_sizes=[1, 2, 3])

    assert result.frame_equal(correct_df)

