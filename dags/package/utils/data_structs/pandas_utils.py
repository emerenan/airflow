import pandas as pd


def get_first_element_of_data_frame(series: pd.Series):
    """
        Get the first Non NaN element from a Pandas Series
        Fails if no element is found
        
        :param series: Pandas series to retrieve
    """
    return series.loc[~series.isnull()].iloc[0]
