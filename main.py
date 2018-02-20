import os, glob
import pandas as pd

atlanta_records = pd.read_csv('atlanta.csv', header=None)
chicago_records = pd.read_csv('chicago.csv', header=None)
dallas_records = pd.read_csv('dallas.csv', header=None)
oakland_records = pd.read_csv('oakland.csv', header=None)

# get the current directort
current_dir = os.getcwd()
filenames = glob.glob(current_dir + "/*.csv")

def add_source_column(frame, col_name, file_path):
    # if file_path is a filepath for example /Users/test/Dev/csv_panda/dallas.csv
    # the command file_path.split('/') is used to split the pathname with the / as delimiter
    # into an array of strings, the result should look like this
    # ['Users', 'test','Dev', 'csv_panda','dallas.csv'], the [-1] is used to get the last element in the array
    # which happens to be name of the csv itself, which usually is the name of the city and the file format.
    # Next we call the `split` method on the this and with the '.' as delimiter. This should give us something
    # like ['dallas', 'csv']. Then we access the first element in the array which is the city itself and the result
    # should be 'dallas'
    frame[col_name] = file_path.split('/')[-1].split('.')[0]

def get_year_from_quarter(quarter):
    no_of_years = int(int(quarter)/4)
    year = 1970 + no_of_years
    return year

def add_year_column(row):
    return get_year_from_quarter(row[2])

def append_year(frame, col):
    quarters = frame[col]
    column_name = col + "_year"
    frame[column_name] = frame.apply(add_year_column, axis=1)

def add_year2_column(row):
    return get_year_from_quarter(row[3])

def append_year2(frame, col):
    quarters = frame[col]
    column_name = col + "_year"
    frame[column_name] = frame.apply(add_year2_column, axis=1)

# 1. Complete the function below to read multiple files into a single pandas
#    data frame.  This function takes three arguments:
#    - a string 'dir_path'
#    - an array of strings that will be used as column names
#    - a string name of a new column
#    Your function should return a single DataFrame with the column names
#    as specified in the column_names argument.  The resulting frame should
#    be indexed with unique integers, starting at 0.  For now, you can ignore
#    the third argument.
#    A sample data row is:
#    0     2550     3300       38   54
def read_all_files(dir_path, column_names, source_column=''):
    data_files = glob.glob(dir_path + "/*.csv")
    frames = []
    for f in data_files:
        #FILL THIS IN
        #CREATE A NEW DATAFRAME CALLED new_frame FROM FILE f and columns
        new_frame = pd.DataFrame(data = pd.read_csv(f, header=None))
        new_frame.columns = column_names
        add_source_column(new_frame, 'city', f)
        append_year(new_frame, 'purchase_date')
        append_year2(new_frame, 'sales_date')
        frames.append(new_frame)
    return pd.concat(frames, ignore_index=True)


def fast_sales(frame, period):
    result = frame.query('sales_date=={0}'.format(period))
    return result

def calculate_profit(row):
    return row[1] - row[0]

def profit_on_fast_sales(frame,period):
    sales = fast_sales(frame, period)
    sales["profit"] = frame.apply(calculate_profit, axis=1)
    return sales["profit"].mean()

def sales_by_column(frame, col_name):
    return frame.groupby('city').size()
