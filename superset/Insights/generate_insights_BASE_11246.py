import sys
from os import path
import time
from multiprocessing import Process, Queue
from dateutil.parser import parse

import pandas as pd

from insight_exceptions import FileNotFoundError,InsightException
import Insights as ins
from graphs import generate_graphs
from insights_correlation import generate_correlation_insights

start_time = time.time()

def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.
    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try: 
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False

def classify_attributes_by_threshold_and_get_unique_categorical_data(df,threshold):
    data_dict = {}
    data = pd.Series([]) 
    categorical_attributes = [] 
    measure_attributes = []
    timeseries_attributes = {}
    df_length = len(df)

    for attribute in df.columns:
        # if float then measure attribute 
        if df[attribute].dtypes=='float64':
            measure_attributes.append(attribute)
            continue

        data_without_duplicates = df[attribute].unique()
        if len(data_without_duplicates) > 0:
            if is_date(str(data_without_duplicates[0])):
                timeseries_attributes[attribute] = attribute

        if len(data_without_duplicates)/df_length<threshold:
             data_dict[attribute] = data_without_duplicates
             categorical_attributes.append(attribute)
        else:
            measure_attributes.append(attribute)
        
    return pd.Series(data_dict),categorical_attributes,measure_attributes,timeseries_attributes



def load_data(filename):

    #TODO: add check if filetype is csv 
    if not path.isfile(filename):
        raise FileNotFoundError(filename)
    
    data_frame = pd.read_csv(filename, encoding= 'unicode_escape')
    print('Loading of data into dataframe completed')
    return data_frame
    
def data_preprocessing(data_frame,threshold):
    unique_data, categorical_attributes, measure_attributes, timeseries_attributes = classify_attributes_by_threshold_and_get_unique_categorical_data(data_frame,threshold)
    print('Sucessfully generate_insights classified the categorical and measure attributes')
    categorical_index = {}
    counter = 0
    for category in categorical_attributes:
        categorical_index[category] = counter
        counter = counter + 1    
    return unique_data,categorical_attributes,measure_attributes,categorical_index,timeseries_attributes
    


def generate_insights(filename,threshold=0.3):
    print('Generating insights for the file ' +  str(filename))
    data_frame = load_data(filename)
    unique_data,categorical_attributes,measure_attributes,categorical_index,timeseries_attributes = data_preprocessing(data_frame,threshold)

    # Multi processing for correlation
    # TODO: Change logic based on the number of cores for correlation

    out_queue = Queue()
    correlation_p = Process(target=generate_correlation_insights, args=(data_frame,measure_attributes,out_queue))
    correlation_p.start()
    print(categorical_attributes, measure_attributes)
    tau = 2
    top_k = 40
    maxOneInsPerSub = True
    print(timeseries_attributes)
    result = ins.Insights(data_frame, tau, top_k, categorical_attributes, measure_attributes, timeseries_attributes, maxOneInsPerSub)
    result = sorted(result, key=lambda elem: elem[0], reverse=True)
    # print("\n")
    # for r in result:
    #     print(r, "\n")
    corr_result = out_queue.get()
    if correlation_p.is_alive():
        correlation_p.join()
    print('Generating insight graph')

    print("--- %s Minutes ---" % str(round(float((int(time.time()) - int(start_time)) /60), 2)))

    generate_graphs(result,categorical_index,top_k,corr_result,filename)
    print("--- %s Minutes ---" % str(round(float((int(time.time()) - int(start_time)) /60), 2)))


    

if __name__ == "__main__" :
    if len(sys.argv)==2:
        generate_insights(sys.argv[1])
    else:
        print('Usage is\n   python3 generate_insights.py <filename.csv>')