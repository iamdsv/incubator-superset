import sys
from os import path
import time
from multiprocessing import Process, Queue
from dateutil.parser import parse
import json 

import pandas as pd

from superset.Insights.insight_exceptions import FileNotFoundError,InsightException, ConfigFileError
import superset.Insights.Insights as ins
from superset.Insights.graphs import generate_graphs
from superset.Insights.insights_correlation import generate_correlation_insights
import numpy as np
import warnings
np.seterr(divide='ignore', invalid='ignore')
start_time = time.time()
warnings.filterwarnings('ignore', r'divide by zero encountered in true_divide')

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

def classify_attributes_by_user_config(dataframe,filename):

    data_dict = {}
    data = pd.Series([]) 
    categorical_attributes = [] 
    measure_attributes = []
    discarded_attributes = []
    timeseries_attributes = {}
    timeseries_list = []

    if not path.isfile(filename):
        raise FileNotFoundError(filename)
    cfile = open(filename) 

    try:
        data = json.load(cfile) 
    except json.JSONDecodeError:
        raise ConfigFileError(filename)

    try:
        c_arr = data['categorical_attributes']
    except KeyError:
        raise CategoricalAttrMissing()
        pass 
    try:
        m_arr = data['measure_attributes']
    except KeyError:
        print('Measure attributes are not configured')
        pass 

    try:
        timeseries_list = data['time_attributes']
    except KeyError:
        print('Time Series attributes are not configured')
        pass 
    for attribute in c_arr:

        #TODO: Convert this to dict and then check 
        if attribute in dataframe.columns:
            categorical_attributes.append(attribute)

        else:
            print('Categorical attributes not present', attribute)

        if len(categorical_attributes)==0:
            print('No categorical attributes')
            raise CategoricalAttrMissing() 

    for attribute in m_arr:
        if attribute in dataframe.columns:
            measure_attributes.append(attribute)
        else:
            print('Measure attribute not present', attribute)
    
    
    for attribute in timeseries_list:
        if attribute in categorical_attributes:
            timeseries_attributes[attribute] = attribute

    return pd.Series(data_dict),categorical_attributes,measure_attributes,timeseries_attributes,None

def classify_attributes_by_threshold_and_get_unique_categorical_data(df,threshold):
    data_dict = {}
    data = pd.Series([]) 
    categorical_attributes = [] 
    measure_attributes = []
    discarded_attributes = []
    timeseries_attributes = {}
    df_length = len(df)

    for attribute in df.columns:
        
        data_without_duplicates = df[attribute].unique()
        unique_factor = len(data_without_duplicates)/df_length
        inv_threshold = 1 - threshold

        

        if df[attribute].dtypes=='object':
            try:
                df[attribute] = df[attribute].str.replace(",","").astype(int)
            except ValueError:
                try:
                    df[attribute] = df[attribute].str.replace(",","").astype(float)
                except ValueError:
                    if unique_factor<threshold:
                        categorical_attributes.append(attribute)
                        if len(data_without_duplicates) > 0 and is_date(str(data_without_duplicates[0])):
                            timeseries_attributes[attribute] = attribute
                    else:
                        discarded_attributes.append(attribute)

        if df[attribute].dtypes=='float64':
            if unique_factor>=inv_threshold:
                measure_attributes.append(attribute)
            else:
                discarded_attributes.append(attribute)

        elif df[attribute].dtypes=='int64':
            if unique_factor>=inv_threshold:
                measure_attributes.append(attribute)
            else:
                categorical_attributes.append(attribute)
                data_dict[attribute] = data_without_duplicates

        
        
    return pd.Series(data_dict),categorical_attributes,measure_attributes,timeseries_attributes,discarded_attributes



def load_data(filename):

    #TODO: add check if filetype is csv 
    if not path.isfile(filename):
        raise FileNotFoundError(filename)
    
    data_frame = pd.read_csv(filename)
    print('Loading of data into dataframe completed')
    return data_frame
    
def data_preprocessing(data_frame,config_file,threshold):
    data_frame.columns = [col.strip() for col in data_frame.columns]
    if config_file is None:
        unique_data, categorical_attributes, measure_attributes, timeseries_attributes,discarded_attributes = classify_attributes_by_threshold_and_get_unique_categorical_data(data_frame,threshold)
    else:
        unique_data, categorical_attributes, measure_attributes ,timeseries_attributes,discarded_attributes = classify_attributes_by_user_config(data_frame,config_file)

    print('Sucessfully generate_insights classified the categorical and measure attributes')
    print(categorical_attributes,measure_attributes, timeseries_attributes,discarded_attributes)
    
    categorical_index = {}
    counter = 0
    for category in categorical_attributes:
        categorical_index[category] = counter
        counter = counter + 1    
    return unique_data,categorical_attributes,measure_attributes,categorical_index,timeseries_attributes,discarded_attributes
    


def generate_insights(filename,datasource_id,config_file=None,threshold=0.3):
    print('Generating insights for the file ' +  str(filename))
    data_frame = load_data(filename)
    unique_data,categorical_attributes,measure_attributes,categorical_index,timeseries_attributes,discarded_attributes = data_preprocessing(data_frame,config_file,threshold)
    

    # Multi processing for correlation
    # TODO: Change logic based on the number of cores for correlation

    out_queue = Queue()
    correlation_p = Process(target=generate_correlation_insights, args=(data_frame,measure_attributes,out_queue))
    correlation_p.start()
    tau = 2
    top_k = 40
    maxOneInsPerSub = True
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

    generated_filename = generate_graphs(result,categorical_index,top_k,corr_result,filename, datasource_id)
    print("--- %s Minutes ---" % str(round(float((int(time.time()) - int(start_time)) /60), 2)))
    return generated_filename

    

if __name__ == "__main__" :
    if len(sys.argv)==3:
        generate_insights(sys.argv[1],sys.argv[2])
    elif len(sys.argv)==2:
        generate_insights(sys.argv[1])
    else:
        print('Usage is\n   python3 generate_insights.py <filename.csv>')