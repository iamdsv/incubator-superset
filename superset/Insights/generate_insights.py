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


user_configured_extractors = []
user_configured_extractors_desc = {}
depth1_corr = False

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
    m_arr = []

    global depth1_corr
    
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
        # correlation between measures can be calculated even if no categorical variables
        print('Categorical attributes are not configured')

    try:
        m_arr = data['measure_attributes']
    except KeyError:
        print('Measure attributes are not configured')

    try:
        discarded_attributes = data['discarded_attributes']
    except KeyError:
        print('Discarded attributes are not configured')

    try:
        timeseries_list = data['time_attributes']
    except KeyError:
        print('Time Series attributes are not configured')

    try:
         depth1_corr = data['depth1_corr']
    except KeyError:
        print('Depth1_corr is not configured')

    for attribute in c_arr:

        if attribute in dataframe.columns:
            categorical_attributes.append(attribute)
            data_dict[attribute] = dataframe[attribute].unique()

        else:
            print('Categorical attributes not present', attribute)

    if len(categorical_attributes)==0:
        print('No categorical attributes')

    for attribute in m_arr:
        if attribute in dataframe.columns:
            attr_dt = dataframe[attribute].dtype
            if attr_dt==np.object:
                try:
                    dataframe[attribute] = dataframe[attribute].str.replace(",","").astype(int)
                except ValueError:
                    try:
                        dataframe[attribute] = dataframe[attribute].str.replace(",","").astype(float)
                    except ValueError:
                        try:
                            dataframe[attribute] = dataframe[attribute].str.replace("%","").astype(float)
                        except ValueError:
                            pass
            
            attr_dt = dataframe[attribute].dtype

            if attr_dt==np.int64 or attr_dt==np.float64:
                measure_attributes.append(attribute)
            else:
                print('Measure attribute wrongly configured', attribute,attr_dt)
        else:
            print('Measure attribute wrongly configured', attribute)
    
    # if len(measure_attributes)==0:
    #     print('No measure attributes present. Cannot mine insights')
    #     raise MeasureMissing()
    
    for attribute in timeseries_list:
        if attribute in categorical_attributes:
            # no checks as the user can give in format 2014-2015 which is still a time attribute    
            timeseries_attributes[attribute] = attribute

    try:
        configured_extractors = data['extractors']
        set_user_defined_extractors(configured_extractors)

    except KeyError:
        print('User did not configure extractor')

    return pd.Series(data_dict),categorical_attributes,measure_attributes,timeseries_attributes,discarded_attributes

def set_user_defined_extractors(configured_extractors):
    
    global user_configured_extractors
    for i in range(0,len(configured_extractors)):
        extractor_name = configured_extractors[i]['extractor_name']
        try:
            is_ordinal = configured_extractors[i]['is_ordinal']
        except KeyError:
            is_ordinal = False

        extractor_expression = configured_extractors[i]['expression']
        try:
            exec(extractor_expression)
        except:
            print('Invalid extractor expression',extractor_name, )
        extractor_description = configured_extractors[i]['description']
        user_configured_extractors_desc[str(extractor_name)] = str(extractor_description)

        extractor = [extractor_name,is_ordinal,extractor_expression,extractor_description]

        user_configured_extractors.append(extractor)

    print('User configured extractors are' , user_configured_extractors)
    

def classify_attributes_by_threshold(df,threshold):
    data_dict = {}
    data = pd.Series([]) 
    categorical_attributes = [] 
    measure_attributes = []
    discarded_attributes = []
    timeseries_attributes = {}
    df_length = len(df)
    global depth1_corr
    depth1_corr = True


    for attribute in df.columns:
        data_without_duplicates = df[attribute].unique()
        unique_factor = len(data_without_duplicates)/df_length
        attr_dt = df[attribute].dtypes

      
        """
            Check if the object has the patterns of other data types
            Convert them if present
        """

        if attr_dt==np.object:
            try:
                df[attribute] = df[attribute].str.replace(",","").astype(int)
            except ValueError:
                try:
                    df[attribute] = df[attribute].str.replace(",","").astype(float)
                except ValueError:
                    try:
                        df[attribute] = df[attribute].str.replace("%","").astype(float)
                    except ValueError:
                        try:
                            # converts to one dateformat if multiple date formats are there in one columns
                            df[attribute] = pd.to_datetime(df[attribute])
                        except ValueError:
                            pass
                        

        if attr_dt==np.bool:
            categorical_attributes.append(attribute)
            data_dict[attribute] = data_without_duplicates

        elif attr_dt==np.float64:
            # A float can never be a categorical data 
             measure_attributes.append(attribute)

        elif attr_dt==np.int64:
            if unique_factor<threshold:
                categorical_attributes.append(attribute)
                data_dict[attribute] = data_without_duplicates
            else:
                measure_attributes.append(attribute)
        
        elif attr_dt=='datetime64[ns]':
            if unique_factor<threshold:
                categorical_attributes.append(attribute)
                data_dict[attribute] = data_without_duplicates
                timeseries_attributes[attribute] = attribute
            else:
                discarded_attributes.append(attribute)

        elif attr_dt==np.object:
            if unique_factor<threshold:
                categorical_attributes.append(attribute)
                data_dict[attribute] = data_without_duplicates
                try:    
                    if len(data_without_duplicates) > 0 and is_date(str(data_without_duplicates[0])):
                        timeseries_attributes[attribute] = attribute
                except:
                    pass
            else:
                discarded_attributes.append(attribute)
        else:
            discarded_attributes.append(attribute)               
        
    # if len(measure_attributes)==0:
    #     print('No measure attributes present. Cannot mine insights')
    #     raise MeasureMissing()

    if len(categorical_attributes)==0:
        print('No categorical attributes present')


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
        unique_data, categorical_attributes, measure_attributes, timeseries_attributes,discarded_attributes = classify_attributes_by_threshold(data_frame,threshold)

    else:
        unique_data, categorical_attributes, measure_attributes ,timeseries_attributes,discarded_attributes = classify_attributes_by_user_config(data_frame,config_file)

    print('Successfully generate_insights classified the categorical and measure attributes')
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
    top_k = 20
    maxOneInsPerSub = False
    newImpactCalc = False
    limit_search_space = True
    wrapImpactCalc = False
    newOneInsPerSub = True
    unique_share = 0
    result = ins.Insights(data_frame, tau, top_k, categorical_attributes, measure_attributes, timeseries_attributes, maxOneInsPerSub, newImpactCalc, limit_search_space,unique_share,wrapImpactCalc,newOneInsPerSub,user_configured_extractors,filename)
    
    result = sorted(result, key=lambda elem: elem[0], reverse=True)
    # print("\n")
    # for r in result:
    #     print(r, "\n")
    corr_result = out_queue.get()
    if correlation_p.is_alive():
        correlation_p.join()
    print('Generating insight graph')

    print("--- %s Minutes ---" % str(round(float((int(time.time()) - int(start_time)) /60), 2)))

    generated_filename = generate_graphs(result,categorical_index,top_k,corr_result,filename,datasource_id)
    print("--- %s Minutes ---" % str(round(float((int(time.time()) - int(start_time)) /60), 2)))
    return generated_filename


    

if __name__ == "__main__" :
    if len(sys.argv)==3:
        generate_insights(sys.argv[1],sys.argv[2])
    elif len(sys.argv)==2:
        generate_insights(sys.argv[1])
    else:
        print('Usage is\n   python3 generate_insights.py <filename.csv>')