from ast import literal_eval
from datetime import datetime
import os
from pathlib import Path

import plotly.graph_objects as go
import json
import copy
from urllib.parse import urlencode, quote
from superset import app
config = app.config
bar_json_file = open(config["SAMPLE_JSON"] + 'sampleBarJSON.json')
adhocFilter_json_file = open(config["SAMPLE_JSON"] + 'adhocFilter.json')
sampleBarJSON = json.load(bar_json_file)
adhocFilter = json.load(adhocFilter_json_file)
addSliceLinkList = []

insight_description = {
'On': 'Outstanding#N',
'O1': 'Outstanding#1',
'Trend': 'Trend'
}


extractor_description = {
    'Sum' : 'Sum',
    'Count' : 'Count',
    'Rank' : 'Rank',
    '%' : 'Percentage',
    'DAvg': u"\u0394" + 'Avg',
    'DPrev': u"\u0394" + 'Prev'
}

extractor_text = {
    'Sum' : 'Sum of',
    'Count' : 'Count of',
    'Rank' : 'Rank',
    '%' : 'percentage',
    'DAvg': 'difference from average',
    'DPrev': 'difference from previous year'
}

def get_extractor_description(composite_extractor, start_range):
   
    desc = ''
    for extractor in composite_extractor[start_range:]:
        desc = desc + ' ' + extractor_description[extractor[0]] + ' by ' + extractor[1]

    return desc

"""

Format of result is 
0 - Score of the insight
1 - Subspace
2 - Breakdown Dimension
3 - The winning subspace
4 - The value of phi 
5 - All the sibling group values
6 - Composite extractor
7 - Insight Type
8 - X-Value(s) from breakdown dimension 
9 - Y-Value(s) from breakdown dimension 
10 - Highlight Index

#TODO: Add measure also as an return value
11 - Measure
"""
def generate_graphs(results,categorical_index,k,corr_fig,filename):
    figures = []
    counter = 1

    for result in results:
        adhocFilterList = []
        subspace_dict = literal_eval(result[1])
        subspace_str = ""
        link = "http://www.google.com"
        for key, value in subspace_dict.items():
            if value!='*':
                tempAdhocFilter = copy.deepcopy(adhocFilter)
                subspace_str = subspace_str + " " + key + " " + value
                tempAdhocFilter["subject"] = str(key)
                tempAdhocFilter["comparator"] = str(value)
                adhocFilterList.append(tempAdhocFilter)
        if len(subspace_str)==0:
            subspace_str = ' Entire space'

        insight_desc = insight_description[result[7]]
        explanation = ""
        # explantion = get_extractor_description(result[6], 1) + " breakdown by " + str(result[2]) + str(result[8][result[10]])
        ce_desc = get_extractor_description(result[6], 0)
        breakdown_dimension = str(result[2])
        measure = str(result[6][0][1])
        if result[7] =='On' or result[7] =='O1':
            category_index = categorical_index[result[2]]
            indices_dict = literal_eval(str(result[10]))
            # get domain value which is the highest valued subspace
            value = result[3][category_index][1]
            outstanding_index = indices_dict[value]
            winning_category = str(result[8][int(outstanding_index)])
            if len(result[6]) > 1:
                comparision_var = "has highest" if result[7] =='O1' or result[6][1][0] == 'DPrev' or result[6][1][0] == 'DAvg' else "has lowest"
                extractor_word = extractor_text[result[6][1][0]]
            else:
                comparision_var = "has highest" if result[7] =='O1' else "has lowest" 
                extractor_word = extractor_text[result[6][0][0]]
            explanation += winning_category + " " + breakdown_dimension + " " + comparision_var + " " + extractor_word + " " + measure + " for" + subspace_str
            graph_title = extractor_word + " " + measure + " for " + breakdown_dimension + " for" + subspace_str
            graph_title = graph_title.replace(" ", "+")
            # if last composite extractor is % then display as pie chart only for On and O1
            if result[6][len(result[6])-1][0]=='%':
                pull_arr = [0] * len(result[8])
                pull_arr[outstanding_index] = 0.1
                fig = go.Figure(data=[go.Pie(labels=result[8], values=result[9],pull=pull_arr)])
                xaxis_title = ""

            else:
                tempBarJSON = copy.deepcopy(sampleBarJSON)
                if len(result[6]) == 1:
                    tempBarJSON["groupby"].append(str(result[2]))
                    tempBarJSON["metrics"][0]["sqlExpression"] = str(result[6][0][0]) + "(\"" + str(result[6][0][1]) + "\")"
                    tempBarJSON["metrics"][0]["label"] = str(result[6][0][0]) + "(\"" + str(result[6][0][1]) + "\")"
                    tempBarJSON["adhoc_filters"] = adhocFilterList
                    tempBarJSONStr = json.dumps(tempBarJSON)
                    link = "/testSliceAdder?formData=" + str(quote(tempBarJSONStr)) + "&graphTitle=" + graph_title
                colors = ['lightslategray'] * len(result[8])
                colors[outstanding_index] = 'crimson'
                fig = go.Figure([go.Bar(x=result[8], y=result[9],marker_color=colors)])
                xaxis_title=result[2]


        elif result[7]=='Trend':
            fig = go.Figure(data=go.Scatter(x=result[8][1:], y=result[9][1:]))
            xaxis_title=result[2]
            explanation += measure + " of" + subspace_str + " follows a trend across " + breakdown_dimension
            
        fig.update_layout(
            xaxis_title=result[2],
            # TODO: Change to the measure
            yaxis_title=str(result[6][0][1]),
            autosize=False,
            height=400, width=725,
            font=dict(
                family="Arial",
                size=10,
                color="Black"
            ),
            title_text= 'Insight type: ' + insight_desc + ' Score: ' + str(result[0]) + "       <a href=\"" + link + "\">Add slice</a>" + '<br>Subspace: ' + subspace_str + ' | Extractor: ' + ce_desc + '<br>Explanation: ' + explanation,
            # title_text= 'Insight type: ' + insight_desc + '<br>Subspace: ' + subspace_str + ' | Extractor: ' + ce_desc + '<br>Explanation: ' + explanation,
        )

        figures.append(fig)
    
    if corr_fig!=None and len(corr_fig)>0:
        figures.extend(corr_fig)

    if len(figures)==0:
        return

    inp_filename = os.path.splitext(os.path.basename(filename))[0]
    file_path = 'reports/' + inp_filename
    check_path = config["REPORT_SAVE"] + file_path
    full_filename = file_path + '/' + inp_filename + '.html'
    full_filename = full_filename.replace(":", "-")
    added_styles=False
    return_filename = full_filename
    full_filename = config['REPORT_SAVE'] + full_filename
    Path(check_path).mkdir(parents=True, exist_ok=True)
    with open( full_filename, 'w+') as f:
        if not added_styles:
            f.write("<meta name='viewport' content='width=device-width, initial-scale=1.0'>")
            f.write("<link  rel='stylesheet' href='/static/css/styles.css'/>")
        for fig in figures:
            if counter % 2 == 1:
                f.write("<div class='container'>")
                f.write("<div class='first'>")
            else:
                f.write("<div class='second'>")
            f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))
            f.write("</div>")
            if counter % 2 == 0:
                f.write("</div>")
            counter = counter + 1
    print('Graphs generation completed')
    return return_filename
