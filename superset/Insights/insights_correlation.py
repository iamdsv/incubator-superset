from itertools import combinations

import plotly.express as px

"""
Traverse all possible combination of measures and find correlation 
"""
def generate_correlation_insights(data_frame,measure_attributes,out_queue):
    print('Generating correlation insights for the datasets')
    # corrMatrix = data_frame[measure_attributes].corr()
    figures = []
    if len(measure_attributes)==0:
        out_queue.put(figures)
        return 

    for comb in combinations(measure_attributes, 2):
        corr_value = data_frame[comb[0]].corr(data_frame[comb[1]])
        if corr_value>=0.7 or corr_value<=-0.7:
            print('Generated correlation for ',comb)
            fig = px.scatter(data_frame, x=comb[0], y=comb[1],trendline="ols")
            corr_value = round(corr_value,2)
            if corr_value>=0.7:
                corr_reference = 'Positively correlated with coefficient value ' + str(corr_value)
            elif corr_value<=-0.7:
                corr_reference = 'Negatively correlated with coefficient value ' + str(corr_value)
            else:
                corr_reference = 'Correlation coefficient is' + str(corr_value)

            fig.update_layout(
                autosize=False,
                title_text= corr_reference
            )
            figures.append(fig)

    out_queue.put(figures)
    print('Correlation graphs are generated')

