# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import itertools

def findsubsets(s, n): 
    return [list(i) for i in itertools.combinations(s, n)]

def getAllSubsets(s, limit):
    result = []
    n = limit
    for i in range(1, n + 1):
        tmp = findsubsets(s, i)
        for el in tmp:
            result.append(el)
    return result

def getCategoricalCols(cols, measures):
    return list(set(cols) - set(measures))


# %%
import numpy as np
import datetime
import pandas as pd
def generateDictionary(df, catVar):
    dictionary = {}
    for cat in catVar:
        # if pd.api.types.is_datetime64_ns_dtype(df[cat]):
        #     uniqueList = df[cat].dt.date.unique().tolist()
        #     uniqueList = [str(el) + " 00:00:00" for el in uniqueList]
        # else :
        uniqueList = df[cat].unique().tolist()
        print(uniqueList)
        strList = [str(el) for el in uniqueList]
        dictionary[cat] = strList
    return dictionary


# %%
import pandas as pd
OrdinalCols = {}

def generateDataCube(df, categoricalCols, measures, timeseriesCols):
    global OrdinalCols
    dataCube = {}
    dataCubeCount = {}
    allCols = df.columns
    catCols = categoricalCols 
    OrdinalCols = timeseriesCols
    allSubs = getAllSubsets(catCols, 2)
    allCols = categoricalCols + measures
    calls = [df]
    for el in allSubs:
        calls.append(df.groupby(el))
    for c in calls:
        groupNames = c.median().index.names
        lenGroupNames = len(groupNames)
        if groupNames[0] != None:
            diction = {}
            for i in range(lenGroupNames):
                diction[groupNames[i] + 'List'] = c.median().index.get_level_values(groupNames[i]).tolist()
            allMeasuresValues = []
            allCategoricalValues = []
            for measure in measures:
                allMeasuresValues.append(c.sum()[measure].tolist())
            countFrame = c.nunique()
            for cat in catCols:
                allCategoricalValues.append(countFrame[cat].tolist() if cat in countFrame.columns else [0] * len(countFrame))
            for ind in range(len(allMeasuresValues[0])):
                inner_dict = {}
                for cols in allCols:
                    if cols in groupNames:
                        inner_dict[cols] = str(diction[cols + 'List'][ind])
                    else:
                        inner_dict[cols] = "*"
                dataCube[tuple(inner_dict.items())] = [row[ind] for row in allMeasuresValues]
            for ind in range(len(allCategoricalValues[0])):
                inner_dict = {}
                for cols in allCols:
                    if cols in groupNames:
                        inner_dict[cols] = str(diction[cols + 'List'][ind])
                    else:
                        inner_dict[cols] = "*"
                dataCubeCount[tuple(inner_dict.items())] = [row[ind] for row in allCategoricalValues]
        else:
            inner_dict = {}
            allMeasuresValues = []
            allCategoricalValues = []
            for measure in measures:
                allMeasuresValues.append(c.sum()[measure])
            countFrame = c.nunique()
            for cat in catCols:
                allCategoricalValues.append(countFrame[cat])
            for cols in allCols:
                inner_dict[cols] = "*"
            dataCube[tuple(inner_dict.items())] = allMeasuresValues
            dataCubeCount[tuple(inner_dict.items())] = allCategoricalValues

            print(dataCube[tuple(inner_dict.items())])
        
    return dataCube, dataCubeCount, allCols


# %%
# import pandas as pd
# df = pd.read_csv("datasets/CarSales.csv")
# measures = ['Sales']
# dataCube, allCols = generateDataCube(df, measures)
# # print(dataCube)
# getCeAll(['Sales'], ['Brand', 'Category'], 3)


# %%
CeAll = []
getIndexCe = {'%': 0, 'DAvg': 1, 'DPrev': 2}
Taxonomy = [[False, False, True, True],
        [False, False, False, True],
        [False, False, True, False]]
def getCeAll(measures, catCols, maxLevels) :
    CeFirstLevel = []
    extractors = ['Sum', 'Count']
    forMeasure = ['Sum']
    forCat = ['Count']
    for extractor in extractors:
        if extractor in forMeasure:
            for measure in measures:
                CeFirstLevel.append([str(extractor), str(measure)])
        if extractor in forCat:
            for cat in catCols:
                if cat not in OrdinalCols:
                    CeFirstLevel.append([str(extractor), str(cat)])
    CeAll.clear()
    for Ce in CeFirstLevel:
        CeAll.append([Ce])
    generateCeAll(CeFirstLevel, catCols, 2, maxLevels)
    return CeAll


# %%
def generateCeAll(levelCe, catCols, level, maxLevels):
    if(level == maxLevels + 1) :
        return
    newLevelCe = []
    for levCe in levelCe: 
        for catVars in catCols:
            for Ce in getIndexCe:
                if levCe[0] != "Count":
                    if ((str(Ce) !=  "DPrev") or (str(catVars) in OrdinalCols)):
                        if level == 2:
                            newLevelCe.append([levCe, [str(Ce), str(catVars)]])
                        else :
                            if Taxonomy[getIndexCe[levCe[level - 2][0]]][getIndexCe[Ce]]:
                                tempLev = levCe.copy()
                                tempLev.append([str(Ce), str(catVars)])
                                newLevelCe.append(tempLev)   
    CeAll.extend(newLevelCe)
    generateCeAll(newLevelCe, catCols, level + 1, maxLevels)


# %%


