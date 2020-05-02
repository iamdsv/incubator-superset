# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import copy
import heapq
from scipy.stats import powerlaw
import matplotlib.pyplot as plt
import pandas as pd
import superset.Insights.reqGenerator as req
import sys
import numpy as np
np.seterr(divide='ignore', invalid='ignore')
# df = pd.read_excel('CarSales_Tables.xlsx')
# df['Year'] = pd.to_datetime(df['Year'], format='%Y-%m-%d %H:%M:%S')
# measures = ['Sales']
# dataCube, catCols, allCols = req.generateDataCube(df, measures)
# dictionary = req.generateDictionary(df, catCols)
# dim = len(allCols)
# dimCatCols = len(catCols)
measures = []
catCols = []
timeseriesCols = {}
# dataCube = {}
# dataCubeCount = {}
dictionary = {}
TotalSub = {}
InsightTypesAll = ['O1', 'On', 'Trend']
PI = ['O1', 'On']
SI = ['Trend']

def progressBar(value, endvalue, bar_length=20):

    percent = float(value) / endvalue
    arrow = '-' * int(round(percent * bar_length)-1) + '>'
    spaces = ' ' * (bar_length - len(arrow))

    sys.stdout.write("\rGenerating Insights: [{0}] {1}%".format(arrow + spaces, int(round(percent * 100))))
    sys.stdout.flush()


# %%
class Subspace:
    dim = 0
    S = {}
    def __init__(self, dim, colNames): 
        self.dim = dim
        for name in colNames:
            self.S[name] = '*'


# %%
class SiblingGroup:
    Sub = {}
    Di = ""
    def __init__(self, Sub, Di): 
        self.Sub = Sub
        self.Di = Di


# %%
def isValid(SG, Ce) :
    S = SG.Sub
    Da = SG.Di
    if len(Ce) == 1 :
        return True
    for ext in Ce[1:]:
        if ext[1] != Da and S[ext[1]] == '*' :
            return False
    return True


# %%
def getImpactOrderList(S, dictionary, Di, measure, dataCube, lookup) :
    STemp = copy.deepcopy(S)
    ImpactList = []
    for v in dictionary[Di] :
        STemp[Di] = v
        SibG = SiblingGroup(STemp, Di)
        ImpactList.append(Imp(SibG, measure, dataCube, lookup))
    return [x for y, x in sorted(zip(ImpactList, dictionary[Di]), reverse = True)]


# %%
def getOrderedCatCols(catCols, dictionary) :
    lenOfCatCols = []
    for col in catCols:
        lenOfCatCols.append(len(dictionary[col]))
    return [x for y, x in sorted(zip(lenOfCatCols, catCols))]


# %%
counter = 0
ubk = 0
heap = []
kTop = 0
insightInHeap = {}
maxOneInsPerSub = True
def Insights(df, tau, k, categorical_attributes, measure_attributes, timeseries_attributes, maxOneInsPerSubAttr) :
    global ubk, heap, kTop, dictionary, catCols, measures, maxOneInsPerSub, timeseriesCols
    heap = []
    ubk = -1
    kTop = k
    maxOneInsPerSub = maxOneInsPerSubAttr
    CeCounter = 0
    measures = measure_attributes
    catCols = categorical_attributes
    timeseriesCols = timeseries_attributes
    print("Preprocessing Data")
    print("Generating Datacube")
    dataCube, dataCubeCount, allCols = req.generateDataCube(df, catCols, measures, timeseriesCols)
    print("Generated Datacube")
    print("Generating Dictionary")
    dictionary = req.generateDictionary(df, catCols)
    print("Generated Dictionary")
    print("Ordering Categorical Columns")
    orderedCatCols = getOrderedCatCols(catCols, dictionary)
    print("Ordered Categorical Columns")
    print("Preprocessing Done")
    dim = len(allCols)
    dimCatCols = len(catCols)
    CeAll = req.getCeAll(measures, catCols, tau)
    # print(CeAll)
    for cols in allCols:
        TotalSub[str(cols)] = "*"
    progressBar(CeCounter, len(CeAll))
    for Ce in CeAll :
        for i in range(dimCatCols) :
            Subs = Subspace(dim, allCols)
            useDataCube = {}
            lookup = []
            if Ce[0][1] in measures:
                useDataCube = dataCube
                lookup = measures
            elif Ce[0][1] in catCols: 
                useDataCube = dataCubeCount
                lookup = catCols
            EnumerateInsight(Subs.S, orderedCatCols[i], Ce, Ce[0][1], useDataCube, 0, lookup)
        CeCounter += 1
        progressBar(CeCounter, len(CeAll))
    return heap


# %%
import math

def EnumerateInsight(S, Di, Ce, measure, dataCube, depth, lookup) :
    if depth > 2:
        return
    # global counter
    global ubk, heap, insightInHeap
    STemp = copy.deepcopy(S)
    SibG = SiblingGroup(STemp, Di)
    if isValid(SibG, Ce):
        if ubk <= Imp(SibG, measure, dataCube, lookup) or (len(heap) < kTop) :
            Phi,Phi_Index,dim_xvalues,dim_yvalues = Extract(SibG, Ce, measure, dataCube, lookup)
            for inType in InsightTypesAll:
                Score = Imp(SibG, measure, dataCube, lookup)*Sig(Phi, inType, SibG.Di)
                if math.isnan(Score):
                    Score = 0.0
                Score = round(Score, 2)
                if ((Score > ubk) or (len(heap) < kTop)) and len(Phi) > 0 and Score > 0.0:
                    outputList = []
                    if(len(heap) == kTop):
                        if maxOneInsPerSub:
                            removeKey = str(heap[0][1]) + str(heap[0][2])
                            insightInHeap.pop(removeKey, None)
                        heapq.heappop(heap)
                    if inType in PI:
                        if inType == "O1":
                            outputList = [Score, str(SibG.Sub), SibG.Di, max(Phi, key=Phi.get), str(Phi[max(Phi, key=Phi.get)]), str(Phi), Ce, inType,dim_xvalues,dim_yvalues,Phi_Index]
                        else:
                            outputList = [Score, str(SibG.Sub), SibG.Di, min(Phi, key=Phi.get), str(Phi[min(Phi, key=Phi.get)]), str(Phi), Ce, inType,dim_xvalues,dim_yvalues,Phi_Index]
                    else :
                        outputList = [Score, str(SibG.Sub), SibG.Di, min(Phi, key=Phi.get), str(Phi[min(Phi, key=Phi.get)]), str(Phi), Ce, inType,dim_xvalues,dim_yvalues,Phi_Index]
                    if maxOneInsPerSub:    
                        key = str(SibG.Sub) + str(SibG.Di)
                        if key in insightInHeap.keys():
                            ind = [index for index, value in enumerate(heap) if float(value[0]) == float(insightInHeap[key]) and value[1] == str(SibG.Sub) and value[2] == SibG.Di]
                            if Score > insightInHeap[key]:
                                heap[ind[0]] = outputList
                                insightInHeap[key] = Score
                        else:
                            heapq.heappush(heap, outputList)
                            insightInHeap[key] = Score
                    else:
                        heapq.heappush(heap, outputList)
                    heapq.heapify(heap)
                    ubk = float(heap[0][0])
        else :
            return
    highImpactList = getImpactOrderList(S, dictionary, Di, measure, dataCube, lookup)
    for v in highImpactList:
        SDash = copy.deepcopy(S)
        SDash[Di] = v
        for entry in SDash:
            if SDash[entry] == '*' and entry not in measures :
                EnumerateInsight(SDash, entry, Ce, measure, dataCube, depth + 1, lookup)


# %%
def Extract(SG, Ce, measure, dataCube, lookup):
    Phi = {}
    Phi_Index = {}
    dim_xvalues = []
    dim_yvalues = []
    counter = 0
    for v in dictionary[SG.Di]:
        SDash = copy.deepcopy(SG.Sub)
        SDash[SG.Di] = v
        if tuple(SDash.items()) in dataCube:
            MDash = RecurExtract(SDash, len(Ce), Ce, measure, dataCube, lookup)
            if len(Ce) > 1:
                if tuple(SDash.items()) in MDash:
                    Phi[tuple(SDash.items())] = MDash[tuple(SDash.items())]
                    dim_xvalues.append(v)
                    dim_yvalues.append(MDash[tuple(SDash.items())])
            else:
                Phi[tuple(SDash.items())] = MDash
                dim_xvalues.append(v)
                dim_yvalues.append(MDash)
            
            Phi_Index[v] = counter
            counter = counter + 1
    return Phi,Phi_Index,dim_xvalues,dim_yvalues


# %%
import scipy.stats as ss
def RecurExtract(S, level, Ce, measure, dataCube, lookup):
    if level > 1:
        Phi = {}
        Dim = Ce[level - 1][1]
        subDim = S[Dim]
        for v in dictionary[Dim]:
            SDash = copy.deepcopy(S)
            SDash[Dim] = v
            if tuple(SDash.items()) in dataCube:
                MvDash = RecurExtract(SDash, level - 1, Ce, measure, dataCube, lookup)
                if level == 2:
                    Phi[tuple(SDash.items())] = MvDash
                else:
                    if tuple(SDash.items()) in MvDash:
                        Phi[tuple(SDash.items())] = MvDash[tuple(SDash.items())]
        MDash = {}
        CeLocal = Ce[level - 1][0]
        if CeLocal == "Rank":
            keyList = []
            valueList = []
            rankList = []
            for entry in Phi:
                if(Phi[entry] != -9999):
                    TempS = dict((x, y) for x, y in entry)
                    keyList.append(tuple(TempS.items()))
                    valueList.append(float(Phi[entry]))
            rankList = ss.rankdata([-1 * value for value in valueList]).astype(int)
            for key, rank in zip(keyList, rankList):
                MDash[key] = int(rank)
        elif CeLocal == "%":
            keyList = []
            valueList = []
            percentList = []
            indexDim = -1
            i = 0
            for entry in Phi:
                TempS = dict((x, y) for x, y in entry)
                keyList.append(tuple(TempS.items()))
                if TempS[Dim] == subDim:
                    indexDim = i
                valueList.append(float(Phi[entry]))
                i += 1
            TotalSum = sum(valueList)
            if TotalSum > 0:
                for value in valueList:
                    percentList.append(int(round(((value * 100)/TotalSum), 0)))
                #Todo: Check if sum of percentList is 100
                delta = 100 - sum(percentList)
                if indexDim != - 1:
                    if delta > 0:
                        percentList[indexDim] += delta
                    elif delta < 0:
                        percentList[indexDim] += delta
                for key, percent in zip(keyList, percentList):
                    MDash[key] = percent
        elif CeLocal == "DAvg":
            keyList = []
            valueList = []
            avgList = []
            for entry in Phi:
                TempS = dict((x, y) for x, y in entry)
                keyList.append(tuple(TempS.items()))
                valueList.append(float(Phi[entry]))
            TotalSum = sum(valueList)
            if len(keyList) > 0:
                Avg = round(TotalSum/len(keyList), 1)
                for value in valueList:
                    avgList.append(round(value - Avg, 1))
                for key, avg in zip(keyList, avgList):
                    MDash[key] = avg
        elif CeLocal == "DPrev":
            prevEntry = {}
            for entry in Phi:
                TempS = dict((x, y) for x, y in entry)
                if(prevEntry == {}) :
                    prevEntry = entry
                    MDash[tuple(TempS.items())] = -9999
                    continue
                MDash[tuple(TempS.items())] = float(Phi[entry]) - float(Phi[prevEntry])
                prevEntry = entry
    else:
        if tuple(S.items()) in dataCube:
            MDash = dataCube[tuple(S.items())][lookup.index(measure)]
        else:
            MDash = 0
    return MDash


# %%
def Imp(SG, measure, dataCube, lookup):
    SubDash = SG.Sub
    if tuple(SubDash.items()) in dataCube:
        if dataCube[tuple(TotalSub.items())][lookup.index(measure)] > 0 :
            answer = dataCube[tuple(SubDash.items())][lookup.index(measure)] / dataCube[tuple(TotalSub.items())][lookup.index(measure)]
        else:
            answer = 0
    else :
        answer = 0
    return round(answer, 2)


# %%
def Sig(Phi, inType, Di) :
    if inType in PI :
        return getPointScore(Phi, inType)
    elif inType in SI:
        if Di in timeseriesCols:
            return getShapeScore(Phi, inType)
        else: return 0
    else: return 0


# %%
from scipy.stats import norm, zscore
from scipy import optimize
import numpy as np
import math
import scipy

xdata = []
ydata = []

beta = 1.02

def pl1(x, *p):
    global xdata, ydata
    return p[0]*(x**(-p[1]))

def pl(y):
    global xdata, ydata
    return [y[0]*(x**(-beta)) for x in xdata]

def pl_val(x, y):
    global xdata, ydata
    return y[0]*(x**(-beta))

def func_err(y):
    global xdata, ydata
    exp_out = ydata
    pred_out = pl(y)
    if(len(xdata) > 0) :
        return sum([(exp_out[i]-pred_out[i])**2 for i in range(len(xdata))])/len(xdata)
    else :
        return 0


def getPointScore(Phi, inType) :
    global xdata, ydata
    localPhi = copy.deepcopy(Phi)
    if(len(Phi) == 0) :
        return 0
    X = [i for i in list(localPhi.values()) if i!=-9999]
    if(len(X) == 0) :
        return 0
    if inType == "O1":
        X.sort(reverse=True)
    else:
        X.sort()
    XDash = X
    if(len(XDash) == 0) :
        return 0
    # if XDash[-1]<=0 or XDash[0]<=0:
    #     XDash = [i + abs(min(XDash))+1 for i in XDash]
    if inType == "O1":
        XDash = [i for i in XDash if i > 0]
    else :
        XDash = [-1*i for i in XDash if i < 0]
    if(len(XDash) == 0) :
        return 0
    Xmax =XDash[0]
    XDash = XDash[1:]
    if(len(XDash) == 0) :
        return 0
    xdata = list(range(2, len(XDash) + 2))
    ydata = np.asarray(XDash)

    result = optimize.minimize(func_err, x0=[0])
    amp = result.x
    XDerived = pl([amp])

    Res = [XDash[i] - XDerived[i]  for i in range(len(xdata))]
    XmaxCap = pl_val(1, [amp])
    Emax = Xmax - XmaxCap

    data = Res
    mu, std = norm.fit(data)
    return (norm.cdf(Emax, mu, std)[0])


# %%
from scipy import stats
import numpy as np
xdataShape = []
ydataShape = []

def getShapeScore(Phi, inType) :
    global xdataShape, ydataShape
    localPhi = copy.deepcopy(Phi)
    X = list(localPhi.values())
    if(len(Phi) == 0) :
        return 0
    X = [i for i in list(localPhi.values()) if i!=-9999]
    if(len(X) == 0 or len(X) == 1) :
        return 0
    XDash = X
    xdataShape = list(range(1, len(XDash) + 1))
    ydataShape = np.asarray(XDash)
    slope, intercept, r_value, p_value, std_err = stats.linregress(xdataShape, ydataShape)
    p_value = stats.logistic.cdf(slope, 0.2, 2)
    return (r_value**2)*p_value


# %%
# Insights(df, tau, top_k, categorical_attributes, measure_attributes)


# %%


