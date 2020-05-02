class InsightException(Exception):
    def __str___(self):
        return 'Errror occured while generating insights'

class FileNotFoundError(InsightException):
    def __init__(self,filename):
        self.filename = filename
    
    def __str__(self):
        return 'Error while opening file ' + str(self.filename)

class ConfigFileError(InsightException):
    def __init__(self,filename):
        self.filename = filename
    
    def __str__(self):
        return 'Config file is not in JSON format' + str(self.filename)

class CorrelationInsightException(Exception):
    def __init__(self,reason):
        self.reason = reason

    def __str___(self):
        return 'No Correlation based insights could be generated due to ' + reason

class CategoricalAttrMissing(Exception):
    def __str___(self):
        return 'No Categorical attributes. Cannot generate insights'

class ExitProcess(Exception):
    pass