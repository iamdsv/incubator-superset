import sys

from insight_exceptions import ExitProcess
from insights_common import InsightType

file_handler  = None

def correlation_insight(msg):
    global file_handler
    print('printing correlation insight')
    # for result in msg.result:
    #     print(result)
    file_handler.write("Correlation \n" + msg.result[0].to_string() + " \n")

def process_exit(msg):
    raise ExitProcess

switcher = {
    InsightType.Correlation : correlation_insight,
    InsightType.ProcessExit: process_exit
}

def gather_opt(queue):
    global file_handler
    file_handler= open("insights.txt","w")
    while True:
        msg = queue.get()
        func = switcher[msg.insight_type]
        try:
            func(msg)
        except ExitProcess:
            print('exiting the process')
            file_handler.close()
            sys.exit()


        
