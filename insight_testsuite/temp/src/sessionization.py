#!/usr/bin/env python3
# Created on Wed May  30 10:10:20 2018
# author: Yasmeen

import sys
from datetime import datetime, timedelta
import csv




TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
session = dict()
session_step = dict()


def sessionStart(log,inactivity_time):
    # Generate for each ip for the live floe from the input
    ip=log['ip']
    curr_time = datetime.strptime(log['date']+' '+log['time'],TIME_FORMAT)
    session_end = curr_time+timedelta(seconds=int(inactivity_time)+1)
    if session_end not in session_step.keys():
        session_step[session_end]=[ip]   
    else:
        if ip not in session_step[session_end]:
            session_step[session_end].append(ip)
    return ip,curr_time,session_end,session_step    
    


def sessionDelete(ip,curr_time,session_end,session_step,inactivity_time):
    # Deleting session for the ip's whose session has been inactive for inactivity time period"""
    if curr_time in session_step.keys():
            temp = set(session_step[curr_time])
            for index in temp:
                if session[index]['end'] == curr_time-timedelta(seconds=int(inactivity_time)+1):
                        logOutput(session,index)                   
                        session_step[curr_time].remove(index)
                        del session[index]
    if ip not in session:
        session[ip] = {'start':curr_time,'end':curr_time,'duration':1,'requests':1}
    else:
        session[ip]['requests'] += 1
        session[ip]['end'] = curr_time
        session[ip]['duration'] = int((session[ip]['end'] - session[ip]['start']).total_seconds())+1
    return session_step,session
    



def sessionModify(session_step,session):
    #Modify at end all the expiring sessions"""
    update = []
    for key,values in session_step.items():
        for value in values:
            if value not in update:
                logOutput(session,value)
                update.append(value)




def logOutput(session,index):
    #Genenarate live output to sessionation.txt
    with open(sys.argv[3],'a') as final:
                    writer = csv.writer(final)
                    output = [index,session[index]['start'].strftime('%Y-%m-%d %H:%M:%S'),session[index]['end'].strftime('%Y-%m-%d %H:%M:%S'),session[index]['duration'],session[index]['requests']]
                    writer.writerow(output)





try:
    open(sys.argv[3],'w').close()  #for reprocessing same file deleting existing file
    with open(sys.argv[2]) as a:
        inactivity = a.readlines()
        inactivity_time = inactivity[0]
    
    with open(sys.argv[1]) as b:
        log = csv.DictReader(b)
        for l in log:
            ip,curr_time,session_end,session_step = sessionStart(l,inactivity_time)
            session_step,session = sessionDelete(ip,curr_time,session_end,session_step,inactivity_time)
    sessionModify(session_step,session)
    
except IOError as e:
    print('Processing is failed: %s') % e.strerror

