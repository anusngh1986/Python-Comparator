#Author-Anurag
import string
import csv, itertools
import sys
import csv
import datetime
import time
import os
from operator import itemgetter
from operator import *
from collections import *
###################### DESIGNING OF REPORT###############################
 
#designing of the final Report
print '-------------------------------------------------------------------------------'
print '                               ANALYSIS REPORT                                 '
print '-------------------------------------------------------------------------------'
#print '\n'
from time import ctime
go = str(ctime())
print '  Run time      :' ,go.center(40)
import socket
kul = str(socket.gethostname())
print '  WELCOME-USER  :' , kul.center(40)
print ('---------------------------------------------------------------------------------------')
print 'Validated Source File : ' ,sys.argv[1]
print 'Validated Target File : ' ,sys.argv[2]
print 'Output Directory : ' ,sys.argv[3]
#print '\n'
print '-------------------------------------------------------------------------------'
source_delim=sys.argv[4]
target_delim=sys.argv[5]

print(str(source_delim))
print(str(target_delim))

source_datas = open(sys.argv[1],'rb')
file1 = open('bravo_temp_src.csv','wb')
for row in source_datas:
file1.write(row.replace(str(source_delim),'|'))
file1.close()
file2 = csv.reader(open('bravo_temp_src.csv','rb'),delimiter='|')
#for t in file2:
# print t
#column_names=next(source_datas)
sort_src = sorted(file2, key=itemgetter(1))
#for rr in sort_src:
# print(rr.replace('\x01','|')




print(sort_src)
sc=len(list(sort_src))
print ("                         TOTAL RECORDS COUNT OPERATION STARTED                           ")
print ("TOTAL SOURCE RECORD COUNT is:  %s " %sc)



target_dataa = open(sys.argv[2],'rb')
file3 = open('bravo_temp_tgt.csv','wb')
for row in target_dataa:
file3.write(row.replace(str(target_delim),'|'))
file3.close()
file4 = csv.reader(open('bravo_temp_tgt.csv','rb'),delimiter='|')
#for t in file4:
# print ("File4:",t)
#column_names_target=next(target_dataa)
sort_tgt = sorted(file4, key=itemgetter(0))
tc=len(list(sort_tgt))
print ("TOTAL TARGET RECORD COUNT is: %s" %tc)





print '-------------------------------------------------------------------------------'
print ("                         DUPLICATE RECORDS OPERATION STARTED                       ")
######################  DUPLICATES OPERATION STARTS############
dup_s_output = []
seen = set()
for value in sort_src:
     if value[1] not in seen:
         dup_s_output.append(value)
         #print value
         seen.add(value[1])
#print dup_s_output
dup_s_count=sc - (len(list(dup_s_output)))
print ("DUPLICATES COUNT IN SOURCE IS : %s " %dup_s_count)
 
dup_t_output = []
seen_t = set()
for valuet in sort_tgt:
     if valuet[0] not in seen_t:
         dup_t_output.append(valuet)
         seen_t.add(valuet[0])
#print dup_t_output
dup_t_count=tc - (len(list(dup_t_output)))
print ("DUPLICATES COUNT IN TARGET IS : %s" %dup_t_count)
 
######################  COMMON OPERATION STARTS############
print '-------------------------------------------------------------------------------'
print ("                         COMMON RECORDS OPERATION STARTED                          ")
masterlist = [rowm[0] for rowm in dup_t_output]
scr=[]
for hosts_row in dup_s_output:
    if hosts_row[1]in masterlist:
        scr.append(hosts_row)
masterlistt = [rowmt[1] for rowmt in dup_s_output]
tcr=[]
for hosts_rowt in dup_t_output:
  if hosts_rowt[1]in masterlistt:
     tcr.append(hosts_rowt)
print ("COMMON RECORDS IN SOURCE & TARGET IS : %s" %len(scr))
#print ("COMMON RECORDS IN TARGET is : %s" %len(tcr))
##############################################################missing started####
print '-------------------------------------------------------------------------------'
print ("                         MISSING RECORDS OPERATION STARTED                         ")
source_miss=[] 
for valuemi in sort_src:
    source_miss.append(valuemi[1])
#print source_miss
#dupl_s=[si for si in Counter(source_miss) if Counter(source_miss)[si] > 1]
#print dupl_s
tgt_miss=[]
for valuemit in sort_tgt:
    tgt_miss.append(valuemit[1])
#print tgt_miss
#dupl_t=[ti for ti in Counter(tgt_miss) if Counter(tgt_miss)[ti] > 1]
#print dupl_t
#print set(source_miss)
Mism_in_target=set(source_miss)-set(tgt_miss)
print ("MISSING COUNT IN TARGET IS : %s" %len(Mism_in_target))
Mism_in_source=set(tgt_miss)-set(source_miss)
print ("MISSING COUNT IN SOURCE IS  : %s" %len(Mism_in_source))
###########MISSING OPERATION closed########################
#print scr
#print tcr
#######Mismatch Started#############
print '-------------------------------------------------------------------------------'
print ("                         MISMATCH RECORDS OPERATION STARTED                        ")
counter = 1
mismatch=[]
mismatch_w = open(sys.argv[3]+"/mismatch.txt","w")
def rowElementCompare(sourceRow, targetRow):
    row_length = min(len(sourceRow), len(targetRow))
    counts=0
    for i in range(row_length):
        if sourceRow[i] != targetRow[i]:
            counts=counts+1
            yield i # UPDATED
    #print counts
    return# UPDATED)
for source_row,target_row in itertools.izip_longest(scr,tcr):
    comparison_result = None
    for comparison_result in rowElementCompare(source_row, target_row): # UPDATED
         mismatch.append(counter)
         gok=("Mismatch in column %s on row number %d , source value %s, target value %s" % (comparison_result, counter, source_row[comparison_result], target_row[comparison_result]))
         #print (gok)
         mismatch_w.write(gok+'\n')
    counter += 1
mismatch_w.close()
print ("MISMATCH COUNT IN SOURCE & TARGET is : %s" %len(mismatch))
print '-------------------------------------------------------------------------------'
print '  Current working directory : ' , os.getcwd()
print '-------------------------------------------------------------------------------'
from time import ctime
goend = str(ctime())
print '  Completed at      :' ,goend.center(40) 
print '-------------------------------------------------------------------------------' 
print '                                 END OF REPORT                                 '
print '-------------------------------------------------------------------------------'