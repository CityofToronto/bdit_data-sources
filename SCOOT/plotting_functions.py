# -*- coding: utf-8 -*-
"""
Created on Thu Feb 16 13:51:58 2017

@author: qwang2
"""

import matplotlib.pyplot as plt
import preprocess
import pandas as pd
import numpy as np


def TOD(ax,data,station,direction,date,count,gg,color,normalize,source):  
    ''' data: Dataframe to be plotted; preprocessed to have 
            - 'time_15': index of 15min bin in a day
            - station: indicates segment info (centreline_id, index, etc)
            - direction: indicates the direction (content can be +/- 1, EB/WB/SB/NB, etc)
            - date: count_date
            - count: actual count
        station, direction, date, count: Names of the columns in the given Dataframe
        gg: a list of (station, direction) to be graphed
        color: a list of colors to use for different segments 
        normalize: boolean value indicating whether plot is normalized or not
        
        OUTPUT:
            a plot to the currently active figure
            ncounts: a list of number of counted 15min bins each day
            average: average time of day profile (veh or % daily volume) in the given time period
            ndays: number of effective days (more than 64/96 15min bins counted)'''
    
    gstation = data.groupby([station,direction])
    i = 0
    average = np.zeros(24)
    ndays = 0
    g0 = gstation.get_group(gg)
    ncounts = []
    for (d), g1 in g0.groupby([date]):
        g1 = g1.groupby('time_15',as_index=False).sum()
        ncounts.append((d,len(g1[g1[count]>0])))
        if len(g1[g1[count]>0])>64:
            filled = preprocess.fill_missing_values(g1['time_15'].tolist(),g1[count].tolist(),0,95)
            g2 = pd.DataFrame({'time_15':list(range(96)), count:filled})
            g2['hour'] = g2['time_15']//4
            g2 = g2.groupby(['hour'], as_index=False).sum()
            ndays += 1
            totvol = sum(g2[count].tolist())
            if normalize:
                g2[count] = [x/totvol for x in g2[count]]
            index = range(24)
            average += np.array(g2[count])
            if ndays < 100:
                ax.plot(index,g2[count],linewidth=1,color=color[i],alpha=0.3, label='_nolegend_')

            if ndays > 100:
                average = average/ndays
                ax.plot(average,linewidth=5,color=color,label=source)
                return ncounts, average, ndays
    average = average/ndays
    ax.plot(average,linewidth=5,color=color[i],label=source)
        
    ax.set_xlabel('hour')
    if normalize:
        ax.set_ylabel('volume (% of daily volume)')
    else:
        ax.set_ylabel('volume (veh)')
        
    return ncounts, average, ndays

def daily_vol(ax1,ax2,data,station,direction,date,count,color,source):
    ''' data: Dataframe to be plotted; preprocessed to have 
                - 'time_15': index of 15min bin in a day
                - station: indicates segment info (centreline_id, index, etc)
                - direction: indicates the direction (content can be +/- 1, EB/WB/SB/NB, etc)
                - date: count_date
                - count: actual count
        station, direction, date, count: Names of the columns in the given Dataframe
        color: a list of colors to use for different segments '''
    for (d), g0 in data.groupby([date]):
        for (di), g1 in g0.groupby([direction]):
            g2 = g1.groupby([date,direction,station,'time_15'], as_index=False).sum()
            g2['n']=g2[count]
            g3 = g2.groupby([date,direction,station], as_index=False).agg({'n':lambda x: (x>0).sum(), count:np.sum})
            g3 = g3[g3['n']>64]            
            g3[count] = g3[count]/g3['n']*96
            if di == 'EB' or di==1:
                ax1.scatter(g3[station].tolist(),g3[count].tolist(),color=color[0],label=source)
            else:
                ax2.scatter(g3[station].tolist(),g3[count].tolist(),color=color[0],label=source)
    plt.xticks(np.arange(0, 11, 1))
    locs, labels = plt.xticks()
    plt.setp(labels, rotation = 45)
    ax1.set_title('Lake Shore Eastbound')
    ax2.set_title('Lake Shore Westbound')
    stations = ['Ontario Pl','Strachan','','Fort York','Remembrance','Stadium','Bathurst','','Dan Leckie','','Spadina','Rees']
    ax1.set_xticklabels(stations)
    ax1.grid(b=True)
    ax2.grid(b=True)

def seasonality_plot(ax,data,station,direction,date,count,gg,color):
    ''' data: Dataframe to be plotted; preprocessed to have 
                - 'month': 1-12
                - 'time_15': index of 15min bin in a day
                - station: indicates segment info (centreline_id, index, etc)
                - direction: indicates the direction (content can be +/- 1, EB/WB/SB/NB, etc)
                - date: count_date
                - count: actual count
        station, direction, date, count: Names of the columns in the given Dataframe
        color: a list of colors to use for different segments '''
    gstation = data.groupby([station,direction])
    g0 = gstation.get_group(gg)
    for (m), g1 in g0.groupby(['month']):
        g2 = g1.groupby([date,direction,station,'time_15'], as_index=False).sum() #sum if summing all detectors, min if plotting single lane detections
        g2['n']=g2[count]
        g3 = g2.groupby([date,direction,station], as_index=False).agg({'n':lambda x: (x>0).sum(), count:np.sum})       
        g3 = g3[g3['n']>64]       
        g3[count] = g3[count]/g3['n']*96
        if (not g3[date].tolist()) == False:
            ax.scatter(g3[date].tolist(),g3[count].tolist(),color=color[m-1])
            ax.plot(g3[date].tolist(),g3[count].tolist(),color=color[m-1])
    #ax.legend(prop={'size':10},bbox_to_anchor=(1.13,1.03))
    #ax.set_title('Lake Shore')
    #stations = ['Ontario Pl','Strachan','','Fort York','Remembrance','Stadium','Bathurst','','Dan Leckie','Spadina','Rees']
    #ax1.set_xticklabels(stations)
    ax.grid(b=True)
    
