import sys
import re
import csv
from collections import defaultdict

def load_meta():
    try:
        meta_dict = {}
        with open('metadata.txt', 'r') as mfile:
            i = 0
            tab_n = ""
            for row in mfile:
                if row.strip() == "<begin_table>":
                    i = 1
                elif i == 1:
                    i = 0
                    tab_n = row.strip()
                    meta_dict[tab_n] = []
                elif row.strip() != "<end_table>":
                    meta_dict[tab_n].append(row.strip())
    except IOError:
        print("Error: The File is not found ")
        sys.exit()
    return meta_dict


def remove_redundant_spaces(sq):
    return re.sub(' +', ' ', sq).strip()


def read_tab(tabn):
    tabn += ".csv"
    try:
        with open(tabn, 'r') as cfile:
            cont = csv.reader(cfile)
            tabl = []
            for line in cont:
                tabl.append(line)
    except IOError:
        print("Error: The table" + tabn + "is not found ")
        sys.exit()
    # print(tabl)
    return tabl


def col_head(tabn, cols):
    s = ''
    for col in cols:
        if s == '':
            s += tabn + '.' + col
        else:
            s += ','
            s += tabn + '.' + col
    # print(s)
    return s


def otpt(tabs_given, cols_in_tab, tab_info, tabs_data, join):
    if join:
        tab0 = tabs_given[0]
        tab1 = tabs_given[1]
        head0 = col_head(tab0, cols_in_tab[tab0])
        head1 = col_head(tab1, cols_in_tab[tab1])
        print(head0 + ',' + head1)
        for it in tabs_data:
            ans = ''
            for col in cols_in_tab[tab0]:
                ans += it[tab_info[tab0].index(col)] + ','
            for col in cols_in_tab[tab1]:
                ans += it[tab_info[tab1].index(col) + len(tab_info[tab0])] + ','
            print(ans.strip(','))
    else:
        for tab in tabs_given:
            print(col_head(tab, cols_in_tab[tab]))
            for data in cols_in_tab[tab]:
                ans = ''
                for col in cols_in_tab[tab]:
                    ans += data[tab_info[tab].index(col)] + ','
                print(ans.strip(','))


def get_tabs_cols(cols, tabs, tab_info):
    cols_in_tab = defaultdict(list)
    if len(cols) == 1 and cols[0] == '*':
        for tab in tabs:
            cols_in_tab[tab] = []
            for col in tab_info[tab]:
                cols_in_tab[tab].append(col)
        return cols_in_tab, tabs
    for col in cols:
        tab, col = search_col(col, tabs, tab_info)
        cols_in_tab[tab].append(col)
    return cols_in_tab, list(cols_in_tab.keys())


def search_col(col, tabs, tab_info):
    if '.' in col:
        tab, col = col.split('.')
        tab = remove_redundant_spaces(tab)
        col = remove_redundant_spaces(col)
        if tab not in tabs:
            print('Error: No such table exists')
            sys.exit()
        return tab, col
    cnt = 0
    tabs_given = ''
    for tab in tabs:
        if col in tab_info[tab]:
            cnt += 1
            if cnt > 1:
                print('Error: Ambiguous column name')
                sys.exit()
            tabs_given = tab
    if cnt == 0:
        print('Error: No such column found' + col)
        sys.exit()
    return tabs_given, col


def join_ndata(oper, tab, dat, tabs_data):
    fin_data = []
    if oper == 'and':
        tab1 = remove_redundant_spaces(tab[0])
        tab2 = remove_redundant_spaces(tab[1])
        for it1 in dat[tab1]:
            for it2 in dat[tab2]:
                fin_data.append(it1 + it2)
    elif oper == 'or':
        tab1 = remove_redundant_spaces(tab[0])
        tab2 = remove_redundant_spaces(tab[1])
        for it1 in dat[tab1]:
            for it2 in tabs_data[tab2]:
                if it2 not in dat[tab2]:
                    fin_data.append(it1 + it2)
        for it1 in dat[tab2]:
            for it2 in tabs_data[tab1]:
                if it2 not in dat[tab1]:
                    fin_data.append(it2 + it1)
    else:
        tab1 = dat.keys()[0]
        flag = False
        tab2 = tab[1]
        if tab1 == tab[1]:
            tab2 = tab[0]
            flag = True
        for it1 in dat[tab1]:
            for it2 in tabs_data[tab2]:
                if not flag:
                    fin_data.append(it2 + it1)
                    continue
                fin_data.append(it1 + it2)

    return fin_data
