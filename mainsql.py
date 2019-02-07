import sys

from util import col_head
from util import get_tabs_cols
from util import join_ndata
from util import load_meta
from util import otpt
from util import read_tab
from util import remove_redundant_spaces
from util import search_col

# Constants
MAX = 'max'
MIN = 'min'
SUM = 'sum'
AVG = 'avg'
DISTINCT = 'distinct'
FROM = 'from'
SELECT = 'select'
WHERE = 'where'
AND = 'and'
OR = 'or'
FUNCTIONS = [DISTINCT, MAX, SUM, AVG, MIN]
KEYWORDS = [SELECT, FROM, WHERE, AND, OR]


def main():
	queries = str(sys.argv[1])
	if ';' not in queries:
		print("Error : Missing ; in the query")
		sys.exit(-1)
	queries = queries.split(';')
	meta_dict = load_meta()
	for query in queries:
		query = clean_query(query)
		if query:
			process(query, meta_dict)


def clean_query(query):
	"""
	Clean the query
	1.  Convert the keywords and function names to smaller case if they are
		present in upper case and return the query
	2.  Remove redundant spaces

	:param query: query to be cleaned
	:return: cleaned version of query
	"""
	for keyword in KEYWORDS:
		query = query.replace(keyword.upper(), keyword)
	for function in FUNCTIONS:
		query = query.replace(function.upper(), function)
	return remove_redundant_spaces(query)


def process(query, meta_dict):
	"""
	Process the query and print the result
	:param query: query to be processed
	:param meta_dict: Dictionary containing meta data of the database
	"""
	ls = []
	# Perform standard error handling around the `from` part of the query
	if FROM not in query.lower().split():
		print("Error : No table selected")
		sys.exit(-1)
	else:
		ls = query.split(FROM)
		if len(ls) != 2:
			print("Error: Too many from statements")
			sys.exit(-1)

	fir = remove_redundant_spaces(str(ls[0]))
	if SELECT not in fir.lower().split():
		print("Error: Not a select query")
		sys.exit(-1)
	elif query.lower().count(SELECT) > 1:
		print("Error: More than one select query can't be processed")
		sys.exit(-1)

	sec = remove_redundant_spaces(str(ls[1]))
	sectc = sec.split(WHERE)
	tables = remove_redundant_spaces(sectc[0]).split(',')
	for i in range(0, len(tables)):
		tables[i] = remove_redundant_spaces(tables[i])
	tables_dict = {}
	table_names = set(meta_dict.keys())
	required_tables = set(tables)
	if not required_tables.issubset(table_names):
		print("Error:Requested table name not found in given tables")
		sys.exit(-1)

	for table in tables:
		tables_dict[table] = read_tab(table)

	remfir = fir[7:]
	remfir = remove_redundant_spaces(remfir).split(',')
	fp = []
	dp = []
	col = []
	sel_fun(remfir, fp, dp, col)

	# Error handling of faulty select statement
	if len(col) + len(fp) + len(dp) < 1:
		print('Error: There is nothing given to select')
		sys.exit(-1)
	if len(sectc) > 1 and (len(fp) != 0 or len(dp) != 0):
		print('Error: Where cannot be used in this combination')
		sys.exit(-1)
	if len(dp) != 0 and len(fp) != 0:
		print('Error: This combination of aggregate and distinct cannot be used')
		sys.exit(-1)

	# Process the query appropriately based on requirements
	if len(sectc) > 1 and len(tables) == 1:
		where_fun(sectc[1], col, tables[0], meta_dict, tables_dict[tables[0]])
	elif len(sectc) > 1 and len(tables) > 1:
		where_fun_mul(sectc[1], col, tables, meta_dict, tables_dict)
	elif len(fp) != 0:
		aggregate_fun(fp, tables, meta_dict, tables_dict)
	elif len(dp) != 0:
		distinct_fun(dp, tables, meta_dict, tables_dict)
	elif len(tables) > 1:
		join_fun(col, tables, meta_dict, tables_dict)
	else:
		select_fun(col, tables[0], meta_dict, tables_dict)


def gen_eval(cnd, tab, tab_info, dat):
	cnd = cnd.split(' ')
	eval_statement = ''
	for i in cnd:
		i = remove_redundant_spaces(i)
		if i == '=':
			eval_statement += i * 2
		elif i.lower() == AND or i.lower == OR:
			eval_statement += ' ' + i.lower() + ' '
		elif '.' in i:
			tab_here, col = search_col(i, [tab], tab_info)
			if tab_here != tab:
				print('Error: Unknown Table')
				sys.exit(-1)

			elif col not in tab_info[tab]:
				print('Error: Unknown Column')
				sys.exit(-1)
			eval_statement += dat[tab_info[tab].index(col)]
		elif i in tab_info[tab]:
			eval_statement += dat[tab_info[tab].index(i)]
		else:
			eval_statement += i
	return eval_statement


def sel_fun(remfir, fp, dp, col):
	h = 0
	for sel_type in remfir:
		if 'distinct' in sel_type.lower():
			h = 1
	if h == 1:
		for sel_type in remfir:
			sel_type = remove_redundant_spaces(sel_type)
			if sel_type != '':
				dp.append(sel_type.strip('distinct'))
		return
	for sel_type in remfir:
		# print(sel_type)
		fourfun = 0
		sel_type = remove_redundant_spaces(sel_type)
		for fun_type in FUNCTIONS:
			if fun_type + '(' in sel_type.lower():
				fourfun = 1
				if ')' not in sel_type:
					print("Error: Wrong Syntax no ) found")
					sys.exit(-1)
				else:
					colhead = sel_type.strip(')').split(fun_type + '(')[1]
				if fun_type.lower() != 'distinct':
					fp.append([fun_type, colhead])
				break
		if fourfun == 0:
			sel_type = remove_redundant_spaces(sel_type)
			if sel_type != '':
				col.append(sel_type.strip('()'))


def distinct_fun(dp, tables, meta_dict, tables_dict):
	colh = ''
	dat = set()
	col1 = remove_redundant_spaces(dp[0])
	col2 = remove_redundant_spaces(dp[1])
	if len(dp) > 2 or len(tables) > 1:
		print("Error: Distinct is only for a pair of cols from the same table")
		sys.exit(-1)
	tab = ''
	for col in dp:
		col = remove_redundant_spaces(col)
		tab, col = search_col(col, tables, meta_dict)
		colh += tab + '.' + col + ','
	print(colh.strip(','))
	for row in tables_dict[tab]:
		val = row[meta_dict[tab].index(col1)] + ',' + row[
			meta_dict[tab].index(col2)]
		dat.add(val)

	for i in dat:
		print(i)


def aggregate_fun(fp, tabs, meta_dict, tables_dict):
	colh, res = '', ''
	for i in fp:
		fun_name = i[0]
		col_name = i[1]
		tab, col = '', ''
		if '.' in col_name:
			tab, col = col_name.split('.')
		else:
			cnt = 0
			for table in tabs:
				if col_name in meta_dict[table]:
					if cnt > 1:
						print("Error: Ambiguous column name")
						sys.exit(-1)
					tab = table
					col = col_name
					cnt += 1
			if cnt == 0:
				print("Error:No column name")
				sys.exit(-1)
		colh += tab + '.' + col
		dat = []
		for row in tables_dict[tab]:
			val = row[meta_dict[tab].index(col)]
			dat.append(int(val))
		if fun_name.lower() == MAX:
			res += str(max(dat))
		elif fun_name.lower() == MIN:
			res += str(min(dat))
		elif fun_name.lower() == SUM:
			res += str(sum(dat))
		elif fun_name.lower() == AVG:
			res += str(float(sum(dat)) / len(dat))
	# res += ','
	colh.strip(',')
	print(fun_name + '(' + colh + ')')
	print(res)


def where_fun(cnd, cols, tabs, table_info, tables_dict):
	cnd = remove_redundant_spaces(cnd)
	if len(cols) == 1 and cols[0] == '*':
		cols = table_info[tabs]
	print(col_head(tabs, cols))
	for row in tables_dict:
		evaluator = gen_eval(cnd, tabs, table_info, row)
		ans = ''
		if eval(evaluator):
			for col in cols:
				ans += row[table_info[tabs].index(col)] + ','
			print(ans.strip(','))

	return


def where_fun_mul(cnd, col, tables, table_info, tables_dict):
	cnd = remove_redundant_spaces(cnd)
	sent = cnd
	operns = ['<', '>', '=', '>=', '<=']
	oper = ''
	if AND in cnd.lower():
		cnd = cnd.split(AND)
		oper = AND
	elif OR in cnd.lower():
		cnd = cnd.split(OR)
		oper = OR
	else:
		cnd = [cnd]
	if len(cnd) > 2:
		print('Max of one and / or clause can be given')
	cnd1 = cnd[0]
	for opern in operns:
		if opern in cnd1:
			cnd1 = cnd1.split(opern)
	if len(cnd1) == 2 and '.' in cnd1[1]:
		where_fun_join([cnd, oper], col, tables, table_info, tables_dict)
		return
	where_fun_spcl(sent, col, tables, table_info, tables_dict)


def where_fun_spcl(sent, col, tables, table_info, tables_dict):
	cnd = []
	oper = ''
	if AND in sent.lower().split():
		oper = AND
		cnd = sent.split(AND)
	elif OR in sent.lower():
		oper = OR
		cnd = sent.split(OR)
	else:
		cnd = [sent]
	dat = get_data(cnd, tables, tables_dict, table_info)
	cols_in_tab, tabs_given = get_tabs_cols(col, tables, table_info)
	tabs_data = join_ndata(oper, tabs_given, dat, tables_dict)
	otpt(tabs_given, cols_in_tab, table_info, tabs_data, True)


def get_data(cnd, tables, tables_dict, table_info):
	operns = ['<', '>', '=', '>=', '<=']
	dat = {}
	for q in cnd:
		nec = []
		for op in operns:
			if op in q:
				nec = q.split(op)
				break
		if len(nec) != 2:
			print('Syntax error in where condition')
			sys.exit(-1)
		tab, col = search_col(remove_redundant_spaces(nec[0]), tables,
							  table_info)
		dat[tab] = []
		q = q.replace(nec[0], ' ' + col + ' ')
		for data in tables_dict[tab]:
			evaltr = gen_eval(q, tab, table_info, data)
			try:
				if eval(evaltr):
					dat[tab].append(data)
			except NameError:
				print('And clause cannot be used with join')
				sys.exit(-1)
	return dat


def where_fun_join(sectc, cols, tabs, tab_info, tab_dict):
	nec_data = {}
	fail_data = {}
	operns = ['<', '>', '=']
	for cnd in sectc[0]:
		nec = []
		oper = ''
		cnd = remove_redundant_spaces(cnd)
		for op in operns:
			if op in cnd:
				nec = cnd.split(op)
				oper = op
				if oper == '=':
					oper *= 2
				break
		if len(nec) > 2:
			print('Error in where clause')
			sys.exit(-1)
		cols_cndn, tabs_cndn = get_tabs_cols(nec, tabs, tab_info)
		tab1 = tabs[0]
		tab2 = tabs[1]
		col1 = tab_info[tab1].index(cols_cndn[tab1][0])
		col2 = tab_info[tab2].index(cols_cndn[tab2][0])
		fail_data[cnd] = []
		nec_data[cnd] = []
		for data in tab_dict[tab1]:
			for row in tab_dict[tab2]:
				evaltr = data[col1] + oper + row[col2]
				
				if eval(evaltr):
					nec_data[cnd].append(data + row)
					#print(nec_data[cnd])
				else:
					fail_data[cnd].append(data + row)
	if sectc[1] != '':
		join_data = join_ndata(sectc[1], sectc[0], nec_data, fail_data)
	else:
		join_data = []
		for key in nec_data.keys():
			for d in nec_data[key]:
				join_data.append(d)
	cols, tabs = get_tabs_cols(cols, tabs, tab_info)
	otpt(tabs, cols, tab_info, join_data, True)


def join_fun(cols, tables, table_info, tables_dict):
	cols_in_tab, tabs_nec = get_tabs_cols(cols, tables, table_info)
	join_data = []
	if len(tabs_nec) == 2:
		tab1 = remove_redundant_spaces(tabs_nec[0])
		tab2 = remove_redundant_spaces(tabs_nec[1])
		for it1 in tables_dict[tab1]:
			for it2 in tables_dict[tab2]:
				join_data.append(it1 + it2)
		otpt(tabs_nec, cols_in_tab, table_info, join_data, True)
	else:
		otpt(tabs_nec, cols_in_tab, table_info, join_data, False)
	return


def select_fun(cols, tabs, tab_info, tab_dict):
	""" Deals with project operation without where condition"""
	if len(cols) == 1 and cols[0] == '*':
		cols = tab_info[tabs]

	cols_present = set(tab_info[tabs])
	cols_needed = set(cols)
	if not cols_needed.issubset(cols_present):
		print("Column requested in select doesn't exist.")
		sys.exit(-1)
	print(col_head(tabs, cols))
	for data in tab_dict[tabs]:
		ans = ''
		for col in cols:
			ans += data[tab_info[tabs].index(col)] + ','
		print(ans.strip(','))


if __name__ == '__main__':
	main()
