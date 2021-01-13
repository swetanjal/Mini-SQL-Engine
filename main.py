import sqlparse
import sys
import re

def load_table(table):
    arr = []
    f = open('files/' + table + '.csv')
    lines = f.readlines()
    for line in lines:
        toks = line.split(',')
        tmp = []
        for t in toks:
            tmp.append(int(t))
        arr.append(tmp)
    return arr

def perform_join(tables):
    curr = []
    for i, table in enumerate(tables):
        if i == 0:
            curr = load_table(table)
            continue
        data = load_table(table)
        new_curr = []
        for left_element in curr:
            for right_element in data:
                new_curr.append(left_element + right_element)
        curr = new_curr
    return curr

def get_columns(table):
    f = open('files/metadata.txt')
    lines = f.readlines()
    for i, line in enumerate(lines):
        line = line.strip()
        if line == '<begin_table>':
            table_name = lines[i + 1].strip()
            cols = []
            j = i + 2
            while lines[j].strip() != '<end_table>':
                cols.append(lines[j].strip())
                j = j + 1
            if table_name == table:
                return cols
    return None

def parse_column(s):
    global is_aggregate
    global is_non_aggregate
    s = str(s)
    find_max = re.compile("max\((.*)\)")
    find_min = re.compile("min\((.*)\)")
    find_mean = re.compile("average\((.*)\)")
    find_sum = re.compile("sum\((.*)\)")
    find_count = re.compile("count\((.*)\)")
    if find_max.match(s) is not None:
        is_aggregate = True
        return (find_max.match(s).groups()[0], 'max')
    if find_min.match(s) is not None:
        is_aggregate = True
        return (find_min.match(s).groups()[0], 'min')
    if find_mean.match(s) is not None:
        is_aggregate = True
        return (find_mean.match(s).groups()[0], 'mean')
    if find_sum.match(s) is not None:
        is_aggregate = True
        return (find_sum.match(s).groups()[0], 'sum')
    if find_count.match(s) is not None:
        is_aggregate = True
        return (find_count.match(s).groups()[0], 'count')
    is_non_aggregate = True
    return (s, None)

def parse_expression(s):
    s = re.sub(r'where ', '', s)
    eq = re.compile("(.*) = (.*)")
    lt = re.compile("(.*) < (.*)")
    gt = re.compile("(.*) > (.*)")
    le = re.compile("(.*) <= (.*)")
    ge = re.compile("(.*) >= (.*)")
    if eq.match(s) is not None and len(eq.match(s).groups()) == 2:
        return (eq.match(s).groups()[0].strip() ,lambda lhs: lhs == int(eq.match(s).groups()[1].strip()))
    if lt.match(s) is not None and len(lt.match(s).groups()) == 2:
        return (lt.match(s).groups()[0].strip() ,lambda lhs: lhs < int(lt.match(s).groups()[1].strip()))
    if gt.match(s) is not None and len(gt.match(s).groups()) == 2:
        return (gt.match(s).groups()[0].strip() ,lambda lhs: lhs > int(gt.match(s).groups()[1].strip()))
    if le.match(s) is not None and len(le.match(s).groups()) == 2:
        return (le.match(s).groups()[0].strip() ,lambda lhs: lhs <= int(le.match(s).groups()[1].strip()))
    if ge.match(s) is not None and len(ge.match(s).groups()) == 2:
        return (ge.match(s).groups()[0].strip() ,lambda lhs: lhs >= int(ge.match(s).groups()[1].strip()))
    print("Syntax Error: Invalid expression", s)
    exit(0)

def transform_columns(exp, available_columns):
    for i, v in enumerate(available_columns):
        print(exp[0])
        if exp[0] == v:
            return (lambda s: exp[1](s[i]))
    print("Semantic Error: Column does not exist!")
    exit(0)

if len(sys.argv) != 2:
    print("Incorrect Usage! Expected Usage: python3 main.py query")
    exit(0)
query = sys.argv[1]
query_tokens = sqlparse.parse(query)[0]

if query_tokens.get_type() == 'UNKNOWN':
    print("Query Syntax Error")
    exit(0)
elif query_tokens.get_type() == 'UPDATE':
    print("Query not supported")
    exit(0)
elif query_tokens.get_type() == 'CREATE':
    print("Query not supported")
    exit(0)
elif query_tokens.get_type() == 'DELETE':
    print("Query not supported")
    exit(0)


# Parse for valid table
available_columns = []
data = []
for i, token in enumerate(query_tokens.tokens):
    if token.ttype == sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
        # Look for first wildcard/Identifier/IdentifierList
        for j in range(i + 1, len(query_tokens.tokens)):
            if query_tokens.tokens[j].ttype == sqlparse.tokens.Keyword:
                print("Syntax error: No columns to select from!")
                exit(0)
            if isinstance(query_tokens.tokens[j], sqlparse.sql.Identifier):
                tb_name = query_tokens.tokens[j].get_name()
                available_columns = get_columns(tb_name)
                if available_columns == None:
                    print("Semantic Error: Invalid table name", tb_name)
                    exit(0)
                data = perform_join([tb_name])
                break
            if isinstance(query_tokens.tokens[j], sqlparse.sql.IdentifierList):
                tb_names = []
                for identifier in query_tokens.tokens[j].get_identifiers():
                    tb_names.append(identifier.get_name())
                    add_cols = get_columns(identifier.get_name())
                    if add_cols == None:
                        print("Semantic Error: Invalid table name", (identifier.get_name()))
                        exit(0)
                    available_columns = available_columns + add_cols
                data = perform_join(tb_names)
                break
            if query_tokens.tokens[j].ttype == sqlparse.tokens.Wildcard:
                print("Semantic Error: Expected Table name but found wildcard!")
                exit(0)
            continue
        break

if available_columns == []:
    print("Syntax error: No table to select from!")
    exit(0)

# Parse for Distinct Selector
distinct_selector = False
for token in query_tokens.tokens:
    if token.ttype == sqlparse.tokens.Keyword and token.value.upper() == 'DISTINCT':
        distinct_selector = True


# Parse for columns
columns = []
is_aggregate = False
is_non_aggregate = False
for i, token in enumerate(query_tokens.tokens):
    
    if token.ttype == sqlparse.tokens.DML:
        # Look for first wildcard/Identifier/IdentifierList
        for j in range(i + 1, len(query_tokens.tokens)):
            if query_tokens.tokens[j].ttype == sqlparse.tokens.Keyword and query_tokens.tokens[j].value.upper() == 'FROM':
                print("Syntax error: No columns to select!")
                exit(0)
            if isinstance(query_tokens.tokens[j], sqlparse.sql.Identifier):
                column_name, aggregate = parse_column(query_tokens.tokens[j])
                if not (column_name in available_columns):
                    print("Semantic Error: Invalid column name", column_name)
                    exit(0)
                columns.append([column_name, aggregate])
                break
            if isinstance(query_tokens.tokens[j], sqlparse.sql.IdentifierList):
                for identifier in query_tokens.tokens[j].get_identifiers():
                    column_name, aggregate = parse_column(identifier)
                    
                    if not (column_name in available_columns):
                        print("Semantic Error: Invalid column name", column_name)
                        exit(0)
                    columns.append([column_name, aggregate])
                break
            if query_tokens.tokens[j].ttype == sqlparse.tokens.Wildcard:
                columns = available_columns
                break
            if query_tokens.tokens[j].ttype == sqlparse.tokens.Keyword and query_tokens.tokens[j].value.upper() == 'DISTINCT':
                continue
            
            if query_tokens.tokens[j].ttype == sqlparse.tokens.Text.Whitespace:
                continue
            
            column_name, aggregate = parse_column(query_tokens.tokens[j])
            if not (column_name in available_columns):
                print("Semantic Error: Invalid column name", column_name)
                exit(0)
            columns.append([column_name, aggregate])
            break
        break

if is_aggregate and is_non_aggregate:
    print("Semantic Error: Cannot combine aggregate with non aggregate projection!")
    exit(0)

filt = lambda s: True

for token in query_tokens.tokens:
    if isinstance(token, sqlparse.sql.Where):
        token = str(token)
        if token.find(" AND ") != -1:
            tokens = token.split(" AND ")
            exp1 = parse_expression(tokens[0])
            exp2 = parse_expression(tokens[1])
            exp1 = transform_columns(exp1, available_columns)
            exp2 = transform_columns(exp2, available_columns)
            filt = lambda s: exp1(s) and exp2(s)
        elif token.find(" OR ") != -1:
            tokens = token.split(" OR ")
            exp1 = parse_expression(tokens[0])
            exp2 = parse_expression(tokens[1])
            exp1 = transform_columns(exp1, available_columns)
            exp2 = transform_columns(exp2, available_columns)
            filt = lambda s: exp1(s) or exp2(s)
        else:
            exp1 = parse_expression(token)
            exp1 = transform_columns(exp1, available_columns)
            filt = lambda s: exp1(s)
        break

filtered_data = []
for row in data:
    if filt(row):
        filtered_data.append(row)

print(filtered_data)
#where = token for token in query_tokens.tokens if isinstance(token, sqlparse.sql.Where)
#print(where)