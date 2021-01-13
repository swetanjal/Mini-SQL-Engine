import sqlparse
import sys

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
    for table in tables:
        data = load_table(table)
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
            if isinstance(query_tokens.tokens[j], sqlparse.sql.Identifier):
                tb_name = query_tokens.tokens[j].get_name()
                available_columns = get_columns(tb_name)
                if available_columns == None:
                    print("Invalid table name!")
                    exit(0)
                data = perform_join([tb_name])
                break
            if isinstance(query_tokens.tokens[j], sqlparse.sql.IdentifierList):
                tb_names = []
                for identifier in query_tokens.tokens[j].get_identifiers():
                    tb_names.append(identifier.get_name())
                    add_cols = get_columns(identifier.get_name())
                    if add_cols == None:
                        print("Invalid table name!")
                        exit(0)
                    available_columns = available_columns + add_cols
                data = perform_join(tb_names)
                break
            if query_tokens.tokens[j].ttype == sqlparse.tokens.Wildcard:
                print("Expected Table name but found wildcard! Semantic Error!")
                exit(0)
            continue
        break

print(available_columns)
print(data)

# Parse for Distinct Selector
distinct_selector = False
for token in query_tokens.tokens:
    if token.ttype == sqlparse.tokens.Keyword and token.value.upper() == 'DISTINCT':
        distinct_selector = True

# Parse for columns
columns = []
for i, token in enumerate(query_tokens.tokens):
    if token.ttype == sqlparse.tokens.DML:
        # Look for first wildcard/Identifier/IdentifierList
        for j in range(i + 1, len(query_tokens.tokens)):
            if isinstance(query_tokens.tokens[j], sqlparse.sql.Identifier):
                break
            if isinstance(query_tokens.tokens[j], sqlparse.sql.IdentifierList):
                break
            if query_tokens.tokens[j].ttype == sqlparse.tokens.Wildcard:
                break
            continue
        break


#where = token for token in query_tokens.tokens if isinstance(token, sqlparse.sql.Where)
#print(where)