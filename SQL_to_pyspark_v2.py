# from types import NoneType
from moz_sql_parser import parse
from moz_sql_parser import format
import json
query = """

"""

# print(json.dumps(v_parse,indent=4))
# exit()
resultName = "Result"

queries = []

prefix = "Df_"

joins = {
    'inner join'        : '',
    'left join'         : ", 'left'",
    'right join'        : ", 'right'",
    'full join'         : ", 'full'",
    'left outer join'   : ", 'left_outer'",
    'right outer join'  : ", 'right_outer'",
    'full outer join'   : ", 'full_outer'",
}

operators = {
    'eq'    : '==',
    'lt'    : '<',
    'lte'   : '<=',
    'gt'    : '>',
    'gte'   : '>=',
    'neq'   : '!=',
    'div'   : '/',
    'mul'   : '*'
}

math_functions = [
    'sum',
    'min',
    'max',
    'abs',
    'avg',
    'count',
    'floor',
    'ceiling'
]

math_operators = {
    'div': ' / ',
    'mul': ' * ',
    'sub': ' - ',
    'add': ' + '
}

def fn_isnull(null_str):
    try:
        if type(null_str['isnull'][0]) is dict:
            op = next(iter(null_str['isnull'][0]))
            if op in fn_dict.keys():
                v1 = (fn_dict[op](null_str['isnull'][0]))
            elif op == 'literal':
                v1 = ("lit(\"" + null_str['isnull'][0][op] + "\")")
            elif op in other_functions:
                v1 = (other_functions[op](null_str['isnull'][0][op]))
            elif op in math_functions:
                v1 = (math_functions[op](null_str['isnull'][0][op]))
        else:
            v1 = "col(\""+null_str['isnull'][0]+"\")"
        v2 = ("col(\"" + null_str['isnull'][1] + "\")"  if type(null_str['isnull'][1]) is str else "lit(" + str(null_str['isnull'][1]) + ")" if type(null_str['isnull'][1]) is not dict else "lit(\"" + null_str['isnull'][1]['literal'] + "\")" if (type(null_str['isnull'][1]) is dict) and ('literal' in null_str['isnull'][1]) else format({"from": null_str['isnull'][1]})[5:])
        return "coalesce(" + v1 + ", "+v2+")"
    except:
        return format({'from': null_str})[5:]

def fn_math_functions(math):
    try:
        result_math = ""
        key = next(iter(math))
        if type(math[key]) is dict:
            op = next(iter(math[key]))
            if op in fn_dict.keys():
                result_math += key + "(" + fn_dict[op](math[key]) + ")"
            else:
                result_math += format({"from": math[key]})[5:]
        else: 
            result_math += key + "(col(\"" + (math[key]) + "\"))"
        return result_math
    except:
        return format({'from': math})[5:]

def fn_condition(condition):
    try:
        op = next(iter(condition))
        result_select = ""
        if op in operators.keys():
            if type(condition[op][0]) is dict:
                key = next(iter(condition[op][0]))
                if key in fn_dict.keys():
                    result_select += fn_dict[key](condition[op][0])
                elif 'literal' == key:
                    result_select += "\"" + condition[op][0]['literal'] + "\""
                elif key in math_functions:
                    result_select += fn_math_functions(condition[op][0])
                else:
                    result_select += format({"from": condition[op][0]})[5:]
            else:
                result_select += ("col(\""+condition[op][0]+"\") " if type(condition[op][0]) is str else str(condition[op][0]))

            result_select += operators[op]
            
            if type(condition[op][1]) is dict:
                key = next(iter(condition[op][1]))
                if key in fn_dict.keys():
                    result_select += fn_dict[key](condition[op][1])
                elif 'literal' == key:
                    result_select += "\"" + condition[op][1]['literal'] + "\""
                else:
                    result_select += format({"from": condition[op][1]})[5:]
            else:
                result_select += ("col(\""+condition[op][1]+"\") " if type(condition[op][1]) is str else str(condition[op][1]))

        elif op == "between":
            if type(condition[op][1]) is dict:
                key = next(iter(condition[op][1]))
                if key in fn_dict.keys():
                    val1 = fn_dict[key](condition[op][1])
                elif 'literal' == key:
                    val1 = "\"" + condition[op][1]['literal'] + "\""
                else:
                    result_select += format({"from": condition[op][1]})[5:]
            else:
                val1 = (str("col(\"" + condition[op][1] + "\")" if type(condition[op][1]) is str else condition[op][1]))
            
            if type(condition[op][2]) is dict:
                key = next(iter(condition[op][2]))
                if key in fn_dict.keys():
                    val2 = fn_dict[key](condition[op][2])
                elif 'literal' == key:
                    val2 = "\"" + condition[op][2]['literal'] + "\""
                else:
                    result_select += format({"from": condition[op][2]})[5:]
            else:
                val2 = (str("col(\"" + condition[op][2] + "\")" if type(condition[op][2]) is str else condition[op][2]))
            result_select += "col(\"" + condition[op][0] + "\").between(" + val1 + ", " + val2 + ")"
        
        elif op in ["in", "nin"]:
            if op == "nin":
                result_select += "~("

            if type(condition[op][0]) is dict and 'isnull' in condition[op][0].keys():
                result_select += fn_isnull(condition[op][0]) 
            else:
                result_select += "col(\"" + condition[op][0] + "\")"
            
            if type(condition[op][1]) is dict and 'select' in condition[op][1].keys():
                tableName = prefix + (condition[op][1]['from'][0]['value'].title() if type(condition[op][1]['from']) is list else condition[op][1]['from'].title() if type(condition[op][1]['from']) is str else condition[op][1]['from']['value'].title())
                queries.append({
                    'name'  : tableName,
                    'query' : fn_genSQL(condition[op][1]) + ".rdd.flatMap(lambda x: x).collect()"
                })
                result_select += ".isin(" + tableName + ")"   
            else:
                result_select += ".isin(" + (str(condition[op][1]['literal']) if type(condition[op][1]['literal']) is list else "[\"" + condition[op][1]['literal'] +"\"]" ) + ")"   
            
            if op == "nin":
                result_select += ")"

        elif op == "missing":
            if type(condition[op][0]) is dict and 'isnull' in condition[op][0].keys():
                result_select += fn_isnull(condition[op][0]) + ".isNull()"   
            else:
                result_select += "col(\"" + condition[op] + "\").isNull()"

        elif op == "exists":
            if type(condition[op][0]) is dict and 'isnull' in condition[op][0].keys():
                result_select += fn_isnull(condition[op][0]) + ".isNotNull()"   
            else:
                result_select += "col(\"" + condition[op] + "\").isNotNull()"  

        elif op == "like":
            if type(condition[op][0]) is dict and 'isnull' in condition[op][0].keys():
                result_select += fn_isnull(condition[op][0]) + ".like(\"" + str(condition[op][1]['literal']) + "\")"   
            else:
                result_select += "col(\"" + condition[op][0] + "\").like(\"" + str(condition[op][1]['literal']) + "\")"  
        
        elif op == 'or':
            for i in condition[op]:
                result_select += "(" + fn_condition(i) + ") | "
            result_select = result_select[:-2]
        
        elif op == 'and':
            for i in condition[op]:
                result_select += "(" + fn_condition(i) + ") & "
            result_select = result_select[:-2]
        else:
            result_select += format({"from": condition})[5:]

        return result_select
    except:
        return format({'from': condition})[5:]

def fn_when(when_str):
    try:
        result_select = "("
        flag = False
        for case in when_str['case'][:-1]:
            if flag:
                result_select += "\n."
            flag = True

            result_select += "when("
            result_select += fn_condition(case['when'])

            if 'then' in case.keys():
                if (type(case['then']) is dict): 
                    key = next(iter(case['then']))
                    if key in fn_dict.keys():
                        result_select += ", " + (fn_dict[key](case['then'])) + ")"
                    else:
                        result_select += format({"from": case['then']})[5:]
                else:
                    v = ("lit(" + str(case['then']) + ")" if type(case['then']) is not str else "col(\"" + case['then'] + "\")")
                    result_select += ", " + str(v) + ")"
            else:
                result_select += ", " + str(None) + ")"

        if type(when_str['case'][-1]) is dict:
            key = next(iter(when_str['case'][-1]))
            if key in fn_dict.keys():
                result_select += "\n.otherwise(" + (fn_dict[key](when_str['case'][-1])) + "))\n"
            else:
                result_select += format({"from": when_str['case'][-1]})[5:]
        else:
            v = (None if type(when_str['case'][-1]) is type(None) else "lit(" + str(when_str['case'][-1]) + ")" if type(when_str['case'][-1]) is not str else "col(\"" + when_str['case'][-1] + "\")")
            result_select += "\n.otherwise(" + str(v) + "))\n"
        
        return result_select
    except:
        return format({'from': when_str})[5:]

def fn_math_operations(math_str):
    try:
        result_select = "("
        key = next(iter(math_str))
        flag = False
        for val in math_str[key]:
            if flag:
                result_select += math_operators[key]
            if (type(val) is dict) or (type(val) is list):
                op = next(iter(val))
                if op in fn_dict.keys():
                    result_select += (fn_dict[op](val))
                elif op in math_functions:
                    result_select += (fn_math_functions(val))
                else:
                    result_select += format({"from": val})[5:]
            else:
                result_select += (str(fn_math_operations(val)) if type(val) is dict else str(val) if type(val) is not str else "col(\"" + val + "\")")
            flag = True
        result_select += ")"
        return result_select
    except:
        return format({'from': math_str})[5:]

def fn_wrap(_str):
    try:
        result = ""
        if type(_str) is str:
            result = "col(\"" + _str + "\")," 
        elif type(_str) is dict:
            op = next(iter(_str))
            if op in fn_dict.keys():
                result = (fn_dict[op](_str))
            elif op == 'literal':
                if type(_str['literal']) is list:
                    for i in _str['literal']:
                        result += "\"" + i + "\","
                else:
                    result += "\"" + _str['literal'] + "\","
            elif op in other_functions:
                result = (other_functions[op](_str[op]))
            elif op in math_functions:
                result = fn_math_functions(_str)
            else:
                result += format({"from": _str})[5:]
        elif type(_str) is list:
            for i in _str:
                if type(i) is dict:
                    result += fn_wrap(i)
                else:
                    result += "col(\"" + i + "\")," 
        return result
    except:
        return format({'from': _str})[5:]

def fn_check_nested_math(math_str):
    try:
        key = next(iter(math_str))
        for val in math_str[key]:
            if (type(val) is dict) or (type(val) is list):
                op = next(iter(val))
                if op in math_functions:
                    return True
            else:
                return fn_check_nested_math(val)
        return False
    except:
        return False

def fn_coalesce(_str):
    try:
        result = ""
        if type(_str) is str:
            result = "col(\"" + _str + "\")," 
        elif type(_str) is type(None):
            result += str(None) + ","
        elif (type(_str) is int) or (type(_str) is float):
            result += "lit(" + str(_str) + "),"
        elif type(_str) is dict:
            op = next(iter(_str))
            if op in fn_dict.keys():
                result = (fn_dict[op](_str))
            elif op == 'literal':
                if type(_str['literal']) is list:
                    for i in _str['literal']:
                        result += "lit(\"" + i + "\"),"
                else:
                    result += "lit(\"" + _str['literal'] + "\"),"
            elif op in other_functions:
                result = (other_functions[op](_str[op]))
            elif op in math_functions:
                result = fn_math_functions(_str)
            else:
                result += format({"from": _str})[5:]
        elif type(_str) is list:
            for i in _str:
                result += fn_coalesce(i)
        return result
    except:
        return format({'from': _str})[5:]

fn_dict = {
    'isnull': fn_isnull,
    'case'  : fn_when,
    'div'   : fn_math_operations,
    'sub'   : fn_math_operations,
    'mul'   : fn_math_operations,
    'add'   : fn_math_operations,
}

other_functions = {
    'concat'        : lambda x: "concat(" + fn_wrap(x)[:-1] + ")",
    'concat_ws'     : lambda x: "concat_ws(" + fn_wrap(x)[:-1] + ")",
    'replace'       : lambda x: "regexp_replace(" + fn_wrap(x)[:-1] + ")",
    'upper'         : lambda x: "upper(" + fn_wrap(x)[:-1] + ")",
    'sysdatetime'   : lambda x: 'current_timestamp()',
    'month'         : lambda x: "month(" + fn_wrap(x)[:-1] + ")",
    'year'          : lambda x: "year(" + fn_wrap(x)[:-1] + ")",
    'coalesce'      : lambda x: "coalesce(" + fn_coalesce(x)[:-1] + ")"
}

def fn_select(select):
    try:
        response = []
        for col in select:
            if type(col) is str:
                response.append("col(\"" + col + "\")")
            elif type(col) is dict:
                col_str = ""
                if 'value' in col.keys():
                    if type(col['value']) is dict:
                        op = next(iter(col['value']))
                        if op in fn_dict.keys():
                            if op in math_operators.keys():
                                if (fn_check_nested_math(col['value'])):
                                    response.append("col(\"" + col['name'] +"\")")
                                    continue
                            col_str = (fn_dict[op](col['value']))
                        elif op == 'literal':
                            col_str = ("lit(\"" + col['value'][op] + "\")")
                        elif op in other_functions:
                            col_str = (other_functions[op](col['value'][op]))
                        elif (op in math_functions) or (op == 'value'):
                            response.append("col(\"" + col['name'] +"\")")
                            continue
                        else:
                            col_str += format({"from": col['value']})[5:]
                    else:
                        if type(col['value']) is int or type(col['value']) is float:
                            col_str = ("lit(" + str(col['value']) + ")")
                        else:
                            if col['value'] == "CURRENT_TIMESTAMP":
                                col_str = "current_timestamp()"
                            else:
                                col_str = ("col(\"" + col['value'] + "\")")
                if 'name' in col.keys():
                    col_str += ".alias(\"" + col['name'] +"\")"
                response.append(col_str)
        return response
    except:
        return format({'from': select})[5:]

def fn_from(from_str):
    try:
        result_from=""
        if type(from_str) is str:
            if from_str.find(".union") >= 0:
                result_from = from_str
            else:
                result_from = prefix +  from_str.title() + "\n"
        elif type(from_str) is dict:
            key = next(iter(from_str))
            if "value" in from_str.keys():
                result_from += prefix + from_str["value"].title()
            elif key in joins.keys():
                result_from += ".join("
                result_from += fn_from(from_str[key])
                result_from += ", " + fn_condition(from_str['on'])
                result_from += joins[key] + ")\n"
            elif key == "cross join":
                result_from += ".crossJoin("
                result_from += fn_from(from_str[key])
                result_from += ")\n"
            else:
                result_from += format({"from": from_str})[5:]

            if "name" in from_str.keys():
                result_from += ".alias(\""+from_str['name']+"\") "
        elif type(from_str) is list:
            for i in from_str:
                result_from += fn_from(i)

        return (result_from)
    except:
        return format({'from': from_str})[5:]

def fn_groupby(groupby):
    try:
        result_groupby=""
        if type(groupby) is dict:
            result_groupby += "col(\"" + groupby["value"] + "\"),\n"
        else:
            for gb in groupby:
                result_groupby += "col(\"" + gb["value"] + "\"),\n"
        return result_groupby[:-2]
    except:
        return format({'from': groupby})[5:]

def fn_orderby(orderby):
    try:
        result_orderby=""
        if type(orderby) is dict:
            if orderby.get("sort", "asc") == "desc":
                v_sortorder = "desc()"
            else:
                v_sortorder = "asc()"
            result_orderby += "col(\""+str(orderby.get("value", ""))+"\")." +v_sortorder+",\n"
        else:
            for i in orderby:
                result_orderby += fn_orderby(i)
            result_orderby += result_orderby[:-2]
        return result_orderby
    except:
        return format({'from': orderby})[5:]

def fn_agg(select):
    try:
        result_agg = ""
        if type(select) is dict:
            if type(select['value']) is dict:
                key = next(iter(select['value']))
                if key == "value":
                    result_agg += fn_agg(select['value'])
                    if 'name' in select.keys():
                        result_agg += ".alias(\"" + select['name'] + "\"),\n"
                
                elif key in math_functions:
                    result_agg +=fn_math_functions(select['value'])
                    # if ('name' in select.keys()) and ('over' not in select.keys()):
                    if ('name' in select.keys()):
                        result_agg += ".alias(\"" + select['name'] + "\"),\n"
                elif key in math_operators.keys():
                    if (fn_check_nested_math(select['value'])):
                        result_agg +=fn_math_functions(select['value'])
                        if ('name' in select.keys()):
                            result_agg += ".alias(\"" + select['name'] + "\"),\n"


                # if ('over' in select.keys()):
                #     result_agg += ".over("
                #     if 'partitionby' in select['over'].keys():
                #         result_agg += "Window.partitionBy(" + fn_wrap(select['over']['partitionby'])[:-1] + ")"
                #     if 'orderby' in select['over'].keys():
                #         print(select['over']['orderby'])
                #         result_agg += ".orderBy(" + fn_wrap([value['value'] for value in select['over']['orderby']])[:-1] + ")"
                #     result_agg += ")"
                #     if 'name' in select.keys():
                #         result_agg += ".alias(\"" + select['name'] + "\"),\n"
        else:
            for i in select:
                result_agg += fn_agg(i)
            result_agg = result_agg[:-2]
        return result_agg
    except:
        return format({'from': select})[5:]

def fn_having(having):
    try:
    
        def check_dict(d):
            try:
                v_parse = parse(query)
                for s in v_parse["select"]:
                    if s['value'] == d:
                        return s['name']
                return ""
            except:
                return format({'from': d})[5:]

        def fn_solve_agg(having):
            try:
                key = next(iter(having))
                if key in operators.keys():
                    if type(having[key][0]) is dict:
                        having[key][0] = check_dict(having[key][0])
                elif key == 'or':
                    for i in having[key]:
                        fn_having(i)

                elif key == 'and':
                    for i in having[key]:
                        fn_having(i)
                else:
                    result_agg += format({"from": having})[5:]
            except:
                return format({'from': having})[5:]

        fn_solve_agg(having)
        return fn_condition(having)
    except:
        return format({'from': having})[5:]

def fn_genSQL(data):
    try:
        v_fn_from = v_fn_where = v_fn_groupby = v_fn_agg = v_fn_select = v_fn_orderby = v_fn_having = ""
        for key,value in data.items():
            if str(key)=="from":
                v_fn_from = fn_from(value)

            if str(key) =="where":
                v_fn_where = fn_condition(value)

            if str(key) =="groupby":
                v_fn_groupby = fn_groupby(value)

            if str(key) =="groupby":
                value = data['select']
                if (type(value) is dict) and ('distinct' in value['value'].keys()):
                    value = value['value']['distinct']
                v_fn_agg = fn_agg(value)

            if str(key) =="select":
                if (type(value) is dict) and ('distinct' in value['value'].keys()):
                    value = value['value']['distinct']
                v_fn_select = str(",\n".join(fn_select(value)))

            if str(key) =="orderby":
                v_fn_orderby = fn_orderby(value)

            if str(key) =="having":
                v_fn_having = fn_having(value)
                
        v_final_stmt = ""
        if v_fn_from:
            v_final_stmt += v_fn_from[:-1]
        if v_fn_where:
            v_final_stmt += "\n.filter("+v_fn_where+")"
        if v_fn_groupby:
            v_final_stmt += "\n.groupBy("+v_fn_groupby+")"
        if v_fn_agg:
            v_final_stmt += "\n.agg("+v_fn_agg+")"
        if v_fn_select:
            v_final_stmt += "\n.select("+v_fn_select+")"
            if (type(data['select']) is dict) and ('distinct' in data['select']['value'].keys()):
                v_final_stmt += ".distinct()"
        if v_fn_orderby:
            v_final_stmt += "\n.orderBy("+v_fn_orderby+")"
        if v_fn_having:
            v_final_stmt += "\n.filter("+v_fn_having+")"

        v_final_stmt = v_final_stmt.replace("_ID", "_SK")
        return v_final_stmt
    except:
        return format({'from': data})[5:]
        
def fn_get_pyspark(data):
    try:
        global queries
        queries = []
        if ('union_all' in data.keys()) | ('union' in data.keys()):
            tables = ""
            flag = True
            k = next(iter(data))
            for q in data[k]:
                tableName = prefix + (q['from'][0]['value'].title() if type(q['from']) is list else q['from'].title() if type(q['from']) is str else q['from']['value'].title())
                if flag:
                    tables = tableName + "\n"
                else:
                    tables += ".union(" + tableName + ")\n"
                
                queries.append({
                    'name'  : tableName,
                    'query' : fn_genSQL(q)
                })
                flag = False
            queries.append({
                'name'  : prefix + resultName,
                'query' : tables
            })

        elif ((type(data['from']) is not list) and (type(data['from']) is not str) and type(data['from']['value']) is dict) and (('union_all' in data['from']['value'].keys()) | ('union' in data['from']['value'].keys())):
            tables = ""
            flag = True
            k = next(iter(data['from']['value']))
            for q in data['from']['value'][k]:
                tableName = prefix + (q['from'][0]['value'].title() if type(q['from']) is list else q['from'].title() if type(q['from']) is str else q['from']['value'].title())
                if flag:
                    tables = tableName + "\n"
                else:
                    tables += ".union(" + tableName + ")\n"
                
                queries.append({
                    'name'  : tableName,
                    'query' : fn_genSQL(q)
                })
                flag = False
            data['from'] = tables[:-1]
            queries.append({
                'name'  : prefix + resultName,
                'query' : fn_genSQL(data)
            })

        elif ((type(data['from']) is dict) and (type(data['from']['value']) is dict)) and ('select' in data['from']['value'].keys()):
            try:
                tableName = prefix + (data['from']['value']['from'][0]['value'].title() if type(data['from']['value']['from']) is list else data['from']['value']['from'].title() if type(data['from']['value']['from']) is str else data['from']['value']['from']['value'].title())
                queries.append({
                    'name'  : tableName,
                    'query' : fn_genSQL(data['from']['value'])
                })
                data['from']['value'] = tableName
                queries.append({
                    'name'  : prefix + resultName,
                    'query' : fn_genSQL(data)
                })
            except:
                print("Only one level of subquery can be handeled!!")
            
        elif (type(data['from']) is list):
            for q in data['from']:
                k = next(iter(q))
                if (type(q) is not str) and (type(q[k]) is not str):
                    if ((k in joins) and (type(q[k]['value']) is not str)) and  ('select' in q[k]['value'].keys()):
                        tableName = prefix + (q[k]['value']['from'][0]['value'].title() if type(q[k]['value']['from']) is list else q[k]['value']['from'].title() if type(q[k]['value']['from']) is str else q[k]['value']['from']['value'].title())
                        queries.append({
                            'name'  : tableName,
                            'query' : fn_genSQL(q[k]['value'])
                        })
                        q[k]['value'] = tableName
                    elif ('select' in q[k].keys()):
                        tableName = prefix + (q[k]['from'][0]['value'].title() if type(q[k]['from']) is list else q[k]['from'].title() if type(q[k]['from']) is str else q[k]['from']['value'].title())
                        queries.append({
                            'name'  : tableName,
                            'query' : fn_genSQL(q[k])
                        })
                        q[k] = tableName
            queries.append({
                'name'  : prefix + resultName,
                'query' : fn_genSQL(data)
            })

        else:
            queries.append({
                'name'  : prefix + resultName,
                'query' : fn_genSQL(data)
            })
    except:
        queries.append({
                'name'  : prefix + resultName,
                'query' : format({'from': data})[5:]
            })


def main(query):
    query = query.replace("#", "")
    global resultName
    if (query.find("INTO") >= 0) or (query.find("into") >= 0):
        into_s = query.find("INTO") if query.find("INTO") >= 0 else query.find("into")
        from_s1 = query.find("FROM") 
        from_s2 = query.find("from")
        from_s = into_s + 4
        if from_s1 >= 0 and from_s2 >= 0:
            from_s = min(from_s1, from_s2)
        elif from_s1 >= 0:
            from_s = from_s1
        else:
            from_s = from_s2
        resultName = query[into_s:from_s][5:].strip()
        query = (query.replace(query[into_s:from_s], ""))
    try:
        v_parse = parse(query)
        v_json = json.loads(json.dumps(v_parse,indent=4))
    except:
        return ("This query can't be converted!!")

    fn_get_pyspark(v_json)
    response = ""
    for q in queries:
        response += (q['name'] + " = " + q['query'] + "\n")
    return response