import os
from graphviz import Digraph

os.environ["PATH"] += os.pathsep + r'C:\Program Files\Graphviz\bin'

if __name__ == '__main__':
    dot = Digraph(comment='Database ERD')
    dot.attr(rankdir='LR', size='12,8')

    dot.attr('node', shape='record', style='filled', fillcolor='lightblue')

    dot.node('TBL_CODE', '''{{TBL_CODE|
    <pk>region_code (PK)\l|
    sido_code\l|
    sido_name\l|
    sigungu_code\l|
    sigungu_name\l|
    eupmyeondong_code\l|
    eupmyeondong_name\l|
    ri_code\l|
    ri_name\l|
    region_name\l}}''')

    dot.node('TBL_TRADE', '''{{TBL_TRADE|
    <fk>region_code (FK)\l|
    contract_dte\l|
    district\l|
    cd_district\l|
    apt_name\l|
    address\l|
    price\l|
    price_unitdd\l|
    con_year\l|
    area\l|
    floor\l|
    py\l|
    py_unit\l}}''')

    dot.edge('TBL_TRADE:fk', 'TBL_CODE:pk', label='1:1')

    dot.render('images/erd/db_erd', format='png', cleanup=True)