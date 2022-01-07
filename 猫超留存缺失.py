# -*- coding:utf-8 -*-
import warnings
warnings.filterwarnings("ignore")
import pandas as pd

from sqlalchemy import create_engine

lis1 = []
lis2 = []
lis3 = []

dd = pd.read_excel('../维护表.xlsx', sheet_name='供应商id')
dd = dd.astype(str)

dd_dx = dd[dd['经营模式'].isin(['代销'])]
dd_js = dd[dd['经营模式'].isin(['寄售'])]

engine = create_engine('mysql+pymysql://liujingyu:lH2eKb4a0UkgF5Yz@rm-8vb066t7oqic4rt4mso.mysql.zhangbei.rds.aliyuncs.com:3306/spt')




# 代销
sql_dx = 'SELECT ' \
         ' substr(date,1,10) as 日期,' \
         ' supplier_code as 供应商编码,' \
         ' supplier_name as 供应商名称' \
         ' FROM `maochao_agency_daily_rk` group by date,supplier_code order by date,supplier_code' \


# 寄售
sql_js = 'SELECT ' \
         ' substr(business_date,1,10) as 日期,' \
         ' secondary_supplier_code as 供应商编码,' \
         ' secondary_supplier_name as 供应商名称' \
         ' FROM maochao_consignment_daily_rk group by business_date,secondary_supplier_code order by business_date,secondary_supplier_code' \


# 库存
sql_kc = 'SELECT ' \
         ' substr(calculate_the_date,1,10) as 日期,' \
         ' supplier_id as 供应商编码,' \
         ' supplier_name as 供应商名称' \
         ' FROM maochao_warehouse_daily_rk group by substr(calculate_the_date,1,10),supplier_id order by substr(calculate_the_date,1,10),supplier_id' \




def qs_date(df, name):
    l1 = []
    l2 = []
    df[name] = df[name].astype(str).str[:10]
    df[name] = pd.to_datetime(df[name])
    df = df.drop_duplicates(subset=[name])
    df = df.sort_values(by=name).reset_index()
    for i in df[name]:
        l1.append(str(i)[:10])
    t1 = df[name][0]

    t2 = df[name][df.shape[0]-1]

    print('日期范围:',str(t1)[:10],'~',str(t2)[:10])

    p = pd.date_range(start=t1, end=t2)
    for x in p:
        l2.append(str(x)[:10])

    set1 = set(l1)

    set2 = set(l2)

    if len(set1 ^ set2) == 0:

        print('缺失日期: 无')
    else:
        print('缺失日期:', set1 ^ set2)




def code(df_dx, df, lis, str_):

    # lis = []
    # 数据处理部分
    df_dx = df_dx.astype(str)
    # df_dx['日期'] = df_dx['日期'].astype(str).str[:10]

    print('-'*25, str_, '-'*25)
    qs_date(df_dx, '日期')
    # print()
    print('单日缺失供应商:')

    for i in df_dx.drop_duplicates(subset=['日期'])['日期']:
        df_dx_1 = df_dx[df_dx['日期'].isin([i])]

        df_dx_2 = pd.merge(df, df_dx_1, on=['供应商编码'], how='left')

        df_dx_3 = df_dx_2[df_dx_2['日期'].isnull()]

        if not df_dx_3.empty:

            print('日期:', i, df_dx_3['供应商编码'].values.tolist())

            df_dx_3['日期'] = i
            # df_dx_3 = df_dx_3[['日期','供应商编码']]
            lis.append(df_dx_3)

    try:
        df_all = pd.concat(lis)
    except:
        df_all = pd.DataFrame()



    # qs_date(df_dx, '日期')


    return df_all

def run():
    write = pd.ExcelWriter('../供应商编码缺失.xlsx')

    # 读取数据
    df_dx = pd.read_sql(sql_dx, con=engine)
    df_js = pd.read_sql(sql_js, con=engine)
    df_kc = pd.read_sql(sql_kc, con=engine)

    # df_dx = pd.read_excel('./data/代销.xlsx')
    # df_js = pd.read_excel('./data/寄售.xlsx')
    # df_kc = pd.read_excel('./data/库存.xlsx')
    print('读取数据中......')
    print()


    # 代销
    d_dx = code(df_dx, dd_dx, lis1, '代销')
    d_dx.to_excel(write, sheet_name='代销', index=False)


    # 寄售
    d_js = code(df_js, dd_js, lis2, '寄售')
    d_js.to_excel(write, sheet_name='寄售', index=False)


    # 库存
    d_kc = code(df_kc, dd, lis3, '库存')
    d_kc.to_excel(write, sheet_name='库存', index=False)

    write.save()
    write.close()

if __name__ == '__main__':
    run()


    input('任意键退出:')





