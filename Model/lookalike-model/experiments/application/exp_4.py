## total number of rows
df = sql('select * from ads_clicklog_11162021')
df.count()
##7,640,717

## number of row with empty industry_id
df = sql('select * from ads_clicklog_11162021 where Length(industry_id) == 0 ')
df.count()
##2,425,201

## number of
df = sql('select industry_id, count(industry_id) as s from ads_showlog_11162021 where Length(industry_id) != 0  group by industry_id order by count(industry_id) ')
list_of_imp = df.select('s').collect()
l = 0
for i in range(len(list_of_imp)):
    l+=list_of_imp[i][0]

df = df.withColumn('percentage', df.s/l)
df = df.filter(df.percentage >0.01)
l = df.select('industry_id').collect()
keyword_list= [str(row.industry_id) for row in df.select('industry_id').collect()]
