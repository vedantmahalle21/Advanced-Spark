#!/usr/bin/env python
# coding: utf-8

# In[206]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType


# In[2]:


spark = SparkSession.builder.appName("linkage").getOrCreate()


# In[26]:


parsed = spark.read.option("header", "true").option("nullValue", "?").option("inferSchema", "true").csv("gs://gcp-ushi-search-platform-npe/Vedant/Learn/Spark/Linkage/Datasets/donations")


# In[27]:


parsed.show()


# In[28]:


parsed.printSchema()


# In[29]:


parsed.select("cmp_fname_c1").count()


# In[30]:


parsed.select("cmp_fname_c1").where(col("cmp_fname_c2").isNotNull()).count()


# In[37]:


parsed.select(col("is_match")).rdd.countByValue()


# In[52]:


parsed.groupBy("is_match").count().orderBy(col("count").desc()).show()


# In[39]:


parsed.agg(avg("cmp_sex"), stddev("cmp_sex")).show()


# In[40]:


parsed.createOrReplaceTempView("linkage")


# In[47]:


spark.sql("""SELECT is_match, count(*) cnt from linkage group by is_match order by cnt DESC""").show()


# In[53]:


summary = parsed.describe()


# In[60]:


summary.select("summary", "cmp_fname_c1", "cmp_fname_c2", "cmp_sex").show()


# In[66]:


matches = parsed.where(col("is_match") == True)
matchSummary = matches.describe()


# In[67]:


matchSummary.show()


# In[68]:


misses = parsed.where("is_match = False")
missSummary = misses.describe()


# In[69]:


missSummary.show()


# In[70]:


schema = summary.schema


# In[96]:


schema[0].name


# In[189]:


summary.columns


# In[199]:


summary.select(summary['id_1']).show()


# In[114]:


summary_p = summary.toPandas()


# In[120]:


summary_p


# In[174]:


summary_pT = summary_p.set_index('summary').T.reset_index()


# In[175]:


summary_pT = summary_pT.rename(columns={'index':'field'})


# In[176]:


summary_pT = summary_pT.rename_axis(None, axis=1)


# In[177]:


summary_pT


# In[186]:


summaryT = spark.createDataFrame(summary_pT)


# In[202]:


summaryT.count()


# In[192]:


summaryT.printSchema()


# In[207]:


for c in summaryT.columns:
    if c == "field":
        continue
    summaryT = summaryT.withColumn(c, summaryT[c].cast(DoubleType()))


# In[208]:


summaryT.show()


# In[213]:


def pivotSummary(desc: DataFrame) -> DataFrame:
    desc_p = desc.toPandas()
    desc_p = desc_p.set_index("summary").T.reset_index()
    desc_p = desc_p.rename(columns={'index':'field'})
    desc_p = desc_p.rename_axis(None, axis = 1)
    descT = spark.createDataFrame(desc_p)
    for c in descT.columns:
        if c == "field":
            continue
        descT = descT.withColumn(c, descT[c].cast(DoubleType()))
    return descT


# In[217]:


def pivotSummary(desc):
    desc_p = desc.toPandas()
    desc_p = desc_p.set_index("summary").T.reset_index()
    desc_p = desc_p.rename(columns={'index':'field'})
    desc_p = desc_p.rename_axis(None, axis = 1)
    descT = spark.createDataFrame(desc_p)
    for c in descT.columns:
        if c == "field":
            continue
        descT = descT.withColumn(c, descT[c].cast(DoubleType()))
    return descT


# In[218]:


matchSummaryT = pivotSummary(matchSummary)
missSummaryT = pivotSummary(missSummary)


# In[219]:


missSummaryT.show()


# In[220]:


matchSummaryT.createOrReplaceTempView("match_desc")
missSummaryT.createOrReplaceTempView("miss_desc")
spark.sql("""
SELECT a.field, a.count + b.count total, a.mean - b.mean delta
FROM match_desc a INNER JOIN miss_desc b ON a.field = b.field
WHERE a.field NOT IN ("id_1", "id_2")
ORDER BY delta DESC, total DESC
""").show()


# In[257]:


good_features = ["cmp_lname_c1", "cmp_plz", "cmp_by", "cmp_bd","cmp_bm"]


# In[258]:


sum_expression = " + ".join(good_features)


# In[259]:


sum_expression


# In[260]:


scored = parsed.fillna(0, good_features).withColumn('score', expr(sum_expression)).select("score", "is_match")


# In[261]:


scored.show()


# In[262]:


def crossTabs(scored: DataFrame, t: DoubleType) -> DataFrame: 
    return scored.selectExpr(f"score >= {t} as above", "is_match").groupBy("above").pivot("is_match", ("true","false")).count()


# In[263]:


crossTabs(scored, 4.0).show()


# In[ ]:





# In[ ]:




