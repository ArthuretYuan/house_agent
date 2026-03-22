# # 替换成你自己的数据库信息
# db_user = "yaxiong"
# db_pass = "password"
# db_host = "localhost"
# db_port = 5433
# db_name = "housesagent"

import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import numpy as np

# LangChain 最新 API
from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain_ollama import ChatOllama
from langchain_ollama.llms import OllamaLLM

# ------------------------------
# 1️⃣ 读取 PostgreSQL 数据
# ------------------------------
engine = create_engine("postgresql://yaxiong:password@localhost:5433/housesagent")
df = pd.read_sql("SELECT * FROM properties", engine)

# ------------------------------
# 2️⃣ 数据清洗和特征
# ------------------------------
# 用实际列名 surface 和 city
df = df.replace(["", "None"], np.nan)
df = df.dropna(subset=["price", "surface"])
df["price"] = df["price"].astype(float)
df["surface"] = df["surface"].astype(float)
df["rooms"] = df["rooms"].astype(float)
df["bedrooms"] = df["bedrooms"].astype(float)
df["bathrooms"] = df["bathrooms"].astype(float)
df["showers"] = df["showers"].astype(float)
df["basement"] = df["basement"].astype(float)
df["garages"] = df["garages"].astype(float)
df["indoorParking"] = df["indoorParking"].astype(float)
df["outdoorParking"] = df["outdoorParking"].astype(float)
df["groundSurface"] = df["groundSurface"].astype(float)
df["price_per_sqm"] = df["price"] / df["surface"].replace(0, np.nan)

df["createdAt"] = pd.to_datetime(df["createdAt"], format="%Y%m%dT%H%M%SZ", utc=True)
df["updatedAt"] = pd.to_datetime(df["updatedAt"])

df = df.drop(columns=["permalink", "previewDescriptions"])

print("Data loaded and cleaned. Sample:")
print(df.head())

#df = df[:20]  # 取前20行，避免数据过大导致模型处理困难
# ------------------------------
# 3️⃣ 创建 AI Agent
# ------------------------------
llm = ChatOllama(
    model="gpt-oss:20b",
    temperature=0,
    #format="json",
    # other params...
)  # 本地免费模型

agent = create_pandas_dataframe_agent(llm,
                                      df,
                                      agent_type="tool-calling",
                                      #agent_type="zero-shot-react-description",
                                      allow_dangerous_code=True,
                                      verbose=True)

#response = agent.invoke("What are the columns in the dataset and their data types?")
#print(response["output"])

#运行 Agent 分析
report = agent.invoke(
    #"""Analyze Luxembourg housing market data. Generate a detailed report, including: 1. Average price per sqm by city 2. Distribution of houses vs apartments 3. Top 15 most expensive cities 4. Top 15 cheapest cities 5. Years that the houses have been on the market (duration between createdAt and updatedAt dates) 6. Summary of insights"""
    #"""Analyze Luxembourg housing market data. Provide insights: 1. Average price per sqm by city 2. Distribution of houses vs apartments 3. Top 10 most expensive areas 4. Summarize insights in a short market report"""
    {"input": "Analyze Luxembourg housing market data. Generate a detailed report, do not include python code. the report should include: 1. Average price per sqm by city 2. Distribution of houses vs apartments 3. Top 15 most expensive cities 4. Top 15 cheapest cities 5. Years that the houses have been on the market (duration between createdAt and updatedAt dates) 6. Summary of insights"}
)

with open("report.txt", "w") as f:
    f.write("===== AI Generated Market Report =====\n")
    f.write(report["output"])

# ------------------------------
# 4️⃣ 图表可视化
# ------------------------------
# 平均每平方米价格按城市
avg_city = df.groupby("city")["price_per_sqm"].mean().reset_index()
fig = px.bar(
    avg_city,
    x="city",
    y="price_per_sqm",
    title="Average Price per sqm by City",
    labels={"price_per_sqm": "€/sqm", "city": "City"}
)
fig.show()

# 房型分布（如果 type 列存在）
if "type" in df.columns:
    type_count = df["type"].value_counts().reset_index()
    type_count.columns = ["type", "count"]
    fig_type = px.pie(
        type_count,
        names="type",
        values="count",
        title="Property Type Distribution"
    )
    fig_type.show()