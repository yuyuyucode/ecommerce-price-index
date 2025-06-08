电商价格指数分析工具​​ 📊
一个基于Python的简易工具，用于从ClickHouse数据计算和可视化价格指数趋势。

​​功能特点​​
✅ ​​价格指数计算​​ - 基于基期计算每日价格指数
📈 ​​可视化图表​​ - 生成价格趋势图便于分析
🛒 ​​电商数据专用​​ - 针对在线零售价格数据优化

​​环境要求​​
Python 3.8+
ClickHouse数据库
依赖库：clickhouse-driver, pandas, matplotlib, seaborn
​​使用说明​​
​​安装依赖​​：
pip install clickhouse-driver pandas matplotlib seaborn
​​配置数据库​​：
确保ClickHouse服务已运行
在ch_operations.py中修改数据库连接配置
​​运行分析​​：
python main.py
​​输出结果​​
📂 ​​结果目录​​：results_年月日/
📊 ​​生成图表​​：

simple_index.png - 价格指数趋势图
simple_index.csv - 原始指数数据
​​注意事项​​
⚠️ 如需完整功能（分类价格分析），请确保：

local_categories表包含正确的分类层级和权重
local_products表中的分类ID能正确关联