# killLongRunningImpalaQuery
取消impala长时间运行的查询

版本说明：
CDH：CDH 5.12.0

Impala：v2.9.0-cdh5.12.0

Python: Python 3.5.2


需要安装cm_client库：
pip install cm_client

Usage:python  killLongRunningImpalaQuery.py  queryRunningSeconds [KILL]


附注：
主要是用到cm的api，获取impala的查询任务，找出长时间运行未结束的任务，并取消！
为了解决hue长时间不释放impala连接，导致任务池满了，后续任务超时问题！

当然对于这个问题，有更好的解决方案：
在hue的配置界面，调整hue_safety_valve.ini 参数如下：

[impala]
query_timeout_s=20
session_timeout_s=20
[beeswax]
close_queries=true

设置hue查询和session的超时时间，20s之后就会自动关闭查询连接！




