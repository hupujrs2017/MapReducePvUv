# MapReducePvUv
用于计算一天1440分钟每分钟不同页面的pv、uv<br /> 
有这样一种场景，我们需要知道每分钟不同页面的pv/uv量（此处特指离线，实时storm计算再讲），通过写SQL我们发现非常复杂。我们发现mapreduce可以完美的解决这个问题。<br /> 
本程序有一个地方需要特别注意，一个pageid如果app A种产品的详情页，那么它肯定是app的详情页，而我们统计时往往app pv、uv，app A类产品的详情页pv、uv都是需要的。<br /> 
如果没有以上这个逻辑，则可将下面这行去掉，直接发送pageid即可。<br /> 
` `` 
Set<String> products = PVGroup.getProductTypeByPageId(pageId);<br /> 
` `` 
贴一张流程图，更有利于理解：<br /> 
![image](https://github.com/hupujrs2017/MapReducePvUv/blob/master/src/main/resources/pvuv.png)

