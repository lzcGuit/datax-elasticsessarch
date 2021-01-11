# datax-elasticsessarch
elasticsearch writer plugin for datax

## 插件说明
优化```elasticsearch-writer```插件，采用```rest客户端```连接集群的```9200```端口，因为官方后续不会提供```TransportClient```支持

# Support Data Channels 
| 数据源        | Reader(读) | Writer(写) |文档|
| ---------- | :-------: | :-------: |:-------: |
| Elasticsearch       |         |     √     |[写](https://github.com/lzcGuit/datax-elasticsessarch/tree/master/elasticsearchwriter/elasticsearchwriter.md)|

## 使用说明

1、clone本项目，替换```datax```原项目的```elasticsearchwriter```，然后打包，或者直接用```target```下面打包好的。在```elasticsearchwriter/target/datax/plugin/writer```，将```elasticsearchwriter```文件夹拷贝至```datax/plugin/writer```目录下  

2、编写job文件

3、启动任务

## java启动脚本
1、在```datax```根目录下新建```javarun.bat```,写入下列内容保存
```
@echo off
set input=%1%
java -classpath conf;%~dp0\\lib\\*;%~dp0\\plugin\\** -Ddatax.home=%~dp0\\ com.alibaba.datax.core.Engine -mode standalone -jobid -1 -job %~dp0\\job\%1%.json
```
2、在job文件夹中编辑好任务文件，用自带job.json示例

3、运行脚本命令：  ```javarun.bat job(任务文件名)```
