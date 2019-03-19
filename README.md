# Plink
### Author:ThisFang


结合flink提供给python的api轻封装的流处理框架，重构项目[meyer-net/robot](https://github.com/meyer-net/robot)获得



## 说明

* 开发环境
```comment
Centos7
python==3.6.5
virtualenv==16.4.3
pip==19.0.3
setuptools==39.0.1
```


* 部署
```comment
git后请修改flink-restart.sh和start.py中Flink的位置

创建虚拟环境，命名为venv:
cd /{project_dir}
virtualenv -p python3 venv

应用虚拟环境venv: 
source venv/bin/activate

退出虚拟环境: 
deactivate 

安装requirements中的包: 
pip install -r requirements.txt

导出环境中的包: 
pip freeze > requirements.txt

项目启动
适当配置conf中的boot.py后，输入如下命令启动
python start.py app/stream/main.py
```


