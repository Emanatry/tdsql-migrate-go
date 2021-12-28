开发环境：go 1.17
编译目标：go 1.15

## 编译运行

克隆仓库后，运行 `git submodule update --init`，下载依赖。（评测机无公网，必须把依赖一起打包）

创建 `./run_my_db.sh`：
```
go run main.go -data_path ../data/ -dst_ip <数据库公网地址.sql.tencentcdb.com> -dst_port <数据库公网端口> -dst_user <用户名> -dst_password <密码>
```
用于连接自己的数据库

`make.sh` 和 `start.sh` 是评测环境调用的脚本，开发中用不到

`zip_for_uploading.sh` 将代码（含必要脚本，不包含可执行文件）打包到 `./build/tdsql.zip`，用于提交评测。

dbenv 中含有创建测试用 mysql 8.0 的 docker-compose.yaml。执行`docker-compose up -d` 启动  
使用`go run main.go -data_path <data_path> -dst_ip localhost -dst_port 33330 -dst_user root -dst_password root-0h-mai-g0d-conta1neraizeision-yis-gr8t`连接。