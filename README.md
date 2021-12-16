开发环境：go 1.17

## 编译运行

将 `run_my_db.sh` 中的参数更改为自己的数据库实例，然后运行 `./run_my_db.sh`
这个文件在 .gitignore 中，不会被自动上传到代码仓库里。

`make.sh` 和 `start.sh` 是评测环境调用的脚本，开发中用不到

`zip_for_uploading.sh` 将代码（含必要脚本，不包含可执行文件）打包到 `./build/tdsql.zip`，用于提交评测。