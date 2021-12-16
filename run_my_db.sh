# 把下面的参数改成自己的数据库
# 该脚本已经包含在 .gitignore 中，提交时 *不会* 被一并提交到代码仓库
go run main.go -data_path ../data/ -dst_ip <数据库公网地址.sql.tencentcdb.com> -dst_port <数据库公网端口> -dst_user <用户名> -dst_password <密码>
