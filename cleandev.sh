# 为开发环境重置迁移进度，不会删除服务器上已经迁移的数据
rm -rf migration_inprogress.txt
rm -rf ./presort/data
rm -rf ./run
rm -rf ./migration_log