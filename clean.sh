# 为开发环境重置迁移进度，以及已经构建的文件，不会删除服务器上已经迁移的数据
./cleandev.sh
rm -rf label.txt ./presort/sortdata ./run
