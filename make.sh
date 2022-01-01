# 评测的入口，开发不需要用
echo !!! make.sh 是评测时使用的脚本，开发使用 run_my_db.sh

# 从评测机挖一些信息回来
df -H
# cat /proc/cpuinfo | grep "model name"
# cat /proc/meminfo
# du -h /home/data

echo 
echo ======LABEL OF THIS BUILD======
cat label.txt
echo ===============================
# go run ./preflight/preflight.go
g++ -O2 ./presort/sortdata.cpp -o ./presort/sortdata
go build -o run main.go