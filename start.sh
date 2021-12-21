# 评测的入口，开发不需要用
echo !!! start.sh 是评测时使用的脚本，开发使用 run_my_db.sh
./run $@
echo "!!!!!! exited with exit code $?"
# until ./run $@; do
#     echo "!!!!!! exit code $?.  Respawning.." >&2
#     sleep 1
# done