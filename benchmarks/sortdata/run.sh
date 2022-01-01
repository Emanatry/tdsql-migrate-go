echo ==== cppver ==== && /usr/bin/time -f "mem=%KKB RSS=%MKB elapsed=%E cpu.sys=%S user=%U" ./sortdata_cpp < $@
echo ==== gover ==== && /usr/bin/time -f "mem=%KKB RSS=%MKB elapsed=%E cpu.sys=%S user=%U" ./sortdata < $@
echo "==== diff (should be empty) ===="
diff 1_sorted_cpp.csv 1_sorted_go.csv
echo "================================"