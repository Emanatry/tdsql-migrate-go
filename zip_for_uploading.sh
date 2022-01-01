git submodule update --init
echo $@ > label.txt
mkdir -p ./build
rm -rf ./build/tdsql.zip
zip -r ./build/tdsql.zip . -x run_my_db.sh -x 'build/*' -x run -x '.git/*' -x 'benchmarks/*' -x '.vscode/*' -x './presort/data/*'
echo created tdsql.zip with label \"$@\"