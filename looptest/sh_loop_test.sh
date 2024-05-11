rm -rf testlog
mkdir -p testlog

go test -v -timeout 200000s -run TestBitalosdbLoop1 &>./testlog/TestBitalosdbLoop1.log &
go test -v -timeout 200000s -run TestBitalosdbLoop2 &>./testlog/TestBitalosdbLoop2.log &
