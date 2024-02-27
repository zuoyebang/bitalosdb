rm -rf testlog
mkdir -p testlog

go test -v -timeout 200000s -run TestBitalosdbLoop1 &>./testlog/TestBitalosdbLoop1.log &
go test -v -timeout 200000s -run TestBitalosdbLoop2 &>./testlog/TestBitalosdbLoop2.log &
go test -v -timeout 200000s -run TestBitalosdbLoop3 &>./testlog/TestBitalosdbLoop3.log &

go test -v -timeout 200000s -run TestBitalosdbBitableLoop1 &>./testlog/TestBitalosdbBitableLoop1.log &
go test -v -timeout 200000s -run TestBitalosdbBitableLoop2 &>./testlog/TestBitalosdbBitableLoop2.log &
go test -v -timeout 200000s -run TestBitalosdbBitableLoop3 &>./testlog/TestBitalosdbBitableLoop3.log &

